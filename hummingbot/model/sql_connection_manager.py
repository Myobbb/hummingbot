import logging
from enum import Enum
from os.path import join
from typing import TYPE_CHECKING, Optional

from sqlalchemy import MetaData, create_engine, event, inspect
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import Query, Session, sessionmaker
from sqlalchemy.schema import DropConstraint, ForeignKeyConstraint, Table

from hummingbot import data_path
from hummingbot.logger.logger import HummingbotLogger
from hummingbot.model import get_declarative_base
from hummingbot.model.metadata import Metadata as LocalMetadata
from hummingbot.model.transaction_base import TransactionBase

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter


class SQLConnectionType(Enum):
    TRADE_FILLS = 1


class SQLConnectionManager(TransactionBase):
    _scm_logger: Optional[HummingbotLogger] = None
    _scm_trade_fills_instance: Optional["SQLConnectionManager"] = None

    LOCAL_DB_VERSION_KEY = "local_db_version"
    LOCAL_DB_VERSION_VALUE = "20230516"

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._scm_logger is None:
            cls._scm_logger = logging.getLogger(__name__)
        return cls._scm_logger

    @classmethod
    def get_declarative_base(cls):
        return get_declarative_base()

    @classmethod
    def get_trade_fills_instance(
        cls, client_config_map: "ClientConfigAdapter", db_name: Optional[str] = None
    ) -> "SQLConnectionManager":
        if cls._scm_trade_fills_instance is None:
            cls._scm_trade_fills_instance = SQLConnectionManager(
                client_config_map, SQLConnectionType.TRADE_FILLS, db_name=db_name
            )
        elif cls.create_db_path(db_name=db_name) != cls._scm_trade_fills_instance.db_path:
            cls._scm_trade_fills_instance = SQLConnectionManager(
                client_config_map, SQLConnectionType.TRADE_FILLS, db_name=db_name
            )
        return cls._scm_trade_fills_instance

    @classmethod
    def create_db_path(cls, db_path: Optional[str] = None, db_name: Optional[str] = None) -> str:
        if db_path is not None:
            return db_path
        if db_name is not None:
            return join(data_path(), f"{db_name}.sqlite")
        else:
            return join(data_path(), "hummingbot_trades.sqlite")

    def __init__(self,
                 client_config_map: "ClientConfigAdapter",
                 connection_type: SQLConnectionType,
                 db_path: Optional[str] = None,
                 db_name: Optional[str] = None,
                 called_from_migrator = False):
        db_path = self.create_db_path(db_path, db_name)
        self.db_path = db_path

        if connection_type is SQLConnectionType.TRADE_FILLS:
            self._engine: Engine = create_engine(client_config_map.db_mode.get_url(self.db_path))
            if self._engine.dialect.name == "sqlite":
                event.listen(self._engine, "connect", self._sqlite_fast_commits)
            self._metadata: MetaData = self.get_declarative_base().metadata
            self._metadata.create_all(self._engine)

            # SQLite does not enforce foreign key constraint, but for others engines, we need to drop it.
            # See: `hummingbot/market/markets_recorder.py`, at line 213.
            with self._engine.begin() as conn:
                inspector = inspect(conn)

                for tname, fkcs in reversed(
                        inspector.get_sorted_table_and_fkc_names()):
                    if fkcs:
                        if not self._engine.dialect.supports_alter:
                            continue
                        for fkc in fkcs:
                            fk_constraint = ForeignKeyConstraint((), (), name=fkc)
                            Table(tname, MetaData(), fk_constraint)
                            conn.execute(DropConstraint(fk_constraint))

        self._session_cls = sessionmaker(bind=self._engine)

        if connection_type is SQLConnectionType.TRADE_FILLS and (not called_from_migrator):
            self.check_and_migrate_db(client_config_map)

    @classmethod
    def _sqlite_fast_commits(cls, dbapi_connection, connection_record):
        """each new SQLite connection: WAL, then synchronous NORMAL (Pavel 2026-10-17, the P2 hot path: "recorder fix").
        The trade recorder (markets_recorder) commits on HMB's one loop at every order event, created, filled, cancelled,
        failed and completed, ~1,250 an hour live. In the rollback journal with synchronous FULL each commit waits on two
        fsyncs: 8.6 ms p50 on the live 1.29 GB test_multi.sqlite, with every book, tick and order held meanwhile. In WAL
        with NORMAL a commit is an append to the -wal file and the fsync comes only at a checkpoint (0.008 ms). An HMB crash
        loses nothing; an OS crash or a power cut may lose the last commits, never the file. WAL stays set in the file
        (the -wal and -shm files sit beside it). NORMAL is set only once the file IS in WAL: it is not crash-safe in the
        rollback journal. A connection whose file can't switch (another process holding it) keeps its mode, said once"""
        cursor = dbapi_connection.cursor()
        try:
            mode = cursor.execute("PRAGMA journal_mode=WAL").fetchone()
            if mode is not None and str(mode[0]).lower() == "wal":
                cursor.execute("PRAGMA synchronous=NORMAL")
            else:
                cls.logger().warning(f"the trade database stays in journal mode {mode[0] if mode else '?'}: it could not "
                                     f"switch to WAL, so each commit still waits on its fsyncs")
        except Exception as e:
            cls.logger().warning(f"the trade database's journal mode could not be set ({e}): its commits wait on fsyncs")
        finally:
            cursor.close()

    @property
    def engine(self) -> Engine:
        return self._engine

    def get_new_session(self) -> Session:
        return self._session_cls()

    def get_local_db_version(self, session: Session):
        query: Query = (session.query(LocalMetadata)
                        .filter(LocalMetadata.key == self.LOCAL_DB_VERSION_KEY))
        result: Optional[LocalMetadata] = query.one_or_none()
        return result

    def check_and_migrate_db(self, client_config_map: "ClientConfigAdapter"):
        from hummingbot.model.db_migration.migrator import Migrator
        with self.get_new_session() as session:
            with session.begin():
                local_db_version = self.get_local_db_version(session=session)
                if local_db_version is None:
                    version_info: LocalMetadata = LocalMetadata(key=self.LOCAL_DB_VERSION_KEY,
                                                                value=self.LOCAL_DB_VERSION_VALUE)
                    session.add(version_info)
                    session.commit()
                else:
                    # There's no past db version to upgrade from at this moment. So we'll just update the version value
                    # if needed.
                    if local_db_version.value < self.LOCAL_DB_VERSION_VALUE:
                        was_migration_successful = Migrator().migrate_db_to_version(
                            client_config_map, self, int(local_db_version.value), int(self.LOCAL_DB_VERSION_VALUE)
                        )
                        if was_migration_successful:
                            # Cannot use variable local_db_version because reference is not valid
                            # since Migrator changed it
                            self.get_local_db_version(session=session).value = self.LOCAL_DB_VERSION_VALUE

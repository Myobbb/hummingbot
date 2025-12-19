import asyncio
import websockets
import json
import gzip

async def test_sub():
    url = "wss://api-aws.huobi.pro/ws"  # Try standard WS endpoint
    # Try both trade.detail and mbp to see difference
    subs = [
        {"sub": "market.fluxusdt.mbp.5", "id": "1"},
        {"sub": "market.fluxusdt.trade.detail", "id": "2"}
    ]
    
    async with websockets.connect(url) as ws:
        for sub in subs:
            await ws.send(json.dumps(sub))
            print(f"Sent: {sub}")
            
        while True:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                try:
                    msg = json.loads(gzip.decompress(raw))
                except:
                    msg = json.loads(raw)
                    
                if "ping" in msg:
                    await ws.send(json.dumps({"pong": msg["ping"]}))
                    print("Pong")
                    continue
                    
                print(f"Received: {msg}")
                
            except asyncio.TimeoutError:
                print("Timeout waiting for message")
                break
            except Exception as e:
                print(f"Error: {e}")
                break

if __name__ == "__main__":
    asyncio.run(test_sub())

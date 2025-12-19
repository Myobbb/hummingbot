import requests
import json

def check_symbol(symbol):
    url = "https://api-aws.huobi.pro/v1/common/symbols"
    try:
        resp = requests.get(url)
        data = resp.json()
        
        if data.get("status") != "ok":
            print(f"Error fetching symbols: {data}")
            return

        found = False
        for s in data["data"]:
            if s["symbol"] == symbol:
                found = True
                print(f"Symbol: {s['symbol']}")
                print(f"State: {s['state']}")
                print(f"API Trading: {s.get('api-trading', 'unknown')}")
                print(f"Tags: {s.get('tags', 'none')}")
                break
        
        if not found:
            print(f"Symbol {symbol} NOT FOUND in HTX active symbols list.")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_symbol("fluxusdt")

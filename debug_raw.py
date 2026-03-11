"""
debug_raw.py — показывает сырой JSON ответ от каждой биржи
Запуск: python debug_raw.py
"""
import hashlib, hmac, time, base64, os, requests
from dotenv import load_dotenv
load_dotenv()

def ts_ms(): return int(time.time() * 1000)
def ts_s():  return int(time.time())
def sign256(secret, msg): return hmac.new(secret.encode(), msg.encode(), hashlib.sha256).hexdigest()
def sign256b64(secret, msg): return base64.b64encode(hmac.new(secret.encode(), msg.encode(), hashlib.sha256).digest()).decode()
def sign512b64(secret, msg): return hmac.new(secret.encode(), msg.encode(), hashlib.sha512).hexdigest()

SEP = "─" * 55

# ── BINANCE ───────────────────────────────────────────────────
print(SEP)
print("BINANCE")
key = os.getenv("BINANCE_API_KEY",""); sec = os.getenv("BINANCE_API_SECRET","")
if key:
    ts = ts_ms()
    msg = f"timestamp={ts}&recvWindow=10000"
    sig = sign256(sec, msg)
    r = requests.get("https://fapi.binance.com/fapi/v2/balance",
        headers={"X-MBX-APIKEY": key},
        params={"timestamp": ts, "recvWindow": 10000, "signature": sig}, timeout=15)
    print(f"Status: {r.status_code}")
    # Показываем только USDT и ненулевые
    data = r.json()
    if isinstance(data, list):
        nonzero = [x for x in data if float(x.get('balance','0')) > 0 or x.get('asset') == 'USDT']
        print(f"USDT/nonzero entries: {nonzero[:5]}")
    else:
        print(f"Response: {data}")

# ── BYBIT ─────────────────────────────────────────────────────
print(SEP)
print("BYBIT")
key = os.getenv("BYBIT_API_KEY",""); sec = os.getenv("BYBIT_API_SECRET","")
if key:
    for acc in ["CONTRACT", "UNIFIED"]:
        ts = str(ts_ms()); rw = "10000"
        params_str = f"accountType={acc}"
        sig = sign256(sec, ts + key + rw + params_str)
        r = requests.get("https://api.bybit.com/v5/account/wallet-balance",
            headers={"X-BAPI-API-KEY": key, "X-BAPI-SIGN": sig,
                     "X-BAPI-TIMESTAMP": ts, "X-BAPI-RECV-WINDOW": rw},
            params={"accountType": acc}, timeout=15)
        data = r.json()
        print(f"[{acc}] retCode={data.get('retCode')} retMsg={data.get('retMsg')}")
        for wallet in data.get('result',{}).get('list',[]):
            coins = wallet.get('coin',[])
            usdt = [c for c in coins if c.get('coin') == 'USDT']
            print(f"  totalEquity={wallet.get('totalEquity')} coins_count={len(coins)} USDT={usdt}")

# ── MEXC ──────────────────────────────────────────────────────
print(SEP)
print("MEXC")
key = os.getenv("MEXC_API_KEY",""); sec = os.getenv("MEXC_API_SECRET","")
if key:
    ts = ts_ms()
    sig = sign256(sec, f"timestamp={ts}")
    # Futures
    r = requests.get("https://contract.mexc.com/api/v1/private/account/assets",
        headers={"ApiKey": key, "Request-Time": str(ts), "Signature": sig,
                 "Content-Type": "application/json"}, timeout=15)
    print(f"Futures status={r.status_code}")
    data = r.json()
    assets = data.get('data') or []
    usdt = [a for a in assets if a.get('currency') == 'USDT'] if isinstance(assets, list) else []
    print(f"  USDT entry: {usdt}")
    if not usdt:
        print(f"  All assets: {assets[:3]}")
        print(f"  Full response: {data}")

# ── BITGET ────────────────────────────────────────────────────
print(SEP)
print("BITGET")
key = os.getenv("BITGET_API_KEY",""); sec = os.getenv("BITGET_API_SECRET",""); pp = os.getenv("BITGET_PASSPHRASE","")
if key:
    ts = str(ts_ms())
    path = "/api/v2/mix/account/accounts?productType=USDT-FUTURES"
    sig = sign256b64(sec, ts + "GET" + path)
    r = requests.get(f"https://api.bitget.com{path}",
        headers={"ACCESS-KEY": key, "ACCESS-SIGN": sig, "ACCESS-TIMESTAMP": ts,
                 "ACCESS-PASSPHRASE": pp, "Content-Type": "application/json"}, timeout=15)
    data = r.json()
    print(f"code={data.get('code')} msg={data.get('msg')}")
    for item in (data.get('data') or []):
        print(f"  marginCoin={item.get('marginCoin')} available={item.get('available')} usdtEquity={item.get('usdtEquity')}")
    if not data.get('data'):
        print(f"  Full response: {data}")

# ── GATE ──────────────────────────────────────────────────────
print(SEP)
print("GATE")
key = os.getenv("GATE_API_KEY",""); sec = os.getenv("GATE_API_SECRET","")
if key:
    ts = str(ts_s())
    method = "GET"; path = "/api/v4/futures/usdt/accounts"; body = ""
    body_hash = hashlib.sha512(body.encode()).hexdigest()
    sign_str = "\n".join([method, path, "", body_hash, ts])
    sig = hmac.new(sec.encode(), sign_str.encode(), hashlib.sha512).hexdigest()
    r = requests.get(f"https://api.gateio.ws{path}",
        headers={"KEY": key, "SIGN": sig, "Timestamp": ts,
                 "Content-Type": "application/json"}, timeout=15)
    print(f"Status={r.status_code}")
    data = r.json()
    print(f"  Response: {data}")

print(SEP)
print("Готово — скинь вывод")

"""
3대 거래소(Binance, Bybit, OKX) 공통 선물 1분 거래량 상위 심볼 다중구독 실시간 청산 포착 예제
"""
import asyncio
import json
import time
import urllib.request
import websockets
from concurrent.futures import ThreadPoolExecutor
from crypto_liquidation import LiquidationEvent, NormalizedLiquidation

HEADERS = {'User-Agent': 'Mozilla/5.0'}

def to_ms(ts):
    if ts is None: return int(time.time() * 1000)
    try:
        t = int(float(ts))
        return t * 1000 if t < 10**11 else t
    except Exception:
        return int(time.time() * 1000)

def fetch_symbol_1m_volume(s: str) -> dict:
    base = s[:-4]
    b_1m, by_1m, ok_1m = 0.0, 0.0, 0.0
    try:
        req = urllib.request.Request(f'https://fapi.binance.com/fapi/v1/klines?symbol={s}&interval=1m&limit=2', headers=HEADERS)
        with urllib.request.urlopen(req, timeout=3) as resp:
            b_k = json.loads(resp.read().decode('utf-8'))
        b_1m = float(b_k[-1][7])
    except Exception: pass

    try:
        req = urllib.request.Request(f'https://api.bybit.com/v5/market/kline?category=linear&symbol={s}&interval=1&limit=2', headers=HEADERS)
        with urllib.request.urlopen(req, timeout=3) as resp:
            by_k = json.loads(resp.read().decode('utf-8'))['result']['list']
        by_1m = float(by_k[0][6])
    except Exception: pass

    try:
        ok_sym = f"{base}-USDT-SWAP"
        req = urllib.request.Request(f'https://www.okx.com/api/v5/market/candles?instId={ok_sym}&bar=1m&limit=2', headers=HEADERS)
        with urllib.request.urlopen(req, timeout=3) as resp:
            ok_k = json.loads(resp.read().decode('utf-8'))['data']
        ok_1m = float(ok_k[0][7] or ok_k[0][6] or 0)
    except Exception: pass

    return {
        'symbol': s,
        'base': base,
        'binance_1m': b_1m,
        'bybit_1m': by_1m,
        'okx_1m': ok_1m,
        'total_1m': b_1m + by_1m + ok_1m
    }

def get_common_top_symbols():
    req = urllib.request.Request('https://fapi.binance.com/fapi/v1/ticker/24hr', headers=HEADERS)
    with urllib.request.urlopen(req) as resp:
        b_symbols = {d['symbol']: float(d['quoteVolume']) for d in json.loads(resp.read().decode('utf-8')) if d['symbol'].endswith('USDT')}

    req = urllib.request.Request('https://api.bybit.com/v5/market/tickers?category=linear', headers=HEADERS)
    with urllib.request.urlopen(req) as resp:
        by_symbols = {d['symbol']: float(d['turnover24h']) for d in json.loads(resp.read().decode('utf-8'))['result']['list'] if d['symbol'].endswith('USDT')}

    req = urllib.request.Request('https://www.okx.com/api/v5/market/tickers?instType=SWAP', headers=HEADERS)
    with urllib.request.urlopen(req) as resp:
        ok_data = json.loads(resp.read().decode('utf-8'))['data']
    ok_symbols = {d['instId'].replace('-USDT-SWAP', 'USDT'): float(d.get('volCcy24h', 0) or 0) for d in ok_data if d['instId'].endswith('-USDT-SWAP')}

    common = set(b_symbols.keys()) & set(by_symbols.keys()) & set(ok_symbols.keys())
    top_candidates = sorted(common, key=lambda s: b_symbols[s] + by_symbols[s] + ok_symbols[s], reverse=True)[:60]

    with ThreadPoolExecutor(max_workers=20) as executor:
        res_1m = list(executor.map(fetch_symbol_1m_volume, top_candidates))

    top_sorted = sorted(res_1m, key=lambda x: x['total_1m'], reverse=True)
    pool_candidates = sorted(common, key=lambda s: b_symbols[s] + by_symbols[s] + ok_symbols[s], reverse=True)[:100]
    return top_sorted[:30], pool_candidates

async def main():
    top_30, pool = get_common_top_symbols()
    print("=" * 80)
    print(f"📊 3대 거래소 공통 선물 1분 거래량 상위 30개 심볼 리스팅 완료 (풀 크기: {len(pool)}개)")
    print("=" * 80)
    for idx, item in enumerate(top_30[:10], 1):
        print(f"#{idx:<2} {item['symbol']:<12} 1분 합산: ${item['total_1m']:>12,.2f} (바이낸스: ${item['binance_1m']:>10,.2f}, 바이비트: ${item['bybit_1m']:>10,.2f}, OKX: ${item['okx_1m']:>10,.2f})")

if __name__ == '__main__':
    asyncio.run(main())

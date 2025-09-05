# enhanced_fetch_data_to_dune.py
"""
Розширений збір даних з Extended API для унікального аналізу біржі
Збирає:
1. Markets data (ціни, обсяги, open interest) - з історією
2. Trading statistics (історичні обсяги) - накопичувально
3. TVL дані з контрактів (через DeFiLlama API) - з часовими мітками
4. Funding rates history - нові дані
5. Order book snapshots - нові дані
Зберігає все в окремі CSV файли для Dune Analytics з дедуплікацією
"""

import requests
import pandas as pd
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import json
import hashlib
import time

# === КОНФІГУРАЦІЯ ===
UPLOADS_DIR = "uploads"
TIMEOUT = 30
MAX_RETRIES = 3
RETRY_DELAY = 2

# API endpoints для різних мереж
ENDPOINTS = {
    'ethereum': {
        'markets': 'https://api.extended.exchange/api/v1/info/markets',
        'trading': 'https://api.extended.exchange/api/v1/exchange/stats/trading',
        'orderbook': 'https://api.extended.exchange/api/v1/orderbook',
        'funding': 'https://api.extended.exchange/api/v1/funding',
        'start_date': '2025-03-11'
    },
    'starknet': {
        'markets': 'https://api.starknet.extended.exchange/api/v1/info/markets',
        'trading': 'https://api.starknet.extended.exchange/api/v1/exchange/stats/trading',
        'orderbook': 'https://api.starknet.extended.exchange/api/v1/orderbook',
        'funding': 'https://api.starknet.extended.exchange/api/v1/funding',
        'start_date': '2025-08-10'
    }
}

# DeFiLlama TVL endpoint
DEFILLAMA_TVL_API = "https://api.llama.fi/protocol/extended"

def ensure_uploads_dir():
    """Створює папку uploads якщо її немає"""
    os.makedirs(UPLOADS_DIR, exist_ok=True)
    print("📁 Uploads directory ready")

def make_request_with_retry(url: str, params: Dict = None) -> Optional[Dict]:
    """Робить запит з повтором при помилці"""
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, params=params, timeout=TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"❌ Attempt {attempt + 1}/{MAX_RETRIES} failed for {url}: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
            else:
                return None
    return None

def fetch_markets_data(chain: str) -> List[Dict]:
    """
    Отримує дані ринків (markets) для конкретної мережі
    Повертає список ринків з цінами, обсягами та open interest
    """
    url = ENDPOINTS[chain]['markets']
    print(f"🔄 Fetching markets data for {chain}...")
    
    data = make_request_with_retry(url)
    if not data:
        return []
    
    markets = data.get("data", []) if isinstance(data, dict) else data
    print(f"✅ Got {len(markets)} markets from {chain}")
    return markets or []

def fetch_trading_stats(chain: str, date: str) -> Dict:
    """
    Отримує статистику торгів для конкретної дати та мережі
    """
    url = ENDPOINTS[chain]['trading']
    params = {'fromDate': date, 'toDate': date}
    print(f"🔄 Fetching trading stats for {chain} on {date}...")
    
    data = make_request_with_retry(url, params)
    if not data:
        return {'date': date, 'chain': chain, 'daily_volume': 0, 'trades_count': 0}
    
    # Обробляємо дані статистики
    if isinstance(data, dict) and 'data' in data:
        trading_data = data['data']
        if trading_data:
            total_volume = sum(float(item.get('tradingVolume', 0)) for item in trading_data)
            unique_traders = len(set(item.get('trader', '') for item in trading_data if item.get('trader')))
            
            return {
                'date': date,
                'chain': chain,
                'daily_volume': total_volume,
                'trades_count': len(trading_data),
                'unique_traders': unique_traders,
                'avg_trade_size': total_volume / len(trading_data) if trading_data else 0
            }
    
    return {'date': date, 'chain': chain, 'daily_volume': 0, 'trades_count': 0, 'unique_traders': 0, 'avg_trade_size': 0}

def fetch_funding_rates(chain: str) -> List[Dict]:
    """
    Отримує історію funding rates для всіх ринків
    """
    url = ENDPOINTS[chain]['funding']
    print(f"🔄 Fetching funding rates for {chain}...")
    
    data = make_request_with_retry(url)
    if not data:
        return []
    
    funding_data = data.get("data", []) if isinstance(data, dict) else data
    
    # Нормалізуємо дані
    normalized_data = []
    for item in funding_data:
        if isinstance(item, dict):
            normalized_data.append({
                'chain': chain,
                'market': item.get('market', 'UNKNOWN'),
                'funding_rate': float(item.get('fundingRate', 0) or 0),
                'funding_time': item.get('fundingTime', ''),
                'next_funding_time': item.get('nextFundingTime', ''),
                'fetched_at': datetime.utcnow().isoformat() + "Z"
            })
    
    print(f"✅ Got {len(normalized_data)} funding records from {chain}")
    return normalized_data

def fetch_orderbook_snapshots(chain: str, markets: List[str]) -> List[Dict]:
    """
    Отримує snapshots order book для топ ринків
    """
    snapshots = []
    url = ENDPOINTS[chain]['orderbook']
    
    # Беремо тільки топ 5 ринків щоб не перевантажувати API
    top_markets = markets[:5]
    
    for market in top_markets:
        print(f"🔄 Fetching orderbook for {chain}:{market}...")
        params = {'market': market}
        
        data = make_request_with_retry(url, params)
        if not data:
            continue
            
        orderbook = data.get("data", {})
        bids = orderbook.get("bids", [])
        asks = orderbook.get("asks", [])
        
        if bids or asks:
            # Розраховуємо метрики глибини ринку
            bid_depth = sum(float(bid[1]) for bid in bids[:10])  # Топ 10 бідів
            ask_depth = sum(float(ask[1]) for ask in asks[:10])  # Топ 10 асків
            
            best_bid = float(bids[0][0]) if bids else 0
            best_ask = float(asks[0][0]) if asks else 0
            spread = best_ask - best_bid if best_bid and best_ask else 0
            
            snapshots.append({
                'chain': chain,
                'market': market,
                'best_bid': best_bid,
                'best_ask': best_ask,
                'spread': spread,
                'spread_pct': (spread / best_bid * 100) if best_bid else 0,
                'bid_depth_10': bid_depth,
                'ask_depth_10': ask_depth,
                'total_bids': len(bids),
                'total_asks': len(asks),
                'fetched_at': datetime.utcnow().isoformat() + "Z"
            })
    
    print(f"✅ Got {len(snapshots)} orderbook snapshots from {chain}")
    return snapshots

def fetch_tvl_data() -> Optional[Dict]:
    """Отримує TVL дані з DeFiLlama"""
    print("🔄 Fetching TVL data from DeFiLlama...")
    
    data = make_request_with_retry(DEFILLAMA_TVL_API)
    if data:
        print("✅ Got TVL data from DeFiLlama")
    return data

def normalize_markets_data(markets: List[Dict], chain: str) -> List[Dict]:
    """Нормалізує дані ринків в уніфікований формат"""
    rows = []
    fetched_at = datetime.utcnow().isoformat() + "Z"
    
    for market in markets:
        if not isinstance(market, dict):
            continue
            
        stats = market.get("marketStats", {})
        
        # Створюємо унікальний ID для дедуплікації
        unique_id = f"{chain}_{market.get('name', 'UNKNOWN')}_{fetched_at[:16]}"  # До хвилини
        
        row = {
            "unique_id": unique_id,
            "fetched_at": fetched_at,
            "chain": chain,
            "market": market.get("name", "UNKNOWN"),
            
            # Ціни
            "lastPrice": float(stats.get("lastPrice", 0) or 0),
            "bidPrice": float(stats.get("bidPrice", 0) or 0),
            "askPrice": float(stats.get("askPrice", 0) or 0),
            "markPrice": float(stats.get("markPrice", 0) or 0),
            "indexPrice": float(stats.get("indexPrice", 0) or 0),
            
            # Обсяги та інтереси
            "dailyVolume": float(stats.get("dailyVolume", 0) or 0),
            "dailyVolumeBase": float(stats.get("dailyVolumeBase", 0) or 0),
            "openInterest": float(stats.get("openInterest", 0) or 0),
            
            # Додаткові метрики
            "fundingRate": float(stats.get("fundingRate", 0) or 0),
            "priceChange24h": float(stats.get("priceChange24h", 0) or 0),
            
            # Розраховуємо spread
            "spread_pct": 0,
        }
        
        # Розраховуємо spread
        if row["bidPrice"] > 0 and row["askPrice"] > 0 and row["lastPrice"] > 0:
            row["spread_pct"] = (row["askPrice"] - row["bidPrice"]) / row["lastPrice"] * 100
            
        rows.append(row)
    
    return rows

def save_data_with_deduplication(df: pd.DataFrame, filename: str, unique_columns: List[str]):
    """
    Зберігає дані з дедуплікацією та обмеженням розміру
    """
    file_path = os.path.join(UPLOADS_DIR, filename)
    
    if os.path.exists(file_path):
        try:
            # Читаємо існуючі дані
            existing = pd.read_csv(file_path)
            
            # Об'єднуємо з новими
            combined = pd.concat([existing, df], ignore_index=True)
            
            # Дедуплікація по унікальних колонках
            combined = combined.drop_duplicates(subset=unique_columns, keep='last')
            
            # Сортуємо по часу (новіші записи зверху)
            if 'fetched_at' in combined.columns:
                combined = combined.sort_values('fetched_at', ascending=False)
            elif 'date' in combined.columns:
                combined = combined.sort_values('date', ascending=False)
            
            # Обмежуємо розмір файлу
            max_rows = 100000 if 'markets' in filename else 50000
            combined = combined.head(max_rows)
            
            combined.to_csv(file_path, index=False)
            print(f"✅ Updated {filename}: {len(df)} new rows, {len(combined)} total rows")
            
        except Exception as e:
            print(f"❌ Error updating {filename}: {e}")
            df.to_csv(file_path, index=False)
            print(f"✅ Recreated {filename}: {len(df)} rows")
    else:
        # Створюємо новий файл
        df.to_csv(file_path, index=False)
        print(f"✅ Created {filename}: {len(df)} rows")

def save_tvl_data(tvl_data: Dict):
    """Зберігає TVL дані з часовою міткою - простий формат"""
    if not tvl_data:
        print("⚠️ No TVL data to save")
        return
        
    try:
        current_time = datetime.utcnow().isoformat() + "Z"
        current_date = current_time[:10]  # YYYY-MM-DD
        tvl_records = []
        
        # Загальний поточний TVL
        total_tvl = tvl_data.get('tvl', 0)
        if total_tvl > 0:
            tvl_records.append({
                'fetched_at': current_time,
                'date': current_date,
                'chain': 'total',
                'tvl_usd': float(total_tvl)
            })
        
        # TVL по окремих мережах (поточний)
        chain_tvls = tvl_data.get('chainTvls', {})
        for chain, tvl_value in chain_tvls.items():
            if isinstance(tvl_value, (int, float)) and tvl_value > 0:
                tvl_records.append({
                    'fetched_at': current_time,
                    'date': current_date,
                    'chain': str(chain).lower(),
                    'tvl_usd': float(tvl_value)
                })
        
        if tvl_records:
            df = pd.DataFrame(tvl_records)
            save_data_with_deduplication(df, "extended_tvl_data.csv", ['date', 'chain'])
        else:
            print("⚠️ No valid TVL data to save")
            
    except Exception as e:
        print(f"❌ Error processing TVL data: {e}")
        print("TVL data structure:", tvl_data)

def main():
    """Головна функція - збирає всі дані Extended біржі"""
    print("🚀 Starting Enhanced Extended Exchange data collection...")
    ensure_uploads_dir()
    
    # === 1. ЗБИРАЄМО ДАНІ РИНКІВ ===
    all_markets_data = []
    all_funding_data = []
    all_orderbook_data = []
    
    for chain in ['ethereum', 'starknet']:
        # Markets
        markets = fetch_markets_data(chain)
        if markets:
            normalized = normalize_markets_data(markets, chain)
            all_markets_data.extend(normalized)
            
            # Funding rates
            funding_data = fetch_funding_rates(chain)
            all_funding_data.extend(funding_data)
            
            # Order book для топ ринків
            market_names = [m.get('name', '') for m in markets if m.get('name')]
            orderbook_data = fetch_orderbook_snapshots(chain, market_names)
            all_orderbook_data.extend(orderbook_data)
    
    # Зберігаємо markets data
    if all_markets_data:
        markets_df = pd.DataFrame(all_markets_data)
        save_data_with_deduplication(markets_df, "extended_markets_data.csv", ['unique_id'])
    
    # Зберігаємо funding data
    if all_funding_data:
        funding_df = pd.DataFrame(all_funding_data)
        save_data_with_deduplication(funding_df, "extended_funding_rates.csv", ['chain', 'market', 'funding_time'])
    
    # Зберігаємо orderbook data
    if all_orderbook_data:
        orderbook_df = pd.DataFrame(all_orderbook_data)
        save_data_with_deduplication(orderbook_df, "extended_orderbook_snapshots.csv", ['chain', 'market', 'fetched_at'])
    
    # === 2. ЗБИРАЄМО СТАТИСТИКУ ТОРГІВ ===
    trading_stats = []
    
    # Збираємо дані за останні 30 днів (більше історії)
    for days_back in range(30):
        date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        
        for chain in ['ethereum', 'starknet']:
            # Перевіряємо активність мережі
            start_date = datetime.strptime(ENDPOINTS[chain]['start_date'], '%Y-%m-%d')
            check_date = datetime.strptime(date, '%Y-%m-%d')
            
            if check_date >= start_date:
                stats = fetch_trading_stats(chain, date)
                trading_stats.append(stats)
    
    if trading_stats:
        trading_df = pd.DataFrame(trading_stats)
        save_data_with_deduplication(trading_df, "extended_trading_stats.csv", ['date', 'chain'])
    
    # === 3. ЗБИРАЄМО TVL ДАНІ ===
    tvl_data = fetch_tvl_data()
    if tvl_data:
        save_tvl_data(tvl_data)
    
    print("✅ Enhanced data collection completed!")
    print("📁 Check uploads/ directory for CSV files:")
    print("   - extended_markets_data.csv (markets with prices, volumes, OI)")
    print("   - extended_trading_stats.csv (daily trading statistics)")
    print("   - extended_tvl_data.csv (TVL data)")
    print("   - extended_funding_rates.csv (funding rates history)")
    print("   - extended_orderbook_snapshots.csv (order book depth)")

if __name__ == "__main__":
    main()

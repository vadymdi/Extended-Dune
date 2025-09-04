# enhanced_fetch_data_to_dune.py
"""
Розширений збір даних з Extended API для унікального аналізу біржі
Збирає:
1. Markets data (ціни, обсяги, open interest)
2. Trading statistics (історичні обсяги)
3. TVL дані з контрактів (через DeFiLlama API)
Зберігає все в окремі CSV файли для Dune Analytics
"""

import requests
import pandas as pd
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import json

# === КОНФІГУРАЦІЯ ===
UPLOADS_DIR = "uploads"
TIMEOUT = 30

# API endpoints для різних мереж
ENDPOINTS = {
    'ethereum': {
        'markets': 'https://api.extended.exchange/api/v1/info/markets',
        'trading': 'https://api.extended.exchange/api/v1/exchange/stats/trading',
        'start_date': '2025-03-11'
    },
    'starknet': {
        'markets': 'https://api.starknet.extended.exchange/api/v1/info/markets',
        'trading': 'https://api.starknet.extended.exchange/api/v1/exchange/stats/trading',
        'start_date': '2025-08-10'
    }
}

# DeFiLlama TVL endpoint
DEFILLAMA_TVL_API = "https://api.llama.fi/protocol/extended"

def ensure_uploads_dir():
    """Створює папку uploads якщо її немає"""
    os.makedirs(UPLOADS_DIR, exist_ok=True)
    print("📁 Uploads directory ready")

def fetch_markets_data(chain: str) -> List[Dict]:
    """
    Отримує дані ринків (markets) для конкретної мережі
    Повертає список ринків з цінами, обсягами та open interest
    """
    try:
        url = ENDPOINTS[chain]['markets']
        print(f"🔄 Fetching markets data for {chain}...")
        
        response = requests.get(url, timeout=TIMEOUT)
        response.raise_for_status()
        
        data = response.json()
        markets = data.get("data", []) if isinstance(data, dict) else data
        
        print(f"✅ Got {len(markets)} markets from {chain}")
        return markets or []
        
    except Exception as e:
        print(f"❌ Error fetching {chain} markets: {e}")
        return []

def fetch_trading_stats(chain: str, date: str) -> Dict:
    """
    Отримує статистику торгів для конкретної дати та мережі
    date формат: YYYY-MM-DD
    """
    try:
        url = f"{ENDPOINTS[chain]['trading']}?fromDate={date}&toDate={date}"
        print(f"🔄 Fetching trading stats for {chain} on {date}...")
        
        response = requests.get(url, timeout=TIMEOUT)
        response.raise_for_status()
        
        data = response.json()
        
        # Обробляємо дані статистики
        if isinstance(data, dict) and 'data' in data:
            trading_data = data['data']
            if trading_data:
                total_volume = sum(float(item.get('tradingVolume', 0)) for item in trading_data)
                return {
                    'date': date,
                    'chain': chain,
                    'daily_volume': total_volume,
                    'trades_count': len(trading_data)
                }
        
        return {'date': date, 'chain': chain, 'daily_volume': 0, 'trades_count': 0}
        
    except Exception as e:
        print(f"❌ Error fetching trading stats for {chain}: {e}")
        return {'date': date, 'chain': chain, 'daily_volume': 0, 'trades_count': 0}

def fetch_tvl_data() -> Optional[Dict]:
    """
    Отримує TVL дані з DeFiLlama
    """
    try:
        print("🔄 Fetching TVL data from DeFiLlama...")
        
        response = requests.get(DEFILLAMA_TVL_API, timeout=TIMEOUT)
        response.raise_for_status()
        
        data = response.json()
        print("✅ Got TVL data from DeFiLlama")
        return data
        
    except Exception as e:
        print(f"❌ Error fetching TVL data: {e}")
        return None

def normalize_markets_data(markets: List[Dict], chain: str) -> List[Dict]:
    """
    Нормалізує дані ринків в уніфікований формат
    """
    rows = []
    fetched_at = datetime.utcnow().isoformat() + "Z"
    
    for market in markets:
        if not isinstance(market, dict):
            continue
            
        # Отримуємо статистику ринку
        stats = market.get("marketStats", {})
        
        # Створюємо запис з усіма доступними даними
        row = {
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
        
        # Розраховуємо spread якщо є bid та ask
        if row["bidPrice"] > 0 and row["askPrice"] > 0 and row["lastPrice"] > 0:
            row["spread_pct"] = (row["askPrice"] - row["bidPrice"]) / row["lastPrice"] * 100
            
        rows.append(row)
    
    return rows

def save_markets_data(df: pd.DataFrame):
    """
    Зберігає дані ринків з історією (додає нові записи)
    """
    file_path = os.path.join(UPLOADS_DIR, "extended_markets_data.csv")
    
    if os.path.exists(file_path):
        # Читаємо існуючі дані
        existing = pd.read_csv(file_path)
        
        # Об'єднуємо з новими
        combined = pd.concat([existing, df], ignore_index=True)
        
        # Сортуємо по часу (новіші записи зверху)
        combined = combined.sort_values('fetched_at', ascending=False)
        
        # Обмежуємо кількість записів (останні 50,000 щоб не перевищити ліміт Dune)
        combined = combined.head(50000)
        
        combined.to_csv(file_path, index=False)
        print(f"✅ Updated markets data: {len(combined)} total rows")
    else:
        # Створюємо новий файл
        df.to_csv(file_path, index=False)
        print(f"✅ Created markets data file: {len(df)} rows")

def save_trading_stats(stats_list: List[Dict]):
    """
    Зберігає статистику торгів
    """
    if not stats_list:
        print("⚠️ No trading stats to save")
        return
        
    df = pd.DataFrame(stats_list)
    file_path = os.path.join(UPLOADS_DIR, "extended_trading_stats.csv")
    
    if os.path.exists(file_path):
        existing = pd.read_csv(file_path)
        
        # Уникаємо дублікатів по даті та мережі
        existing_keys = set(zip(existing['date'], existing['chain']))
        new_data = [stat for stat in stats_list 
                   if (stat['date'], stat['chain']) not in existing_keys]
        
        if new_data:
            new_df = pd.DataFrame(new_data)
            combined = pd.concat([existing, new_df], ignore_index=True)
            combined = combined.sort_values('date', ascending=False)
            combined.to_csv(file_path, index=False)
            print(f"✅ Updated trading stats: {len(new_data)} new rows")
        else:
            print("ℹ️ No new trading stats to add")
    else:
        df.to_csv(file_path, index=False)
        print(f"✅ Created trading stats file: {len(df)} rows")

def save_tvl_data(tvl_data: Dict):
    """
    Зберігає TVL дані з часовою міткою
    """
    if not tvl_data:
        print("⚠️ No TVL data to save")
        return
        
    # Додаємо часову мітку
    current_time = datetime.utcnow().isoformat() + "Z"
    
    # Витягуємо TVL по мережах
    tvl_records = []
    
    # Загальний TVL
    total_tvl = tvl_data.get('tvl', 0)
    tvl_records.append({
        'fetched_at': current_time,
        'chain': 'total',
        'tvl_usd': total_tvl
    })
    
    # TVL по окремих мережах
    chain_tvls = tvl_data.get('chainTvls', {})
    for chain, tvl_value in chain_tvls.items():
        tvl_records.append({
            'fetched_at': current_time,
            'chain': chain.lower(),
            'tvl_usd': tvl_value
        })
    
    if tvl_records:
        df = pd.DataFrame(tvl_records)
        file_path = os.path.join(UPLOADS_DIR, "extended_tvl_data.csv")
        
        if os.path.exists(file_path):
            existing = pd.read_csv(file_path)
            combined = pd.concat([existing, df], ignore_index=True)
            combined = combined.sort_values('fetched_at', ascending=False)
            combined = combined.head(10000)  # Зберігаємо останні 10к записів
            combined.to_csv(file_path, index=False)
            print(f"✅ Updated TVL data: {len(df)} new rows")
        else:
            df.to_csv(file_path, index=False)
            print(f"✅ Created TVL data file: {len(df)} rows")

def main():
    """
    Головна функція - збирає всі дані Extended біржі
    """
    print("🚀 Starting Extended Exchange data collection...")
    ensure_uploads_dir()
    
    all_markets_data = []
    trading_stats = []
    
    # === 1. ЗБИРАЄМО ДАНІ РИНКІВ ===
    for chain in ['ethereum', 'starknet']:
        markets = fetch_markets_data(chain)
        if markets:
            normalized = normalize_markets_data(markets, chain)
            all_markets_data.extend(normalized)
    
    if all_markets_data:
        markets_df = pd.DataFrame(all_markets_data)
        save_markets_data(markets_df)
        print(f"📊 Processed {len(all_markets_data)} market records")
    
    # === 2. ЗБИРАЄМО СТАТИСТИКУ ТОРГІВ ===
    # Збираємо дані за останні 7 днів
    for days_back in range(7):
        date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        
        for chain in ['ethereum', 'starknet']:
            # Перевіряємо чи ця мережа була активна на ту дату
            start_date = datetime.strptime(ENDPOINTS[chain]['start_date'], '%Y-%m-%d')
            check_date = datetime.strptime(date, '%Y-%m-%d')
            
            if check_date >= start_date:
                stats = fetch_trading_stats(chain, date)
                trading_stats.append(stats)
    
    if trading_stats:
        save_trading_stats(trading_stats)
    
    # === 3. ЗБИРАЄМО TVL ДАНІ ===
    tvl_data = fetch_tvl_data()
    if tvl_data:
        save_tvl_data(tvl_data)
    
    print("✅ Data collection completed!")
    print("📁 Check uploads/ directory for CSV files")

if __name__ == "__main__":
    main()

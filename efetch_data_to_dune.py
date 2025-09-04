# efetch_data_to_dune.py
"""
Розширений збір даних з Extended API для унікального аналізу біржі
Збирає:
1. Markets data (ціни, обсяги, open interest) - з історією
2. Trading statistics (історичні обсяги) - з історією  
3. TVL дані з контрактів (через DeFiLlama API) - з історією
4. On-chain metrics (через блокчейн API) - нова функція
Зберігає все в окремі CSV файли для Dune Analytics з timestamps
"""

import requests
import pandas as pd
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import json
import time

# === КОНФІГУРАЦІЯ ===
UPLOADS_DIR = "uploads"
TIMEOUT = 30
MAX_RETRIES = 3

# API endpoints для різних мереж
ENDPOINTS = {
    'ethereum': {
        'markets': 'https://api.extended.exchange/api/v1/info/markets',
        'trading': 'https://api.extended.exchange/api/v1/exchange/stats/trading',
        'start_date': '2025-03-11',
        'contract': '0x1cE5D7f52A8aBd23551e91248151CA5A13353C65'
    },
    'starknet': {
        'markets': 'https://api.starknet.extended.exchange/api/v1/info/markets', 
        'trading': 'https://api.starknet.extended.exchange/api/v1/exchange/stats/trading',
        'start_date': '2025-08-10',
        'contract': '0x062da0780fae50d68cecaa5a051606dc21217ba290969b302db4dd99d2e9b470'
    }
}

# DeFiLlama TVL endpoint
DEFILLAMA_TVL_API = "https://api.llama.fi/protocol/extended"

def ensure_uploads_dir():
    """Створює папку uploads якщо її немає"""
    os.makedirs(UPLOADS_DIR, exist_ok=True)
    print("📁 Uploads directory ready")

def retry_request(func, *args, **kwargs):
    """Retry mechanism для API запитів"""
    for attempt in range(MAX_RETRIES):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                raise e
            print(f"⚠️ Attempt {attempt + 1} failed: {e}")
            time.sleep(2 ** attempt)

def fetch_markets_data(chain: str) -> List[Dict]:
    """
    Отримує дані ринків (markets) для конкретної мережі
    Повертає список ринків з цінами, обсягами та open interest
    """
    try:
        url = ENDPOINTS[chain]['markets']
        print(f"🔄 Fetching markets data for {chain}...")
        
        def make_request():
            response = requests.get(url, timeout=TIMEOUT)
            response.raise_for_status()
            return response.json()
        
        data = retry_request(make_request)
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
        
        def make_request():
            response = requests.get(url, timeout=TIMEOUT)
            response.raise_for_status()
            return response.json()
        
        data = retry_request(make_request)
        
        # Обробляємо дані статистики
        if isinstance(data, dict) and 'data' in data:
            trading_data = data['data']
            if trading_data:
                total_volume = sum(float(item.get('tradingVolume', 0)) for item in trading_data)
                trades_count = len(trading_data)
                return {
                    'date': date,
                    'chain': chain,
                    'daily_volume': total_volume,
                    'trades_count': trades_count,
                    'timestamp': datetime.utcnow().isoformat() + "Z"
                }
        
        return {
            'date': date, 
            'chain': chain, 
            'daily_volume': 0, 
            'trades_count': 0,
            'timestamp': datetime.utcnow().isoformat() + "Z"
        }
        
    except Exception as e:
        print(f"❌ Error fetching trading stats for {chain}: {e}")
        return {
            'date': date, 
            'chain': chain, 
            'daily_volume': 0, 
            'trades_count': 0,
            'timestamp': datetime.utcnow().isoformat() + "Z"
        }

def fetch_tvl_data() -> Optional[Dict]:
    """
    Отримує TVL дані з DeFiLlama
    """
    try:
        print("🔄 Fetching TVL data from DeFiLlama...")
        
        def make_request():
            response = requests.get(DEFILLAMA_TVL_API, timeout=TIMEOUT)
            response.raise_for_status()
            return response.json()
        
        data = retry_request(make_request)
        print("✅ Got TVL data from DeFiLlama")
        return data
        
    except Exception as e:
        print(f"❌ Error fetching TVL data: {e}")
        return None

def fetch_onchain_metrics(chain: str) -> Dict:
    """
    Збирає додаткові on-chain метрики
    """
    try:
        print(f"🔄 Fetching on-chain metrics for {chain}...")
        
        # Тут можна додати запити до блокчейн API
        # Поки що повертаємо базові метрики
        current_time = datetime.utcnow().isoformat() + "Z"
        
        metrics = {
            'timestamp': current_time,
            'chain': chain,
            'contract_address': ENDPOINTS[chain]['contract'],
            # Тут будуть додані реальні on-chain дані
            'active_users_24h': 0,  # Буде реалізовано пізніше
            'total_transactions': 0,  # Буде реалізовано пізніше
        }
        
        return metrics
        
    except Exception as e:
        print(f"❌ Error fetching on-chain metrics for {chain}: {e}")
        return {}

def normalize_markets_data(markets: List[Dict], chain: str) -> List[Dict]:
    """
    Нормалізує дані ринків в уніфікований формат з додаванням timestamp
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
            "timestamp": fetched_at,
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

def append_to_csv(df: pd.DataFrame, file_path: str, max_rows: int = 100000):
    """
    Універсальна функція для додавання даних до CSV з обмеженням розміру
    """
    if os.path.exists(file_path):
        # Читаємо існуючі дані
        try:
            existing = pd.read_csv(file_path)
            print(f"📖 Found existing file with {len(existing)} rows")
            
            # Об'єднуємо з новими даними
            combined = pd.concat([existing, df], ignore_index=True)
            
            # Сортуємо по timestamp (новіші записи зверху)
            if 'timestamp' in combined.columns:
                combined = combined.sort_values('timestamp', ascending=False)
            elif 'fetched_at' in combined.columns:
                combined = combined.sort_values('fetched_at', ascending=False)
            
            # Обмежуємо кількість записів
            combined = combined.head(max_rows)
            
            # Зберігаємо
            combined.to_csv(file_path, index=False)
            print(f"✅ Updated {os.path.basename(file_path)}: {len(combined)} total rows")
            
        except Exception as e:
            print(f"⚠️ Error reading existing file: {e}")
            df.to_csv(file_path, index=False)
            print(f"✅ Created new {os.path.basename(file_path)}: {len(df)} rows")
    else:
        # Створюємо новий файл
        df.to_csv(file_path, index=False)
        print(f"✅ Created {os.path.basename(file_path)}: {len(df)} rows")

def save_markets_data(df: pd.DataFrame):
    """Зберігає дані ринків з історією"""
    file_path = os.path.join(UPLOADS_DIR, "extended_markets_data.csv")
    append_to_csv(df, file_path, max_rows=50000)

def save_trading_stats(stats_list: List[Dict]):
    """Зберігає статистику торгів з історією"""
    if not stats_list:
        print("⚠️ No trading stats to save")
        return
    
    df = pd.DataFrame(stats_list)
    file_path = os.path.join(UPLOADS_DIR, "extended_trading_stats.csv")
    
    # Для trading stats перевіряємо дублікати по даті та мережі
    if os.path.exists(file_path):
        try:
            existing = pd.read_csv(file_path)
            existing_keys = set(zip(existing['date'], existing['chain']))
            new_data = [stat for stat in stats_list 
                       if (stat['date'], stat['chain']) not in existing_keys]
            
            if new_data:
                new_df = pd.DataFrame(new_data)
                append_to_csv(new_df, file_path, max_rows=10000)
            else:
                print("ℹ️ No new trading stats to add")
        except Exception as e:
            print(f"⚠️ Error processing trading stats: {e}")
            append_to_csv(df, file_path, max_rows=10000)
    else:
        append_to_csv(df, file_path, max_rows=10000)

def save_tvl_data(tvl_data: Dict):
    """Зберігає TVL дані з історією та покращеною обробкою"""
    if not tvl_data:
        print("⚠️ No TVL data to save")
        return
        
    current_time = datetime.utcnow().isoformat() + "Z"
    date_str = datetime.utcnow().strftime('%Y-%m-%d')
    
    tvl_records = []
    
    # Загальний TVL
    total_tvl = float(tvl_data.get('tvl', 0) or 0)
    tvl_records.append({
        'timestamp': current_time,
        'date': date_str,
        'chain': 'total',
        'tvl_usd': total_tvl
    })
    
    # TVL по окремих мережах
    chain_tvls = tvl_data.get('chainTvls', {})
    for chain, tvl_value in chain_tvls.items():
        chain_clean = chain.lower().replace('-', '_')
        tvl_records.append({
            'timestamp': current_time,
            'date': date_str,
            'chain': chain_clean,
            'tvl_usd': float(tvl_value or 0)
        })
    
    # Додаємо окремо ethereum та starknet якщо їх немає
    chains_found = set(rec['chain'] for rec in tvl_records)
    
    if 'ethereum' not in chains_found and 'Ethereum' in chain_tvls:
        tvl_records.append({
            'timestamp': current_time,
            'date': date_str, 
            'chain': 'ethereum',
            'tvl_usd': float(chain_tvls.get('Ethereum', 0) or 0)
        })
        
    if 'starknet' not in chains_found and 'Starknet' in chain_tvls:
        tvl_records.append({
            'timestamp': current_time,
            'date': date_str,
            'chain': 'starknet', 
            'tvl_usd': float(chain_tvls.get('Starknet', 0) or 0)
        })
    
    if tvl_records:
        df = pd.DataFrame(tvl_records)
        file_path = os.path.join(UPLOADS_DIR, "extended_tvl_data.csv")
        append_to_csv(df, file_path, max_rows=20000)

def save_onchain_metrics(metrics_list: List[Dict]):
    """Зберігає on-chain метрики"""
    if not metrics_list:
        return
        
    df = pd.DataFrame(metrics_list)
    file_path = os.path.join(UPLOADS_DIR, "extended_onchain_metrics.csv")
    append_to_csv(df, file_path, max_rows=10000)

def main():
    """
    Головна функція - збирає всі дані Extended біржі
    """
    print("🚀 Starting Extended Exchange data collection...")
    print(f"🕒 Current time: {datetime.utcnow().isoformat()}Z")
    ensure_uploads_dir()
    
    all_markets_data = []
    trading_stats = []
    onchain_metrics = []
    
    # === 1. ЗБИРАЄМО ДАНІ РИНКІВ ===
    print("\n=== 📊 COLLECTING MARKETS DATA ===")
    for chain in ['ethereum', 'starknet']:
        markets = fetch_markets_data(chain)
        if markets:
            normalized = normalize_markets_data(markets, chain)
            all_markets_data.extend(normalized)
            
            # Збираємо also on-chain метрики
            metrics = fetch_onchain_metrics(chain)
            if metrics:
                onchain_metrics.append(metrics)
    
    if all_markets_data:
        markets_df = pd.DataFrame(all_markets_data)
        save_markets_data(markets_df)
        print(f"📊 Processed {len(all_markets_data)} market records")
    
    # === 2. ЗБИРАЄМО СТАТИСТИКУ ТОРГІВ ===
    print("\n=== 📈 COLLECTING TRADING STATISTICS ===")
    # Збираємо дані за останні 14 днів для більш повної картини
    for days_back in range(14):
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
        print(f"📈 Processed {len(trading_stats)} trading stat records")
    
    # === 3. ЗБИРАЄМО TVL ДАНІ ===
    print("\n=== 💰 COLLECTING TVL DATA ===")
    tvl_data = fetch_tvl_data()
    if tvl_data:
        save_tvl_data(tvl_data)
        print("💰 Processed TVL data")
    
    # === 4. ЗБЕРІГАЄМО ON-CHAIN МЕТРИКИ ===
    if onchain_metrics:
        save_onchain_metrics(onchain_metrics)
        print(f"⛓️ Processed {len(onchain_metrics)} on-chain metric records")
    
    # === 5. SUMMARY ===
    print("\n=== ✅ COLLECTION COMPLETED ===")
    print("📁 Check uploads/ directory for CSV files:")
    
    for filename in os.listdir(UPLOADS_DIR):
        if filename.endswith('.csv'):
            file_path = os.path.join(UPLOADS_DIR, filename)
            file_size = os.path.getsize(file_path)
            with open(file_path, 'r') as f:
                line_count = sum(1 for _ in f) - 1  # -1 для header
            print(f"  📄 {filename}: {line_count} rows, {file_size} bytes")

if __name__ == "__main__":
    main()

# fetch_data_to_dune.py
"""
Адаптовано з fetch_volume_fees.py для роботи з Dune Analytics
Зберігає дані в /uploads/ папку для автоматичного завантаження в Dune
"""
import requests
import pandas as pd
import os
from datetime import datetime

API_URL = "https://api.starknet.extended.exchange/api/v1/info/markets"
UPLOADS_DIR = "uploads"
OUT_FILE = os.path.join(UPLOADS_DIR, "extended_markets_data.csv")
TIMEOUT = 20

def fetch_markets():
    """Отримує дані з Extended API"""
    try:
        r = requests.get(API_URL, timeout=TIMEOUT)
        r.raise_for_status()
        payload = r.json()
        markets = payload.get("data") if isinstance(payload, dict) else payload
        return markets or []
    except Exception as e:
        print("❌ API Error:", e)
        return []

def normalize_markets(markets):
    """Нормалізує дані в потрібний формат"""
    rows = []
    fetched_at = datetime.utcnow().isoformat() + "Z"
    
    for m in markets:
        stats = m.get("marketStats") if isinstance(m, dict) else {}
        rows.append({
            "fetched_at": fetched_at,
            "market": m.get("name") if isinstance(m, dict) else str(m),
            "dailyVolume": stats.get("dailyVolume") or 0,
            "dailyVolumeBase": stats.get("dailyVolumeBase") or 0,
            "openInterest": stats.get("openInterest") or stats.get("openInterestBase") or 0,
            "fundingRate": stats.get("fundingRate") or 0,
            "lastPrice": stats.get("lastPrice") or 0,
            "bidPrice": stats.get("bidPrice") or 0,
            "askPrice": stats.get("askPrice") or 0,
            "markPrice": stats.get("markPrice") or 0,
            "indexPrice": stats.get("indexPrice") or 0
        })
    return rows

def save_for_dune(new_df):
    """Зберігає дані в uploads/ папку для Dune"""
    os.makedirs(UPLOADS_DIR, exist_ok=True)
    
    # Для Dune краще зберігати всі дані з timestamp
    # Dune сам керуватиме дедуплікацією через SQL queries
    if os.path.exists(OUT_FILE):
        # Читаємо існуючі дані
        existing = pd.read_csv(OUT_FILE)
        # Додаємо нові дані
        combined = pd.concat([existing, new_df], ignore_index=True)
        
        # Сортуємо за часом (найновіші зверху)
        combined['fetched_at_sort'] = pd.to_datetime(combined['fetched_at'], errors='coerce')
        combined = combined.sort_values('fetched_at_sort', ascending=False)
        combined = combined.drop(columns=['fetched_at_sort'])
        
        # Зберігаємо тільки останні 10000 записів щоб не перевищити ліміт Dune
        combined = combined.head(10000)
        combined.to_csv(OUT_FILE, index=False)
        print(f"✅ Updated CSV with {len(combined)} total rows")
    else:
        # Створюємо новий файл
        new_df.to_csv(OUT_FILE, index=False)
        print(f"✅ Created new CSV with {len(new_df)} rows")

def main():
    """Головна функція"""
    print("🚀 Fetching Extended markets data...")
    
    markets = fetch_markets()
    rows = normalize_markets(markets)
    
    if not rows:
        print("❌ No data received from API")
        return
    
    df = pd.DataFrame(rows)
    save_for_dune(df)
    print(f"📊 Processed {len(rows)} markets")

if __name__ == "__main__":
    main()

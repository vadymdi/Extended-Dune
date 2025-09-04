#!/usr/bin/env python3
"""
Скрипт для загрузки CSV файлів в Dune Analytics
"""

import os
import sys
from pathlib import Path
from dune_client.client import DuneClient
from dune_client.api.table import InsertTableRequest
from dune_client.types import Address
import pandas as pd

# Додаємо папку проекту в sys.path
sys.path.append(str(Path(__file__).parent.parent))

def get_dune_client():
    """Створює клієнт Dune з API ключем"""
    api_key = os.getenv('DUNE_API_KEY')
    if not api_key:
        raise ValueError("❌ DUNE_API_KEY environment variable not set!")
    
    print("✅ Dune API key found")
    return DuneClient(api_key)

def upload_csv_to_dune(client, csv_path: str, table_name: str):
    """Загружає CSV файл в Dune таблицю"""
    if not os.path.exists(csv_path):
        print(f"⚠️ File not found: {csv_path}")
        return False
    
    try:
        # Читаємо CSV
        df = pd.read_csv(csv_path)
        print(f"📊 Loading {csv_path}: {len(df)} rows")
        
        if len(df) == 0:
            print(f"⚠️ Empty file: {csv_path}")
            return False
        
        # Створюємо запит для загрузки
        table_request = InsertTableRequest(
            table_name=table_name,
            data=df,
            is_private=False  # Публічна таблиця
        )
        
        # Загружаємо в Dune
        response = client.insert_table(table_request)
        print(f"✅ Uploaded {csv_path} to dune.vadymdi.dataset_{table_name}")
        return True
        
    except Exception as e:
        print(f"❌ Error uploading {csv_path}: {e}")
        return False

def main():
    """Головна функція загрузки"""
    uploads_dir = "uploads"
    
    if not os.path.exists(uploads_dir):
        print("❌ uploads/ directory not found")
        return False
    
    # Отримуємо клієнт Dune
    try:
        dune = get_dune_client()
    except Exception as e:
        print(f"❌ Failed to create Dune client: {e}")
        return False
    
    # Мапінг файлів до назв таблиць в Dune
    file_mappings = {
        "extended_markets_data.csv": "extended_markets_data",
        "extended_trading_stats.csv": "extended_trading_stats", 
        "extended_tvl_data.csv": "extended_tvl_data",
        "extended_onchain_metrics.csv": "extended_onchain_metrics"
    }
    
    success_count = 0
    total_files = 0
    
    print("🚀 Starting upload to Dune Analytics...")
    print("=" * 50)
    
    # Загружаємо всі знайдені файли
    for filename, table_name in file_mappings.items():
        file_path = os.path.join(uploads_dir, filename)
        
        if os.path.exists(file_path):
            total_files += 1
            if upload_csv_to_dune(dune, file_path, table_name):
                success_count += 1
            print("-" * 30)
    
    # Загружаємо також FEDFUNDS.csv якщо є
    fedfunds_path = os.path.join(uploads_dir, "FEDFUNDS.csv")
    if os.path.exists(fedfunds_path):
        total_files += 1
        if upload_csv_to_dune(dune, fedfunds_path, "fedfunds"):
            success_count += 1
    
    print("=" * 50)
    print(f"📊 Upload Summary: {success_count}/{total_files} files uploaded successfully")
    
    if success_count == total_files and total_files > 0:
        print("✅ All uploads completed successfully!")
        return True
    elif success_count > 0:
        print("⚠️ Some uploads completed with warnings")
        return True
    else:
        print("❌ Upload failed!")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

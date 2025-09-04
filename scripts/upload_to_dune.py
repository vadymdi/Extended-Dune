#!/usr/bin/env python3
"""
Скрипт для загрузки CSV файлів в Dune Analytics через REST API
"""

import os
import sys
import requests
import pandas as pd
from pathlib import Path

def upload_csv_to_dune(csv_path: str, table_name: str, api_key: str):
    """Загружає CSV файл в Dune через REST API"""
    
    if not os.path.exists(csv_path):
        print(f"⚠️ File not found: {csv_path}")
        return False
    
    try:
        # Перевіряємо розмір файлу (ліміт 200MB)
        file_size = os.path.getsize(csv_path)
        file_size_mb = file_size / (1024 * 1024)
        
        if file_size_mb > 200:
            print(f"❌ File too large: {file_size_mb:.1f}MB (limit: 200MB)")
            return False
            
        print(f"📊 Uploading {csv_path} ({file_size_mb:.2f}MB) as table '{table_name}'...")
        
        # URL для загрузки
        url = "https://api.dune.com/api/v1/table/upload/csv"
        
        # Headers
        headers = {
            'X-DUNE-API-KEY': api_key
        }
        
        # Підготовка файлу та даних
        with open(csv_path, 'rb') as f:
            files = {
                'data': (os.path.basename(csv_path), f, 'text/csv')
            }
            
            data = {
                'table_name': table_name,
                'description': f'Extended Exchange data: {table_name}',
                'is_private': 'false'
            }
            
            # Відправляємо запит
            response = requests.post(url, headers=headers, files=files, data=data)
        
        if response.status_code == 200:
            print(f"✅ Successfully uploaded to dune.vadymdi.dataset_{table_name}")
            return True
        else:
            print(f"❌ Upload failed: {response.status_code}")
            try:
                error_detail = response.json()
                print(f"   Error: {error_detail}")
            except:
                print(f"   Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error uploading {csv_path}: {e}")
        return False

def main():
    """Головна функція загрузки"""
    
    # Перевіряємо API ключ
    api_key = os.getenv('DUNE_API_KEY')
    if not api_key:
        print("❌ DUNE_API_KEY environment variable not set!")
        return False
    
    print("✅ Dune API key found")
    
    uploads_dir = "uploads"
    
    if not os.path.exists(uploads_dir):
        print("❌ uploads/ directory not found")
        return False
    
    # Мапінг файлів до назв таблиць в Dune
    file_mappings = {
        "extended_markets_data.csv": "extended_markets_data",
        "extended_trading_stats.csv": "extended_trading_stats", 
        "extended_tvl_data.csv": "extended_tvl_data",
        "FEDFUNDS.csv": "fedfunds"
    }
    
    success_count = 0
    total_files = 0
    
    print("🚀 Starting upload to Dune Analytics...")
    print("=" * 50)
    
    # Перевіряємо які файли є в uploads
    available_files = os.listdir(uploads_dir)
    csv_files = [f for f in available_files if f.endswith('.csv')]
    
    print(f"📁 Found {len(csv_files)} CSV files in uploads/:")
    for f in csv_files:
        file_path = os.path.join(uploads_dir, f)
        size = os.path.getsize(file_path) / 1024  # KB
        print(f"   📄 {f} ({size:.1f}KB)")
    print("-" * 30)
    
    # Загружаємо відомі файли
    for filename, table_name in file_mappings.items():
        file_path = os.path.join(uploads_dir, filename)
        
        if os.path.exists(file_path):
            total_files += 1
            if upload_csv_to_dune(file_path, table_name, api_key):
                success_count += 1
            print("-" * 30)
    
    # Загружаємо інші CSV файли які знайшли
    for csv_file in csv_files:
        if csv_file not in file_mappings:
            file_path = os.path.join(uploads_dir, csv_file)
            table_name = csv_file.replace('.csv', '').lower()
            
            total_files += 1
            print(f"📄 Found additional file: {csv_file}")
            if upload_csv_to_dune(file_path, table_name, api_key):
                success_count += 1
            print("-" * 30)
    
    print("=" * 50)
    print(f"📊 Upload Summary: {success_count}/{total_files} files uploaded successfully")
    
    if total_files == 0:
        print("⚠️ No CSV files found to upload")
        return False
    elif success_count == total_files:
        print("✅ All uploads completed successfully!")
        return True
    elif success_count > 0:
        print("⚠️ Some uploads completed with errors")
        return True
    else:
        print("❌ All uploads failed!")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

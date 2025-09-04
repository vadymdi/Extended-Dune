#!/usr/bin/env python3
"""
Скрипт для загрузки CSV файлів в Dune Analytics
Повертаємося до dune-client але з правильним API
"""

import os
import sys
import pandas as pd
from pathlib import Path

def upload_csv_to_dune_simple(csv_path: str, table_name: str, api_key: str):
    """Загружає CSV через dune-client (якщо встановлено) або через curl"""
    
    if not os.path.exists(csv_path):
        print(f"⚠️ File not found: {csv_path}")
        return False
    
    try:
        # Спробуємо через dune-client
        try:
            from dune_client.client import DuneClient
            
            print(f"📊 Uploading {csv_path} as table '{table_name}' via dune-client...")
            
            # Читаємо дані
            df = pd.read_csv(csv_path)
            
            # Очищаємо назви колонок від спеціальних символів
            df.columns = [col.strip().replace(' ', '_').replace('-', '_') for col in df.columns]
            
            # Створюємо клієнт
            dune = DuneClient(api_key)
            
            # Загружаємо через upload_csv метод
            result = dune.upload_csv(
                data=df,
                table_name=table_name,
                description=f"Extended Exchange data: {table_name}"
            )
            
            print(f"✅ Successfully uploaded to dune.vadymdi.dataset_{table_name}")
            return True
            
        except ImportError:
            print("⚠️ dune-client not installed, trying curl method...")
            return upload_via_curl(csv_path, table_name, api_key)
        except Exception as e:
            print(f"❌ dune-client upload failed: {e}")
            print("🔄 Trying curl method as fallback...")
            return upload_via_curl(csv_path, table_name, api_key)
            
    except Exception as e:
        print(f"❌ Error uploading {csv_path}: {e}")
        return False

def upload_via_curl(csv_path: str, table_name: str, api_key: str):
    """Альтернативний метод через curl"""
    try:
        import subprocess
        
        print(f"📊 Uploading {csv_path} as table '{table_name}' via curl...")
        
        # Створюємо curl команду
        curl_cmd = [
            'curl',
            '-X', 'POST',
            'https://api.dune.com/api/v1/table/upload/csv',
            '-H', f'X-DUNE-API-KEY: {api_key}',
            '-F', f'data=@{csv_path}',
            '-F', f'table_name={table_name}',
            '-F', f'description=Extended Exchange data: {table_name}',
            '-F', 'is_private=false'
        ]
        
        # Виконуємо команду
        result = subprocess.run(curl_cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"✅ Successfully uploaded via curl to dune.vadymdi.dataset_{table_name}")
            return True
        else:
            print(f"❌ curl upload failed:")
            print(f"   stdout: {result.stdout}")
            print(f"   stderr: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ curl method failed: {e}")
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
        try:
            df = pd.read_csv(file_path)
            size = os.path.getsize(file_path) / 1024  # KB
            print(f"   📄 {f} ({len(df)} rows, {size:.1f}KB)")
        except:
            print(f"   📄 {f} (unable to read)")
    print("-" * 30)
    
    # Загружаємо відомі файли
    for filename, table_name in file_mappings.items():
        file_path = os.path.join(uploads_dir, filename)
        
        if os.path.exists(file_path):
            total_files += 1
            if upload_csv_to_dune_simple(file_path, table_name, api_key):
                success_count += 1
            print("-" * 30)
    
    print("=" * 50)
    print(f"📊 Upload Summary: {success_count}/{total_files} files uploaded successfully")
    
    if total_files == 0:
        print("⚠️ No matching CSV files found to upload")
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

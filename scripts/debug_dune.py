#!/usr/bin/env python3
"""
Скрипт для діагностики проблем з Dune Analytics
"""

import os
import requests
import pandas as pd

def check_dune_api_key():
    """Перевіряє валідність API ключа Dune"""
    api_key = os.getenv('DUNE_API_KEY')
    if not api_key:
        print("❌ DUNE_API_KEY не знайдено!")
        return False
    
    print(f"✅ API ключ знайдено: {api_key[:10]}...{api_key[-5:]}")
    
    # Тестуємо API ключ
    headers = {'X-DUNE-API-KEY': api_key}
    
    try:
        # Спробуємо отримати список наших таблиць
        response = requests.get('https://api.dune.com/api/v1/table', headers=headers, timeout=10)
        
        if response.status_code == 200:
            print("✅ API ключ валідний!")
            tables = response.json()
            print(f"📊 Знайдено {len(tables)} таблиць у вашому акаунті")
            
            # Показуємо існуючі таблиці
            for table in tables:
                table_name = table.get('name', 'unknown')
                if 'extended' in table_name or 'fedfunds' in table_name:
                    print(f"   📄 {table_name}")
                    
        else:
            print(f"❌ API ключ не працює: {response.status_code}")
            print(f"   Відповідь: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Помилка при тестуванні API: {e}")
        return False
    
    return True

def check_csv_files():
    """Перевіряє наші CSV файли"""
    uploads_dir = "uploads"
    
    if not os.path.exists(uploads_dir):
        print("❌ Папка uploads/ не існує!")
        return False
    
    csv_files = [f for f in os.listdir(uploads_dir) if f.endswith('.csv')]
    
    if not csv_files:
        print("❌ CSV файли не знайдено!")
        return False
    
    print(f"📁 Знайдено {len(csv_files)} CSV файлів:")
    
    for filename in csv_files:
        filepath = os.path.join(uploads_dir, filename)
        try:
            df = pd.read_csv(filepath)
            size_mb = os.path.getsize(filepath) / (1024 * 1024)
            
            print(f"   📄 {filename}:")
            print(f"      📊 Рядків: {len(df)}")
            print(f"      📏 Розмір: {size_mb:.2f} MB")
            print(f"      🏷️ Колонки: {', '.join(df.columns[:5])}{'...' if len(df.columns) > 5 else ''}")
            
            # Перевіряємо на спеціальні символи у назвах колонок
            bad_columns = [col for col in df.columns if col[0].isdigit() or not col.replace('_', '').replace('-', '').isalnum()]
            if bad_columns:
                print(f"      ⚠️ Проблемні колонки: {bad_columns}")
            
            # Показуємо приклад даних
            if len(df) > 0:
                print(f"      📋 Останній запис: {dict(df.iloc[0].head(3))}")
            
        except Exception as e:
            print(f"   ❌ Не вдалося прочитати {filename}: {e}")
    
    return True

def test_upload_small_file():
    """Тестує загрузку маленького файлу"""
    api_key = os.getenv('DUNE_API_KEY')
    if not api_key:
        return False
    
    # Створюємо тестовий файл
    test_data = pd.DataFrame({
        'timestamp': ['2024-01-01T00:00:00Z'],
        'test_value': [123],
        'test_string': ['hello']
    })
    
    test_file = "uploads/test_upload.csv"
    test_data.to_csv(test_file, index=False)
    
    print("🧪 Тестуємо загрузку маленького файлу...")
    
    try:
        with open(test_file, 'rb') as f:
            files = {'data': (os.path.basename(test_file), f, 'text/csv')}
            data = {
                'table_name': 'test_upload',
                'description': 'Test upload',
                'is_private': 'false'
            }
            headers = {'X-DUNE-API-KEY': api_key}
            
            response = requests.post(
                'https://api.dune.com/api/v1/table/upload/csv',
                headers=headers,
                files=files,
                data=data,
                timeout=30
            )
        
        print(f"📤 Статус відповіді: {response.status_code}")
        print(f"📄 Відповідь: {response.text}")
        
        if response.status_code == 200:
            print("✅ Тестова загрузка успішна!")
        else:
            print("❌ Тестова загрузка не вдалася!")
        
        # Видаляємо тестовий файл
        os.remove(test_file)
        
    except Exception as e:
        print(f"❌ Помилка тестової загрузки: {e}")
        return False
    
    return response.status_code == 200

def main():
    """Головна функція діагностики"""
    print("🔍 Діагностика проблем з Dune Analytics")
    print("=" * 50)
    
    # 1. Перевіряємо API ключ
    print("\n1️⃣ Перевірка API ключа...")
    api_valid = check_dune_api_key()
    
    # 2. Перевіряємо CSV файли
    print("\n2️⃣ Перевірка CSV файлів...")
    files_valid = check_csv_files()
    
    # 3. Тестуємо загрузку
    if api_valid and files_valid:
        print("\n3️⃣ Тестування загрузки...")
        upload_works = test_upload_small_file()
    
    print("\n" + "=" * 50)
    print("📋 Результати діагностики:")
    print(f"   API ключ: {'✅ ОК' if api_valid else '❌ Проблема'}")
    print(f"   CSV файли: {'✅ ОК' if files_valid else '❌ Проблема'}")
    
    if api_valid and files_valid:
        print(f"   Тест загрузки: {'✅ ОК' if upload_works else '❌ Проблема'}")
        
        if upload_works:
            print("\n💡 Загрузка працює! Перевірте:")
            print("   1. Чи правильно відображаються таблиці в Dune UI")
            print("   2. Чи оновлюються дані в ваших дашбордах")
            print("   3. Можливо потрібен час на синхронізацію")
        else:
            print("\n💡 Рекомендації:")
            print("   1. Перевірте права доступу API ключа")
            print("   2. Перевірте тарифний план (чи підтримується загрузка)")
            print("   3. Спробуйте загрузити через Dune UI вручну")

if __name__ == "__main__":
    main()

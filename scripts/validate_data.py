#!/usr/bin/env python3
"""
Скрипт для валідації зібраних даних перед відправкою в Dune
"""

import os
import pandas as pd
import sys
from datetime import datetime

def validate_data():
    """Валідує всі CSV файли в uploads директорії"""
    
    uploads_dir = 'uploads'
    
    if not os.path.exists(uploads_dir):
        print('❌ No uploads directory found')
        return False
    
    csv_files = [f for f in os.listdir(uploads_dir) if f.endswith('.csv')]
    
    if not csv_files:
        print('❌ No CSV files found')
        return False
    
    print('📊 Data validation summary:')
    print('=' * 60)
    
    total_rows = 0
    valid_files = 0
    
    for filename in csv_files:
        filepath = os.path.join(uploads_dir, filename)
        try:
            df = pd.read_csv(filepath)
            rows = len(df)
            total_rows += rows
            size_mb = os.path.getsize(filepath) / (1024*1024)
            
            print(f'📄 {filename}:')
            print(f'   Rows: {rows:,}')
            print(f'   Size: {size_mb:.2f} MB')
            
            # Перевіряємо наявність timestamp колонок
            if 'timestamp' in df.columns or 'fetched_at' in df.columns:
                time_col = 'timestamp' if 'timestamp' in df.columns else 'fetched_at'
                latest = df[time_col].max()
                oldest = df[time_col].min()
                print(f'   Time range: {oldest} to {latest}')
            else:
                print('   ⚠️  No timestamp column found')
            
            # Показуємо перші кілька колонок
            columns_preview = ', '.join(df.columns[:5])
            if len(df.columns) > 5:
                columns_preview += f' ... (+{len(df.columns)-5} more)'
            print(f'   Columns: {columns_preview}')
            
            # Перевіряємо на пусті дані
            if rows == 0:
                print('   ❌ Empty file!')
            else:
                print('   ✅ Valid data')
                valid_files += 1
                
            print('-' * 60)
            
        except Exception as e:
            print(f'❌ Error validating {filename}: {e}')
            print('-' * 60)
    
    print(f'✅ Summary: {valid_files}/{len(csv_files)} valid files, {total_rows:,} total rows')
    
    if total_rows == 0:
        print('❌ No data collected!')
        return False
        
    if valid_files == 0:
        print('❌ No valid files!')
        return False
    
    print('✅ Data validation passed')
    return True

if __name__ == "__main__":
    success = validate_data()
    sys.exit(0 if success else 1)

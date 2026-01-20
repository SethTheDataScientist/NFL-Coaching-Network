#!/usr/bin/env python3
"""
Reformat Existing NFL Staff CSV Files
======================================

This script reformats the raw CSV output from test_scraper.py into clean DataFrames.

Usage:
    python reformat_existing.py input.csv [--format wide|long|both] [--output dir]

Examples:
    # Wide format only
    python reformat_existing.py nfl_coaching_staff_20260112_094256.csv
    
    # Long format only
    python reformat_existing.py nfl_coaching_staff_20260112_094256.csv --format long
    
    # Both formats
    python reformat_existing.py nfl_coaching_staff_20260112_094256.csv --format both
    
    # Custom output directory
    python reformat_existing.py input.csv --output ./cleaned_data
"""

import pandas as pd
import ast
import sys
import argparse
from pathlib import Path
from datetime import datetime


def reformat_to_wide(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Convert raw CSV to WIDE format DataFrame.
    
    One row per team, one column per staff position.
    """
    formatted_rows = []
    
    for idx, row in df_raw.iterrows():
        # First cell contains [team, year]
        try:
            team_year = ast.literal_eval(row[0])
            team_name = team_year[0]
            year = team_year[1]
        except:
            continue
        
        # Create base row
        record = {'Team': team_name, 'Year': year}
        
        # Process remaining cells
        for cell in row[1:]:
            if pd.isna(cell) or cell == '':
                continue
            
            try:
                staff_dict = ast.literal_eval(cell)
                if isinstance(staff_dict, dict) and 'Role' in staff_dict and 'Name' in staff_dict:
                    role = staff_dict['Role'].strip()
                    name = staff_dict['Name'].strip()
                    
                    # Skip template artifacts
                    if role in ['v', 't', 'e'] or not role:
                        continue
                    
                    # Handle duplicate roles
                    original_role = role
                    counter = 2
                    while role in record:
                        role = f"{original_role}_{counter}"
                        counter += 1
                    
                    record[role] = name if name else None
            except:
                continue
        
        formatted_rows.append(record)
    
    # Create DataFrame
    df = pd.DataFrame(formatted_rows)
    
    # Reorder columns: Team and Year first, then alphabetically
    if len(df.columns) > 2:
        meta_cols = ['Team', 'Year']
        staff_cols = sorted([col for col in df.columns if col not in meta_cols])
        df = df[meta_cols + staff_cols]
    
    return df


def reformat_to_long(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Convert raw CSV to LONG format DataFrame.
    
    One row per staff member.
    Columns: Team, Year, Role, Name
    """
    formatted_rows = []
    
    for idx, row in df_raw.iterrows():
        # First cell contains [team, year]
        try:
            team_year = ast.literal_eval(row[0])
            team_name = team_year[0]
            year = team_year[1]
        except:
            continue
        
        # Process each staff member as a separate row
        for cell in row[1:]:
            if pd.isna(cell) or cell == '':
                continue
            
            try:
                staff_dict = ast.literal_eval(cell)
                if isinstance(staff_dict, dict) and 'Role' in staff_dict and 'Name' in staff_dict:
                    role = staff_dict['Role'].strip()
                    name = staff_dict['Name'].strip()
                    
                    # Skip empty or artifacts
                    if role in ['v', 't', 'e'] or not role or not name:
                        continue
                    
                    formatted_rows.append({
                        'Team': team_name,
                        'Year': year,
                        'Role': role,
                        'Name': name
                    })
            except:
                continue
    
    return pd.DataFrame(formatted_rows)


def save_dataframe(df: pd.DataFrame, output_dir: Path, base_name: str):
    """Save DataFrame to CSV and Excel"""
    output_dir.mkdir(exist_ok=True, parents=True)
    
    # CSV
    csv_file = output_dir / f"{base_name}.csv"
    df.to_csv(csv_file, index=False)
    print(f"  ✅ CSV: {csv_file}")
    
    # Excel
    excel_file = output_dir / f"{base_name}.xlsx"
    df.to_excel(excel_file, index=False, engine='openpyxl')
    print(f"  ✅ Excel: {excel_file}")


def print_summary(df: pd.DataFrame, format_type: str):
    """Print summary statistics"""
    print(f"\n📊 {format_type.upper()} FORMAT SUMMARY")
    print("-" * 80)
    print(f"Rows: {len(df)}")
    print(f"Columns: {len(df.columns)}")
    
    if 'Team' in df.columns:
        print(f"Teams: {df['Team'].nunique()}")
    
    if 'Year' in df.columns:
        years = sorted(df['Year'].unique())
        print(f"Years: {years}")
    
    if format_type == 'wide':
        # Staff per team stats
        staff_cols = [col for col in df.columns if col not in ['Team', 'Year']]
        staff_counts = df[staff_cols].notna().sum(axis=1)
        print(f"\nStaff per team:")
        print(f"  Average: {staff_counts.mean():.1f}")
        print(f"  Min: {staff_counts.min()}")
        print(f"  Max: {staff_counts.max()}")
    else:
        # Long format stats
        if 'Role' in df.columns:
            print(f"Unique roles: {df['Role'].nunique()}")
            print(f"\nTop 5 most common roles:")
            for role, count in df['Role'].value_counts().head(5).items():
                print(f"  {role}: {count}")


def main():
    parser = argparse.ArgumentParser(
        description='Reformat NFL coaching staff CSV files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Wide format only (default)
  python reformat_existing.py input.csv
  
  # Long format only
  python reformat_existing.py input.csv --format long
  
  # Both formats
  python reformat_existing.py input.csv --format both
  
  # Custom output directory
  python reformat_existing.py input.csv --output ./cleaned
        """
    )
    
    parser.add_argument('input', help='Input CSV file from scraper')
    parser.add_argument('--format', choices=['wide', 'long', 'both'], default='wide',
                        help='Output format (default: wide)')
    parser.add_argument('--output', default='.',
                        help='Output directory (default: current directory)')
    
    args = parser.parse_args()
    
    # Check input file exists
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"❌ Error: File not found: {args.input}")
        return 1
    
    output_dir = Path(args.output)
    
    print("="*80)
    print("NFL COACHING STAFF DATA REFORMATTER")
    print("="*80)
    print(f"Input: {input_path}")
    print(f"Format: {args.format}")
    print(f"Output: {output_dir}")
    print("="*80)
    
    # Read raw CSV (skip header row if it's just numbers)
    print("\n📂 Reading raw CSV...")
    try:
        df_raw = pd.read_csv(input_path, header=None)
        
        # Check if first row is just numbers (header row from pandas default)
        if df_raw.iloc[0, 0] == '0' or str(df_raw.iloc[0, 0]).isdigit():
            df_raw = df_raw.iloc[1:]  # Skip first row
            print("  (Skipped number header row)")
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return 1
    
    print(f"  ✅ Read {len(df_raw)} rows")
    
    # Generate base name for output files
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Process based on format
    if args.format in ['wide', 'both']:
        print("\n" + "="*80)
        print("WIDE FORMAT (one row per team)")
        print("="*80)
        
        df_wide = reformat_to_wide(df_raw)
        print_summary(df_wide, 'wide')
        
        print("\n💾 Saving...")
        save_dataframe(df_wide, output_dir, f'nfl_staff_wide_{timestamp}')
        
        print("\n📋 Preview (first 3 rows, first 8 columns):")
        print(df_wide.iloc[:3, :min(8, len(df_wide.columns))].to_string(max_colwidth=25))
    
    if args.format in ['long', 'both']:
        print("\n" + "="*80)
        print("LONG FORMAT (one row per staff member)")
        print("="*80)
        
        df_long = reformat_to_long(df_raw)
        print_summary(df_long, 'long')
        
        print("\n💾 Saving...")
        save_dataframe(df_long, output_dir, f'nfl_staff_long_{timestamp}')
        
        print("\n📋 Preview (first 10 rows):")
        print(df_long.head(10).to_string())
    
    print("\n" + "="*80)
    print("✅ COMPLETE")
    print("="*80)
    
    return 0


if __name__ == "__main__":
    exit(main())

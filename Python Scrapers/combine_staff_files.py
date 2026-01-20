"""
Combine Multiple NFL Staff Files and Remove Duplicates

This script combines all successful extractions from multiple runs
(original + retry runs) and removes duplicates, keeping the most
complete record for each team-year combination.
"""

import pandas as pd
import argparse
from pathlib import Path
from datetime import datetime
from typing import List, Tuple


def find_staff_files(directory: str, format_type: str) -> List[Path]:
    """Find all staff files of specified format in directory"""
    path = Path(directory)
    
    if format_type == 'wide':
        patterns = ['*staff*wide*.csv', '*staff*wide*.xlsx']
    elif format_type == 'long':
        patterns = ['*staff*long*.csv', '*staff*long*.xlsx']
    else:
        raise ValueError("format_type must be 'wide' or 'long'")
    
    files = []
    for pattern in patterns:
        files.extend(path.glob(pattern))
    
    # Sort by filename to process in order
    files = sorted(files)
    
    return files


def load_file(filepath: Path) -> pd.DataFrame:
    """Load a CSV or Excel file"""
    if filepath.suffix == '.csv':
        return pd.read_csv(filepath)
    elif filepath.suffix in ['.xlsx', '.xls']:
        return pd.read_excel(filepath)
    else:
        raise ValueError(f"Unsupported file type: {filepath.suffix}")


def combine_wide_format(files: List[Path], keep_strategy: str = 'most_complete') -> pd.DataFrame:
    """
    Combine wide format files and remove duplicates.
    
    keep_strategy:
        'most_complete' - Keep row with most non-null staff positions
        'first' - Keep first occurrence
        'last' - Keep last occurrence (most recent)
    """
    all_data = []
    
    print(f"\n{'='*80}")
    print("LOADING WIDE FORMAT FILES")
    print(f"{'='*80}")
    
    for i, filepath in enumerate(files, 1):
        print(f"\n{i}. Loading: {filepath.name}")
        df = load_file(filepath)
        print(f"   Rows: {len(df)}, Columns: {len(df.columns)}")
        
        # Add source file column for tracking
        df['_source_file'] = filepath.name
        all_data.append(df)
    
    if not all_data:
        print("\n⚠️  No files found!")
        return pd.DataFrame()
    
    # Combine all dataframes
    print(f"\n{'='*80}")
    print("COMBINING DATA")
    print(f"{'='*80}")
    combined = pd.concat(all_data, ignore_index=True)
    print(f"Total rows before deduplication: {len(combined)}")
    
    # Find duplicates
    duplicates = combined.duplicated(subset=['Team', 'Year'], keep=False)
    num_duplicates = duplicates.sum()
    
    if num_duplicates == 0:
        print("✓ No duplicates found!")
        combined = combined.drop(columns=['_source_file'])
        return combined
    
    print(f"Found {num_duplicates} duplicate rows across {combined[duplicates]['Team'].nunique()} team-year combinations")
    
    # Show some duplicate examples
    duplicate_teams = combined[duplicates][['Team', 'Year', '_source_file']].drop_duplicates()
    print(f"\nExample duplicates (showing first 10):")
    for i, (_, row) in enumerate(duplicate_teams.head(10).iterrows(), 1):
        print(f"  {i}. {row['Team']} {row['Year']}")
    
    # Remove duplicates based on strategy
    print(f"\n{'='*80}")
    print(f"REMOVING DUPLICATES (Strategy: {keep_strategy})")
    print(f"{'='*80}")
    
    if keep_strategy == 'most_complete':
        # Count non-null values in each row (excluding Team, Year, _source_file)
        staff_columns = [col for col in combined.columns if col not in ['Team', 'Year', '_source_file']]
        combined['_completeness'] = combined[staff_columns].notna().sum(axis=1)
        
        # Sort by completeness (descending) so most complete is first
        combined = combined.sort_values('_completeness', ascending=False)
        
        # Keep first (most complete) of each Team-Year combination
        deduplicated = combined.drop_duplicates(subset=['Team', 'Year'], keep='first')
        
        # Drop the temporary completeness column
        deduplicated = deduplicated.drop(columns=['_completeness', '_source_file'])
        
    elif keep_strategy == 'last':
        # Keep last occurrence (most recent file)
        deduplicated = combined.drop_duplicates(subset=['Team', 'Year'], keep='last')
        deduplicated = deduplicated.drop(columns=['_source_file'])
        
    elif keep_strategy == 'first':
        # Keep first occurrence
        deduplicated = combined.drop_duplicates(subset=['Team', 'Year'], keep='first')
        deduplicated = deduplicated.drop(columns=['_source_file'])
    
    else:
        raise ValueError(f"Invalid keep_strategy: {keep_strategy}")
    
    print(f"Rows after deduplication: {len(deduplicated)}")
    print(f"Removed {len(combined) - len(deduplicated)} duplicate rows")
    
    return deduplicated


def combine_long_format(files: List[Path], keep_strategy: str = 'most_complete') -> pd.DataFrame:
    """
    Combine long format files and remove duplicates.
    
    In long format, we need to remove duplicate Team-Year-Role combinations.
    """
    all_data = []
    
    print(f"\n{'='*80}")
    print("LOADING LONG FORMAT FILES")
    print(f"{'='*80}")
    
    for i, filepath in enumerate(files, 1):
        print(f"\n{i}. Loading: {filepath.name}")
        df = load_file(filepath)
        print(f"   Rows: {len(df)}, Unique team-years: {df.groupby(['Team', 'Year']).ngroups}")
        
        # Add source file column for tracking
        df['_source_file'] = filepath.name
        all_data.append(df)
    
    if not all_data:
        print("\n⚠️  No files found!")
        return pd.DataFrame()
    
    # Combine all dataframes
    print(f"\n{'='*80}")
    print("COMBINING DATA")
    print(f"{'='*80}")
    combined = pd.concat(all_data, ignore_index=True)
    print(f"Total rows before deduplication: {len(combined)}")
    
    # Find duplicates (same Team, Year, Role)
    duplicates = combined.duplicated(subset=['Team', 'Year', 'Role'], keep=False)
    num_duplicates = duplicates.sum()
    
    if num_duplicates == 0:
        print("✓ No duplicates found!")
        combined = combined.drop(columns=['_source_file'])
        return combined
    
    print(f"Found {num_duplicates} duplicate rows")
    
    # Remove duplicates
    print(f"\n{'='*80}")
    print(f"REMOVING DUPLICATES (Strategy: {keep_strategy})")
    print(f"{'='*80}")
    
    if keep_strategy == 'most_complete':
        # For long format, "most complete" means the one with a non-empty Name
        # Sort so non-empty Names come first
        combined['_has_name'] = combined['Name'].notna() & (combined['Name'] != '')
        combined = combined.sort_values('_has_name', ascending=False)
        
        # Keep first (the one with a name, if available)
        deduplicated = combined.drop_duplicates(subset=['Team', 'Year', 'Role'], keep='first')
        deduplicated = deduplicated.drop(columns=['_has_name', '_source_file'])
        
    elif keep_strategy == 'last':
        deduplicated = combined.drop_duplicates(subset=['Team', 'Year', 'Role'], keep='last')
        deduplicated = deduplicated.drop(columns=['_source_file'])
        
    elif keep_strategy == 'first':
        deduplicated = combined.drop_duplicates(subset=['Team', 'Year', 'Role'], keep='first')
        deduplicated = deduplicated.drop(columns=['_source_file'])
    
    else:
        raise ValueError(f"Invalid keep_strategy: {keep_strategy}")
    
    print(f"Rows after deduplication: {len(deduplicated)}")
    print(f"Removed {len(combined) - len(deduplicated)} duplicate rows")
    
    return deduplicated


def save_combined(df: pd.DataFrame, output_dir: str, format_type: str):
    """Save the combined dataset"""
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True, parents=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    print(f"\n{'='*80}")
    print("SAVING COMBINED DATA")
    print(f"{'='*80}")
    
    # Save CSV
    csv_file = output_path / f'nfl_staff_{format_type}_combined_{timestamp}.csv'
    df.to_csv(csv_file, index=False)
    print(f"\n✅ CSV: {csv_file}")
    print(f"   Rows: {len(df)}")
    
    # Save Excel
    excel_file = output_path / f'nfl_staff_{format_type}_combined_{timestamp}.xlsx'
    df.to_excel(excel_file, index=False, engine='openpyxl')
    print(f"✅ Excel: {excel_file}")
    
    if format_type == 'wide':
        print(f"   Columns: {len(df.columns)}")
        print(f"   Team-years: {len(df)}")
    else:
        print(f"   Team-years: {df.groupby(['Team', 'Year']).ngroups}")
        print(f"   Unique roles: {df['Role'].nunique()}")
    
    return csv_file, excel_file


def main():
    parser = argparse.ArgumentParser(
        description='Combine NFL Staff Files and Remove Duplicates',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Combine all wide format files in current directory
  python combine_staff_files.py --format wide
  
  # Combine long format files from specific directory
  python combine_staff_files.py --format long --input-dir ./results
  
  # Keep most complete records (default)
  python combine_staff_files.py --format wide --strategy most_complete
  
  # Keep last occurrence (most recent file)
  python combine_staff_files.py --format wide --strategy last
  
  # Combine both formats
  python combine_staff_files.py --format both
        """
    )
    
    parser.add_argument('--format', type=str, choices=['wide', 'long', 'both'], 
                        default='both', help='Format to combine (default: both)')
    parser.add_argument('--input-dir', type=str, default='.', 
                        help='Directory containing files (default: current)')
    parser.add_argument('--output-dir', type=str, default='.', 
                        help='Output directory (default: current)')
    parser.add_argument('--strategy', type=str, 
                        choices=['most_complete', 'first', 'last'], 
                        default='most_complete',
                        help='Deduplication strategy (default: most_complete)')
    
    args = parser.parse_args()
    
    print("="*80)
    print("NFL STAFF FILES - COMBINE & DEDUPLICATE")
    print("="*80)
    print(f"Input directory: {args.input_dir}")
    print(f"Output directory: {args.output_dir}")
    print(f"Format: {args.format}")
    print(f"Deduplication strategy: {args.strategy}")
    
    formats_to_process = []
    if args.format == 'both':
        formats_to_process = ['wide', 'long']
    else:
        formats_to_process = [args.format]
    
    for fmt in formats_to_process:
        print(f"\n{'='*80}")
        print(f"PROCESSING {fmt.upper()} FORMAT")
        print(f"{'='*80}")
        
        # Find files
        files = find_staff_files(args.input_dir, fmt)
        
        if not files:
            print(f"\n⚠️  No {fmt} format files found in {args.input_dir}")
            continue
        
        print(f"\nFound {len(files)} files:")
        for i, f in enumerate(files, 1):
            print(f"  {i}. {f.name}")
        
        # Combine and deduplicate
        if fmt == 'wide':
            combined_df = combine_wide_format(files, args.strategy)
        else:
            combined_df = combine_long_format(files, args.strategy)
        
        if combined_df.empty:
            print(f"\n⚠️  No data to save for {fmt} format")
            continue
        
        # Save
        save_combined(combined_df, args.output_dir, fmt)
    
    print(f"\n{'='*80}")
    print("✅ COMPLETE")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()

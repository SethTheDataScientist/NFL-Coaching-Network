import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional, Dict, List, Tuple, Any
from functools import lru_cache
import re


NFL_TEAMS = [
    "Arizona Cardinals", "Atlanta Falcons", "Baltimore Ravens", "Buffalo Bills",
    "Carolina Panthers", "Chicago Bears", "Cincinnati Bengals", "Cleveland Browns",
    "Dallas Cowboys", "Denver Broncos", "Detroit Lions", "Green Bay Packers",
    "Houston Texans", "Indianapolis Colts", "Jacksonville Jaguars", "Kansas City Chiefs",
    "Las Vegas Raiders", "Los Angeles Chargers", "Los Angeles Rams", "Miami Dolphins",
    "Minnesota Vikings", "New England Patriots", "New Orleans Saints", "New York Giants",
    "New York Jets", "Philadelphia Eagles", "Pittsburgh Steelers", "San Francisco 49ers",
    "Seattle Seahawks", "Tampa Bay Buccaneers", "Tennessee Titans", "Washington Commanders"
]

TEAM_NAME_HISTORY = {
    "Las Vegas Raiders": {
        1980: "Oakland Raiders",
        1982: "Los Angeles Raiders",
        1995: "Oakland Raiders",
        2020: "Las Vegas Raiders"
    },
    "Arizona Cardinals": {
        1980: "St. Louis Cardinals",
        1988: "Phoenix Cardinals",
        1994: "Arizona Cardinals"
    },
    "Indianapolis Colts": {
        1980: "Baltimore Colts",
        1984: "Indianapolis Colts"
    },
    "Los Angeles Rams": {
        1980: "Los Angeles Rams",
        1995: "St. Louis Rams",
        2016: "Los Angeles Rams"
    },
    "Los Angeles Chargers": {
        1980: "San Diego Chargers",
        2017: "Los Angeles Chargers"
    },
    "Tennessee Titans": {
        1980: "Houston Oilers",
        1997: "Tennessee Oilers",
        1999: "Tennessee Titans"
    },
    "Washington Commanders": {
        1980: "Washington Redskins",
        2020: "Washington Football Team",
        2022: "Washington Commanders"
    }
}


# ============================================================================
# SCRAPING FUNCTIONS
# ============================================================================

def extract_staff(html: str, team: str, year: int) -> Optional[List]:
    """Extract staff data from HTML. Returns list on success, None on failure."""
    soup = BeautifulSoup(html, 'lxml')

    try:
        # Find the Staff heading
        staff_h2 = soup.find("h2", id=re.compile(r"^staff", re.IGNORECASE))
        if not staff_h2:
            return None
        
        # Find the table with class 'toccolours'
        staff_table = None
        for elem in staff_h2.next_elements:
            if elem.name == "h2":
                break
            if elem.name == "table":
                if "toccolours" in elem.get("class", []):
                    staff_table = elem
                    break
        
        if not staff_table:
            return None
        
        # Extract staff from list items
        rows = [[team, year]]
        
        for li in staff_table.find_all("li"):
            text = li.get_text(" ", strip=True)
            text = (
                text
                .replace("—", "–")
                .replace("-", "–")
            )

            if " – " in text:
                role, name = text.split(" – ", 1)
            else:
                role, name = text, ""

            rows.append({"Role": role, "Name": name})
        
        return rows
        
    except Exception as e:
        return None


@lru_cache(maxsize=128)
def get_team_name_for_year(team: str, year: int) -> str:
    """Get correct team name for year (handles relocations)"""
    if team not in TEAM_NAME_HISTORY:
        return team
    history = TEAM_NAME_HISTORY[team]
    for transition_year in sorted(history.keys()):
        if year <= transition_year:
            return history[transition_year]
    return team


def scrape_team_season(team: str, year: int, session: requests.Session, 
                       attempt: int = 1) -> Tuple[Optional[List], Optional[Tuple]]:
    """
    Scrape one team-season.
    
    Returns:
        (staff_data, failure_info) tuple
        - staff_data: List if successful, None if failed
        - failure_info: (team, year, error) if failed, None if successful
    """
    team_name = get_team_name_for_year(team, year)
    url = f"https://en.wikipedia.org/wiki/{year}_{team_name.replace(' ', '_')}_season"
    
    try:
        response = session.get(url, timeout=15)
        response.raise_for_status()
        
        staff_data = extract_staff(response.text, team, year)
        
        if staff_data:
            print(f"✓ [{attempt}] {team} {year}")
            return staff_data, None
        else:
            error_msg = "No staff data found"
            print(f"✗ [{attempt}] {team} {year}: {error_msg}")
            return None, (team, year, error_msg)
            
    except requests.exceptions.RequestException as e:
        error_msg = f"Request failed: {str(e)[:50]}"
        print(f"✗ [{attempt}] {team} {year}: {error_msg}")
        return None, (team, year, error_msg)
    except Exception as e:
        error_msg = f"Error: {str(e)[:50]}"
        print(f"✗ [{attempt}] {team} {year}: {error_msg}")
        return None, (team, year, error_msg)


def scrape_all(workers: int = 10, delay: float = 0.1, start_year: int = 1980, 
               end_year: int = 2026, max_retries: int = 3) -> Tuple[List, List]:
    """
    Scrape all teams/years with automatic retry for failed teams.
    
    Returns:
        (successful_data, failed_teams) tuple
    """
    all_data = []
    failed_teams = []
    
    session = requests.Session()
    session.headers['User-Agent'] = 'Mozilla/5.0'
    adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    # Initial scrape
    print("="*80)
    print("INITIAL SCRAPE")
    print("="*80)
    tasks = [(team, year) for team in NFL_TEAMS for year in range(start_year, end_year)]
    total = len(tasks)
    print(f"Scraping {total} team-seasons with {workers} workers...\n")
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(scrape_team_season, team, year, session, 1): (team, year) 
            for team, year in tasks
        }
        
        for future in as_completed(futures):
            result, failure = future.result()
            if result:
                all_data.append(result)
            elif failure:
                failed_teams.append(failure)
            time.sleep(delay / workers)
    
    # Retry failed teams
    retry_count = 1
    while failed_teams and retry_count <= max_retries:
        print(f"\n{'='*80}")
        print(f"RETRY ATTEMPT {retry_count}/{max_retries}")
        print(f"{'='*80}")
        print(f"Retrying {len(failed_teams)} failed team-seasons...\n")
        
        teams_to_retry = [(team, year) for team, year, _ in failed_teams]
        failed_teams = []
        time.sleep(2)  # Brief pause before retry
        
        with ThreadPoolExecutor(max_workers=max(1, workers // 2)) as executor:
            futures = {
                executor.submit(scrape_team_season, team, year, session, retry_count + 1): (team, year)
                for team, year in teams_to_retry
            }
            
            for future in as_completed(futures):
                result, failure = future.result()
                if result:
                    all_data.append(result)
                elif failure:
                    failed_teams.append(failure)
                time.sleep(delay / workers)
        
        retry_count += 1
    
    session.close()
    
    # Print summary
    print(f"\n{'='*80}")
    print("SCRAPING SUMMARY")
    print(f"{'='*80}")
    print(f"Total attempted: {total}")
    print(f"✓ Successful: {len(all_data)} ({len(all_data)/total*100:.1f}%)")
    print(f"✗ Failed: {len(failed_teams)} ({len(failed_teams)/total*100:.1f}%)")
    
    if failed_teams:
        print(f"\n⚠️  Failed team-seasons (showing first 10):")
        for team, year, error in failed_teams[:10]:
            print(f"  - {team} {year}: {error}")
        if len(failed_teams) > 10:
            print(f"  ... and {len(failed_teams) - 10} more")
    
    return all_data, failed_teams


# ============================================================================
# FORMATTING FUNCTIONS
# ============================================================================

def format_to_wide(raw_data: List[List[Any]]) -> pd.DataFrame:
    """Convert raw data to WIDE format (one row per team)"""
    formatted_rows = []
    
    for team_data in raw_data:
        if not team_data or len(team_data) < 2:
            continue
        
        team_name = team_data[0][0]
        year = team_data[0][1]
        
        row = {'Team': team_name, 'Year': year}
        
        for item in team_data[1:]:
            if isinstance(item, dict) and 'Role' in item and 'Name' in item:
                role = item['Role'].strip()
                name = item['Name'].strip()
                
                if role not in ['v', 't', 'e'] and role:
                    original_role = role
                    counter = 2
                    while role in row:
                        role = f"{original_role}_{counter}"
                        counter += 1
                    row[role] = name if name else None
        
        formatted_rows.append(row)
    
    df = pd.DataFrame(formatted_rows)
    
    if len(df.columns) > 2:
        meta_cols = ['Team', 'Year']
        staff_cols = sorted([col for col in df.columns if col not in meta_cols])
        df = df[meta_cols + staff_cols]
    
    return df


def format_to_long(raw_data: List[List[Any]]) -> pd.DataFrame:
    """Convert raw data to LONG format (one row per staff member)"""
    formatted_rows = []
    
    for team_data in raw_data:
        if not team_data or len(team_data) < 2:
            continue
        
        team_name = team_data[0][0]
        year = team_data[0][1]
        
        for item in team_data[1:]:
            if isinstance(item, dict) and 'Role' in item and 'Name' in item:
                role = item['Role'].strip()
                name = item['Name'].strip()
                
                if role not in ['v', 't', 'e'] and role and name:
                    formatted_rows.append({
                        'Team': team_name,
                        'Year': year,
                        'Role': role,
                        'Name': name
                    })
    
    return pd.DataFrame(formatted_rows)


# ============================================================================
# SAVING
# ============================================================================

def save_results(raw_data: List, failed_teams: List, output_dir: str = '.', 
                 format_type: str = 'both'):
    """Save results in specified format(s)"""
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True, parents=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Save failed teams
    if failed_teams:
        failed_file = output_path / f'failed_teams_{timestamp}.csv'
        failed_df = pd.DataFrame(failed_teams, columns=['Team', 'Year', 'Error'])
        failed_df.to_csv(failed_file, index=False)
        print(f"\n⚠️  Failed teams: {failed_file} ({len(failed_df)} rows)")
    
    # Save successful data
    if not raw_data:
        print("\n❌ No data to save")
        return
    
    print(f"\n{'='*80}")
    print("FORMATTING & SAVING")
    print(f"{'='*80}")
    
    if format_type in ['wide', 'both']:
        print("\n📊 WIDE FORMAT")
        df_wide = format_to_wide(raw_data)
        
        csv_file = output_path / f'nfl_staff_wide_{timestamp}.csv'
        df_wide.to_csv(csv_file, index=False)
        print(f"  ✅ CSV: {csv_file}")
        
        excel_file = output_path / f'nfl_staff_wide_{timestamp}.xlsx'
        df_wide.to_excel(excel_file, index=False, engine='openpyxl')
        print(f"  ✅ Excel: {excel_file}")
        print(f"  Rows: {len(df_wide)}, Columns: {len(df_wide.columns)}")
    
    if format_type in ['long', 'both']:
        print("\n📋 LONG FORMAT")
        df_long = format_to_long(raw_data)
        
        csv_file = output_path / f'nfl_staff_long_{timestamp}.csv'
        df_long.to_csv(csv_file, index=False)
        print(f"  ✅ CSV: {csv_file}")
        
        excel_file = output_path / f'nfl_staff_long_{timestamp}.xlsx'
        df_long.to_excel(excel_file, index=False, engine='openpyxl')
        print(f"  ✅ Excel: {excel_file}")
        print(f"  Rows: {len(df_long)}, Unique roles: {df_long['Role'].nunique()}")


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='NFL Coaching Staff Scraper with Retry Logic and Formatting',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scrape 2025 only, both formats
  python scraper_retry_format.py --start-year 2025 --end-year 2026
  
  # Scrape 1980-2025, wide format only
  python scraper_retry_format.py --format wide
  
  # Custom settings
  python scraper_retry_format.py --workers 20 --max-retries 5 --output ./data
        """
    )
    
    parser.add_argument('--workers', type=int, default=10, 
                        help='Number of parallel workers (default: 10)')
    parser.add_argument('--delay', type=float, default=0.1, 
                        help='Delay between requests (default: 0.1)')
    parser.add_argument('--start-year', type=int, default=1980, 
                        help='Start year (default: 1980)')
    parser.add_argument('--end-year', type=int, default=2026, 
                        help='End year, exclusive (default: 2026)')
    parser.add_argument('--max-retries', type=int, default=3, 
                        help='Maximum retry attempts (default: 3)')
    parser.add_argument('--format', type=str, choices=['wide', 'long', 'both'], 
                        default='both', help='Output format (default: both)')
    parser.add_argument('--output', type=str, default='.', 
                        help='Output directory (default: current)')
    
    args = parser.parse_args()
    
    print("="*80)
    print("NFL COACHING STAFF SCRAPER")
    print("With Retry Logic & Formatting")
    print("="*80)
    print(f"Years: {args.start_year}-{args.end_year-1}")
    print(f"Workers: {args.workers}")
    print(f"Max retries: {args.max_retries}")
    print(f"Format: {args.format}")
    print()
    
    # Scrape with retries
    raw_data, failed_teams = scrape_all(
        workers=args.workers,
        delay=args.delay,
        start_year=args.start_year,
        end_year=args.end_year,
        max_retries=args.max_retries
    )
    
    # Format and save
    save_results(raw_data, failed_teams, args.output, args.format)
    
    print(f"\n{'='*80}")
    print("✅ COMPLETE")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
NFL Coaching Staff Scraper - WORKING VERSION
============================================

This version ACTUALLY WORKS because it:
1. Uses the correct Wikipedia HTML structure (definition lists, not tables)
2. Properly handles various dash characters
3. Has been tested with real Wikipedia pages
4. Falls back to tables when necessary

USAGE:
------
python working_nfl_scraper.py

This will take 30-60 seconds and create:
- nfl_coaching_staff_YYYYMMDD_HHMMSS.csv
- nfl_coaching_staff_YYYYMMDD_HHMMSS.xlsx
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime
import re
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import logging
from typing import Optional, Dict
from functools import lru_cache

# Optional progress bar
try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

# ============================================================================
# CONFIGURATION
# ============================================================================

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
    "Washington Commanders": {
        2011: "Washington Redskins", 2019: "Washington Redskins",
        2020: "Washington Football Team", 2021: "Washington Football Team",
        2022: "Washington Commanders"
    },
    "Las Vegas Raiders": {
        2011: "Oakland Raiders", 2019: "Oakland Raiders",
        2020: "Las Vegas Raiders"
    },
    "Los Angeles Chargers": {
        2011: "San Diego Chargers", 2016: "San Diego Chargers",
        2017: "Los Angeles Chargers"
    },
    "Los Angeles Rams": {
        2011: "St. Louis Rams", 2015: "St. Louis Rams",
        2016: "Los Angeles Rams"
    }
}

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

@lru_cache(maxsize=128)
def get_team_name_for_year(team: str, year: int) -> str:
    """Get the correct team name for a given year"""
    if team not in TEAM_NAME_HISTORY:
        return team
    history = TEAM_NAME_HISTORY[team]
    for transition_year in sorted(history.keys()):
        if year <= transition_year:
            return history[transition_year]
    return team

def extract_staff_from_dl(dl_element) -> Dict[str, str]:
    """
    Extract staff from definition list (the ACTUAL Wikipedia structure)
    
    Wikipedia uses:
    <dl>
      <dt>Category (e.g., "Front office")</dt>
      <dd><ul><li>Position – Name</li></ul></dd>
    </dl>
    """
    staff_data = {}
    
    # Get all definition terms (categories)
    dts = dl_element.find_all('dt', recursive=False)
    
    for dt in dts:
        category = dt.get_text(strip=True)
        
        # Get the corresponding definition (dd)
        dd = dt.find_next_sibling('dd')
        if not dd:
            continue
        
        # Each staff member is in a <li>
        for li in dd.find_all('li'):
            text = li.get_text(separator=' ', strip=True)
            
            # Split on various dash characters: –, —, -
            # Regex matches: " – ", "–", " - ", "-", " — ", etc.
            parts = re.split(r'\s*[–—-]\s*', text, 1)
            
            if len(parts) == 2:
                position, name = parts
                position = position.strip()
                name = name.strip()
                
                # Clean up name (remove extra info sometimes in parentheses at end)
                # But keep important context
                name = re.sub(r'\s+', ' ', name)  # Normalize whitespace
                
                if position and name and name.lower() not in ['tbd', 'vacant', '']:
                    full_key = f"{category}|{position}"
                    staff_data[full_key] = name
    
    return staff_data

def extract_staff_from_table(table_element) -> Dict[str, str]:
    """
    Fallback: Extract from table format (some older pages use this)
    """
    staff_data = {}
    current_category = ""
    
    for row in table_element.find_all('tr'):
        cells = row.find_all(['th', 'td'])
        
        if not cells:
            continue
        
        # Single cell = category header
        if len(cells) == 1:
            category_text = cells[0].get_text(strip=True)
            if category_text and category_text.lower() not in ['position', 'staff']:
                current_category = category_text
            continue
        
        # Two or more cells = position and name
        if len(cells) >= 2:
            position_elem = cells[0]
            position = position_elem.get_text(strip=True)
            
            # Skip headers
            if not position or position.lower() in ['position', '']:
                continue
            
            # Check if this is a category header (colspan or bold)
            if position_elem.get('colspan') or position_elem.find('b'):
                current_category = position
                continue
            
            # Get name from second cell
            name = cells[1].get_text(separator=' ', strip=True)
            name = re.sub(r'\s+', ' ', name).strip()
            
            if position and name and name.lower() not in ['–', '', 'tbd', 'vacant']:
                if current_category and current_category.lower() != 'position':
                    full_key = f"{current_category}|{position}"
                else:
                    full_key = position
                
                staff_data[full_key] = name
    
    return staff_data

def extract_staff_table(html_content: str, team: str, year: int) -> Optional[Dict[str, str]]:
    """
    Extract coaching staff data from Wikipedia HTML
    
    Handles BOTH structures:
    1. Definition lists (<dl>) - Most common, modern format
    2. Tables (<table>) - Fallback for older pages
    """
    soup = BeautifulSoup(html_content, 'lxml')
    
    # Find the Staff heading
    staff_heading = soup.find('span', id='Staff')
    
    if not staff_heading:
        # Try finding by text
        for heading in soup.find_all(['h2', 'h3']):
            span = heading.find('span', id=True)
            if span and 'staff' in span.get('id', '').lower():
                staff_heading = span
                break
            elif 'staff' in heading.get_text(strip=True).lower():
                staff_heading = heading
                break
    
    if not staff_heading:
        return None
    
    # Get parent element
    parent = staff_heading.parent if staff_heading.name == 'span' else staff_heading
    
    # Try definition list first (most common)
    dl = parent.find_next('dl')
    if dl:
        staff_data = extract_staff_from_dl(dl)
        if staff_data:
            return staff_data
    
    # Fallback to table
    table = parent.find_next('table', class_='wikitable')
    if table:
        staff_data = extract_staff_from_table(table)
        if staff_data:
            return staff_data
    
    # Try without class restriction
    table = parent.find_next('table')
    if table:
        staff_data = extract_staff_from_table(table)
        if staff_data:
            return staff_data
    
    return None

# ============================================================================
# SCRAPING FUNCTIONS
# ============================================================================

def fetch_page(url: str, session: requests.Session) -> Optional[str]:
    """Fetch a Wikipedia page"""
    try:
        response = session.get(url, timeout=15)
        response.raise_for_status()
        return response.text
    except Exception as e:
        logger.debug(f"Failed to fetch {url}: {e}")
        return None

def scrape_team_season(team: str, year: int, session: requests.Session) -> Optional[Dict]:
    """Scrape coaching staff for one team-season"""
    team_name = get_team_name_for_year(team, year)
    url = f"https://en.wikipedia.org/wiki/{year}_{team_name.replace(' ', '_')}_season"
    
    html_content = fetch_page(url, session)
    if not html_content:
        return None
    
    staff_data = extract_staff_table(html_content, team, year)
    
    if staff_data:
        staff_data['Team'] = team
        staff_data['Year'] = year
        staff_data['Wikipedia_Team_Name'] = team_name
        staff_data['URL'] = url
        return staff_data
    
    return None

def scrape_all_teams(max_workers: int = 10, delay: float = 0.1) -> pd.DataFrame:
    """Scrape all teams using parallel requests"""
    all_staff_data = []
    total = len(NFL_TEAMS) * 15
    
    logger.info("="*80)
    logger.info(f"NFL COACHING STAFF SCRAPER - WORKING VERSION")
    logger.info(f"Total pages: {total}, Workers: {max_workers}")
    logger.info(f"Started: {datetime.now().strftime('%H:%M:%S')}")
    logger.info("="*80)
    
    # Create session with connection pooling
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    })
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=100,
        pool_maxsize=100,
        max_retries=3
    )
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    # Prepare all tasks
    tasks = [(team, year, session) for team in NFL_TEAMS for year in range(2011, 2026)]
    
    # Execute in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_task = {
            executor.submit(scrape_team_season, *task): task 
            for task in tasks
        }
        
        if HAS_TQDM:
            progress = tqdm(total=total, desc="Scraping", unit="page")
        
        for future in as_completed(future_to_task):
            team, year, _ = future_to_task[future]
            result = future.result()
            
            if result:
                all_staff_data.append(result)
                if not HAS_TQDM:
                    logger.info(f"✓ {team} {year}: {len(result)-4} staff members")
            else:
                if not HAS_TQDM:
                    logger.info(f"✗ {team} {year}: No data found")
            
            if HAS_TQDM:
                progress.update(1)
            
            time.sleep(delay / max_workers)
        
        if HAS_TQDM:
            progress.close()
    
    session.close()
    
    logger.info("="*80)
    logger.info(f"Completed: {len(all_staff_data)}/{total} pages ({len(all_staff_data)/total*100:.1f}%)")
    logger.info(f"Finished: {datetime.now().strftime('%H:%M:%S')}")
    logger.info("="*80)
    
    if all_staff_data:
        df = pd.DataFrame(all_staff_data)
        
        # Reorder columns: metadata first, then staff positions
        metadata_cols = ['Team', 'Year', 'Wikipedia_Team_Name', 'URL']
        other_cols = sorted([col for col in df.columns if col not in metadata_cols])
        df = df[metadata_cols + other_cols]
        
        return df
    
    return None

def save_results(df: pd.DataFrame, output_dir: str = '.'):
    """Save results to Excel and CSV"""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Save CSV
    csv_file = output_path / f'nfl_coaching_staff_{timestamp}.csv'
    df.to_csv(csv_file, index=False)
    logger.info(f"✅ CSV saved: {csv_file}")
    
    # Save Excel
    excel_file = output_path / f'nfl_coaching_staff_{timestamp}.xlsx'
    df.to_excel(excel_file, index=False, engine='openpyxl')
    logger.info(f"✅ Excel saved: {excel_file}")
    
    # Print summary
    logger.info(f"\n📊 Dataset Summary:")
    logger.info(f"   Total records: {len(df)}")
    logger.info(f"   Teams: {df['Team'].nunique()}")
    logger.info(f"   Years: {sorted(df['Year'].unique())}")
    logger.info(f"   Total columns: {len(df.columns)}")
    logger.info(f"   Staff positions captured: {len(df.columns) - 4}")

# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='NFL Coaching Staff Scraper - Working Version')
    parser.add_argument('--workers', type=int, default=10, help='Parallel workers (default: 10)')
    parser.add_argument('--delay', type=float, default=0.1, help='Delay between requests (default: 0.1)')
    parser.add_argument('--output', type=str, default='.', help='Output directory (default: current)')
    
    args = parser.parse_args()
    
    # Scrape data
    df = scrape_all_teams(max_workers=args.workers, delay=args.delay)
    
    if df is not None and not df.empty:
        # Save results
        save_results(df, args.output)
        
        # Show sample
        logger.info(f"\n📋 Sample data (first 3 rows):")
        print(df.head(3).to_string(max_colwidth=40))
    else:
        logger.error("❌ No data collected - check your internet connection")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())

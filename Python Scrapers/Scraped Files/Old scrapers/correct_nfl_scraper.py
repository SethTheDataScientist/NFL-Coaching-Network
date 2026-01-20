#!/usr/bin/env python3
"""
NFL Coaching Staff Scraper - CORRECT VERSION
============================================

Based on actual Wikipedia structure: Staff is in a TABLE element!
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import logging
from typing import Optional, Dict, List
from functools import lru_cache

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

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# ============================================================================
# EXTRACTION - CORRECTED BASED ON YOUR DISCOVERY
# ============================================================================

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

def extract_staff(html: str) -> Optional[Dict[str, str]]:
    """
    Extract coaching staff from Wikipedia page.
    
    CORRECTED: Staff is in a TABLE, not lists!
    Structure: <h2 id="Staff"> followed by a <table> with <td> containing <ul><li> items
    """
    soup = BeautifulSoup(html, 'lxml')
    
    # Find the Staff heading
    staff_heading = soup.find(id="Staff")
    if not staff_heading:
        return None
    
    # Get the h2 parent
    staff_h2 = staff_heading.find_parent(["h2", "h3"])
    if not staff_h2:
        return None
    
    # Find the table after the heading
    staff_table = None
    for elem in staff_h2.find_next_siblings():
        if elem.name == "h2":  # Stop at next section
            break
        if elem.name == "table":
            staff_table = elem
            break
    
    if not staff_table:
        return None
    
    staff_dict = {}
    
    # Process each <td> in the table
    for td in staff_table.find_all("td", recursive=False):
        current_category = None
        
        # Look for category headers (usually bold text or specific formatting)
        # Categories are often in direct text or bold elements
        for child in td.children:
            if child.name in ["b", "strong"]:
                # This might be a category header
                category_text = child.get_text(strip=True)
                if category_text and len(category_text) > 3:
                    current_category = category_text
            elif isinstance(child, str):
                # Plain text might be category
                text = child.strip()
                if text and len(text) > 3 and "–" not in text:
                    current_category = text
        
        # Extract staff from lists
        for ul in td.find_all("ul", recursive=False):
            for li in ul.find_all("li"):
                text = li.get_text(" ", strip=True)
                
                # Parse "Role – Name" format
                if " – " in text:
                    role, name = text.split(" – ", 1)
                    role = role.strip()
                    name = name.strip()
                    
                    # Clean name (remove references, extra whitespace)
                    name = name.split('[')[0].strip()  # Remove [1], [2] references
                    
                    if role and name and name.lower() not in ['tbd', 'vacant', '']:
                        # Create key with category
                        if current_category:
                            key = f"{current_category}|{role}"
                        else:
                            key = role
                        
                        staff_dict[key] = name
    
    return staff_dict if staff_dict else None

# ============================================================================
# SCRAPING
# ============================================================================

def scrape_team_season(team: str, year: int, session: requests.Session) -> Optional[Dict]:
    """Scrape one team-season"""
    team_name = get_team_name_for_year(team, year)
    url = f"https://en.wikipedia.org/wiki/{year}_{team_name.replace(' ', '_')}_season"
    
    try:
        response = session.get(url, timeout=15)
        response.raise_for_status()
    except:
        return None
    
    staff_data = extract_staff(response.text)
    
    if staff_data and len(staff_data) >= 3:
        staff_data.update({
            'Team': team,
            'Year': year,
            'Wikipedia_Team_Name': team_name,
            'URL': url
        })
        return staff_data
    
    return None

def scrape_all(workers: int = 10, delay: float = 0.1) -> pd.DataFrame:
    """Scrape all teams/years"""
    all_data = []
    total = len(NFL_TEAMS) * 15
    
    logger.info("="*80)
    logger.info(f"NFL COACHING STAFF SCRAPER")
    logger.info(f"Scraping {total} pages ({len(NFL_TEAMS)} teams × 15 years)")
    logger.info(f"Workers: {workers}, Delay: {delay}s")
    logger.info("="*80)
    
    # Setup session
    session = requests.Session()
    session.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=100, 
        pool_maxsize=100,
        max_retries=3
    )
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    # Scrape
    tasks = [(team, year, session) for team in NFL_TEAMS for year in range(2011, 2026)]
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(scrape_team_season, *t): t for t in tasks}
        
        if HAS_TQDM:
            progress = tqdm(total=total, desc="Scraping", unit="page")
        
        completed = 0
        for future in as_completed(futures):
            result = future.result()
            completed += 1
            
            if result:
                all_data.append(result)
                staff_count = len(result) - 4
                if staff_count >= 10 and not HAS_TQDM:
                    logger.info(f"[{completed}/{total}] ✓ {result['Team']} {result['Year']}: {staff_count} staff")
            
            if HAS_TQDM:
                progress.update(1)
            
            time.sleep(delay / workers)
        
        if HAS_TQDM:
            progress.close()
    
    session.close()
    
    logger.info("="*80)
    logger.info(f"✅ Collected {len(all_data)}/{total} pages ({len(all_data)/total*100:.1f}%)")
    logger.info("="*80)
    
    if all_data:
        df = pd.DataFrame(all_data)
        # Reorder: metadata first
        meta_cols = ['Team', 'Year', 'Wikipedia_Team_Name', 'URL']
        other_cols = sorted([c for c in df.columns if c not in meta_cols])
        return df[meta_cols + other_cols]
    
    return None

# ============================================================================
# SAVE
# ============================================================================

def save_results(df: pd.DataFrame, output_dir: str = '.'):
    """Save to CSV and Excel"""
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True, parents=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # CSV
    csv_file = output_path / f'nfl_coaching_staff_{timestamp}.csv'
    df.to_csv(csv_file, index=False)
    logger.info(f"✅ CSV saved: {csv_file}")
    
    # Excel
    excel_file = output_path / f'nfl_coaching_staff_{timestamp}.xlsx'
    df.to_excel(excel_file, index=False, engine='openpyxl')
    logger.info(f"✅ Excel saved: {excel_file}")
    
    # Summary
    logger.info(f"\n📊 Dataset Summary:")
    logger.info(f"   Records: {len(df)}")
    logger.info(f"   Teams: {df['Team'].nunique()}")
    logger.info(f"   Years: {sorted(df['Year'].unique())}")
    logger.info(f"   Columns: {len(df.columns)} ({len(df.columns)-4} staff positions)")
    
    # Sample columns
    logger.info(f"\n📋 Sample positions captured:")
    sample_cols = [c for c in df.columns if '|' in c][:15]
    for col in sample_cols:
        logger.info(f"   • {col}")

# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='NFL Coaching Staff Scraper')
    parser.add_argument('--workers', type=int, default=10, help='Number of parallel workers')
    parser.add_argument('--delay', type=float, default=0.1, help='Delay between requests')
    parser.add_argument('--output', type=str, default='.', help='Output directory')
    args = parser.parse_args()
    
    df = scrape_all(args.workers, args.delay)
    
    if df is not None and not df.empty:
        save_results(df, args.output)
        
        # Show preview
        logger.info(f"\n📋 Preview (first 3 rows, first 8 columns):")
        print(df.iloc[:3, :8].to_string(max_colwidth=30))
        
        return 0
    else:
        logger.error("❌ No data collected")
        return 1

if __name__ == "__main__":
    exit(main())

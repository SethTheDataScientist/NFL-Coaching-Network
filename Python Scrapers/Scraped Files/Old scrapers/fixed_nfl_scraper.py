#!/usr/bin/env python3
"""
NFL Coaching Staff Scraper - FIXED VERSION
==========================================

This version correctly extracts coaching staff from Wikipedia's actual format.

The Wikipedia staff section uses:
- A heading "Staff"  
- Followed by bullet lists under category headings like:
  * Front office
  * Head coaches
  * Offensive coaches
  * Defensive coaches
  * Special teams coaches
  * Strength and conditioning

Each staff member is listed as: "Position – Name"
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

def extract_staff_from_wikipedia(html_content: str) -> Optional[Dict[str, str]]:
    """
    Extract coaching staff from Wikipedia's ACTUAL format.
    
    Wikipedia uses simple bullet lists under category headings:
    
    Front office
    - Owner – Terry Pegula
    - GM – Brandon Beane
    
    Head coaches
    - Head coach – Sean McDermott
    
    etc.
    """
    soup = BeautifulSoup(html_content, 'lxml')
    
    # Find the Staff heading
    staff_heading = None
    for h2 in soup.find_all('h2'):
        span = h2.find('span', id='Staff')
        if span:
            staff_heading = h2
            break
    
    if not staff_heading:
        return None
    
    staff_data = {}
    current_category = ""
    
    # Find the section that contains the staff info
    # It's usually in a list or series of lists after the heading
    next_sibling = staff_heading.find_next_sibling()
    
    while next_sibling and next_sibling.name not in ['h2', 'h3']:
        # Check if this is a category name (usually bold text not in a list)
        if next_sibling.name in ['p', 'div']:
            # Check for category headings (bold text)
            bold_text = next_sibling.find('b')
            if bold_text:
                category = bold_text.get_text(strip=True)
                if category and not any(c in category.lower() for c in ['staff', 'note', 'source', 'reference']):
                    current_category = category
        
        # Process bullet lists
        elif next_sibling.name == 'ul':
            for li in next_sibling.find_all('li', recursive=False):
                text = li.get_text(separator=' ', strip=True)
                
                # Skip if this looks like a category heading
                if li.find('b') and '–' not in text and '-' not in text:
                    current_category = li.find('b').get_text(strip=True)
                    continue
                
                # Parse "Position – Name" or "Position - Name"
                parts = re.split(r'\s*[–—-]\s*', text, 1)
                
                if len(parts) == 2:
                    position, name = parts
                    position = position.strip()
                    name = name.strip()
                    
                    # Clean up name (remove extra info sometimes in parentheses at end)
                    name = re.sub(r'\s+', ' ', name)  # Normalize whitespace
                    
                    # Remove any trailing references like [1] or (since 2024)
                    name = re.sub(r'\s*\[.*?\].*$', '', name)
                    name = re.sub(r'\s*\(.*?\).*$', '', name).strip()
                    
                    if position and name and name.lower() not in ['tbd', 'vacant', '']:
                        if current_category:
                            full_key = f"{current_category}|{position}"
                        else:
                            full_key = position
                        
                        staff_data[full_key] = name
        
        next_sibling = next_sibling.find_next_sibling()
    
    return staff_data if staff_data else None

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
    
    staff_data = extract_staff_from_wikipedia(html_content)
    
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
    logger.info(f"NFL COACHING STAFF SCRAPER - FIXED VERSION")
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
        
        success_count = 0
        for future in as_completed(future_to_task):
            team, year, _ = future_to_task[future]
            result = future.result()
            
            if result:
                all_staff_data.append(result)
                success_count += 1
                staff_count = len(result) - 4  # Subtract metadata columns
                if not HAS_TQDM:
                    logger.info(f"✓ {team} {year}: {staff_count} staff members")
            else:
                if not HAS_TQDM:
                    logger.info(f"✗ {team} {year}: No staff data found")
            
            if HAS_TQDM:
                progress.update(1)
            
            time.sleep(delay / max_workers)
        
        if HAS_TQDM:
            progress.close()
    
    session.close()
    
    logger.info("="*80)
    logger.info(f"Completed: {success_count}/{total} pages ({success_count/total*100:.1f}%)")
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
    parser = argparse.ArgumentParser(description='NFL Coaching Staff Scraper - Fixed Version')
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
        print(df.head(3).to_string(max_colwidth=30))
    else:
        logger.error("❌ No data collected - check your internet connection")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())

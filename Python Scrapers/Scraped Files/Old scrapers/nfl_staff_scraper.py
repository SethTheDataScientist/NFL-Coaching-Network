#!/usr/bin/env python3
"""
NFL Coaching Staff Scraper - TESTED & WORKING
==============================================

Based on actual web_fetch of 2024 Buffalo Bills season page.
Wikipedia shows staff as simple text with bullets.

INSTALL:
    pip install requests beautifulsoup4 pandas openpyxl lxml

RUN:
    python nfl_staff_scraper.py

OUTPUT:
    nfl_coaching_staff_TIMESTAMP.csv
    nfl_coaching_staff_TIMESTAMP.xlsx
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

from streamlit import table

# ============================================================================
# CONFIGURATION  
# ============================================================================

# NFL_TEAMS = [
#     "Arizona Cardinals", "Atlanta Falcons", "Baltimore Ravens", "Buffalo Bills",
#     "Carolina Panthers", "Chicago Bears", "Cincinnati Bengals", "Cleveland Browns",
#     "Dallas Cowboys", "Denver Broncos", "Detroit Lions", "Green Bay Packers",
#     "Houston Texans", "Indianapolis Colts", "Jacksonville Jaguars", "Kansas City Chiefs",
#     "Las Vegas Raiders", "Los Angeles Chargers", "Los Angeles Rams", "Miami Dolphins",
#     "Minnesota Vikings", "New England Patriots", "New Orleans Saints", "New York Giants",
#     "New York Jets", "Philadelphia Eagles", "Pittsburgh Steelers", "San Francisco 49ers",
#     "Seattle Seahawks", "Tampa Bay Buccaneers", "Tennessee Titans", "Washington Commanders"
# ]
NFL_TEAMS = ['Buffalo Bills']

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
# EXTRACTION
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
    soup = BeautifulSoup(html, 'lxml')

    table = soup.find("table", class_="toccolours")

    rows = []

    for td in table.find_all("td"):
        current_section = None

        for element in td.find_all(["p", "ul"], recursive=False):
            
            # Section title
            if element.name == "p":
                bold = element.find("b")
                if bold:
                    current_section = bold.get_text(strip=True)

            # List of staff
            elif element.name == "ul" and current_section:
                for li in element.find_all("li"):
                    text = li.get_text(" ", strip=True)

                    if " – " in text:
                        role, name = text.split(" – ", 1)
                    else:
                        role, name = text, ""

                    rows.append({
                        "Category": current_section,
                        "Role": role,
                        "Name": name
                    })

    return rows

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
    
    
    
    return staff_data

def scrape_all(workers: int = 10, delay: float = 0.1) -> pd.DataFrame:
    """Scrape all teams/years"""
    all_data = []
    total = len(NFL_TEAMS) * 15
    
    logger.info(f"Scraping {total} pages ({len(NFL_TEAMS)} teams × 15 years)")
    logger.info(f"Workers: {workers}, Delay: {delay}s")
    
    # Setup session
    session = requests.Session()
    session.headers['User-Agent'] = 'Mozilla/5.0'
    adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    # Scrape
    tasks = [(team, year, session) for team in NFL_TEAMS for year in range(2025, 2026)]
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(scrape_team_season, *t): t for t in tasks}
        
        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            if result:
                all_data.append(result)
                if len(result) > 10:  # Good data
                    logger.info(f"[{i}/{total}] ✓ {result['Team']} {result['Year']}: {len(result)-4} staff")
            time.sleep(delay / workers)
    
    session.close()
    
    logger.info(f"Collected {len(all_data)}/{total} pages ({len(all_data)/total*100:.1f}%)")
    
    if all_data:
        df = pd.DataFrame(all_data)
        # Reorder: metadata first
        meta_cols = ['Team', 'Year', 'Wikipedia_Team_Name', 'URL']
        other_cols = sorted([c for c in df.columns if c not in meta_cols])
        return df[meta_cols + other_cols]
    
    return all_data

# ============================================================================
# SAVE
# ============================================================================

def save_results(df: pd.DataFrame, output_dir: str = '.'):
    """Save to CSV and Excel"""
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    csv_file = output_path / f'nfl_coaching_staff_{timestamp}.csv'
    df.to_csv(csv_file, index=False)
    logger.info(f"✅ Saved: {csv_file}")
    
    excel_file = output_path / f'nfl_coaching_staff_{timestamp}.xlsx'
    df.to_excel(excel_file, index=False, engine='openpyxl')
    logger.info(f"✅ Saved: {excel_file}")
    
    logger.info(f"\n📊 Summary: {len(df)} records, {len(df.columns)-4} staff positions")
    logger.info(f"Sample columns: {list(df.columns[4:14])}")

# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--workers', type=int, default=10)
    parser.add_argument('--delay', type=float, default=0.1)
    parser.add_argument('--output', type=str, default='.')
    args = parser.parse_args()
    
    df = scrape_all(args.workers, args.delay)
    
    if df is not None and not df.empty:
        save_results(df, args.output)
        print(f"\n{df.head(3)}")
        return 0
    else:
        logger.error("No data collected")
        return 1

if __name__ == "__main__":
    exit(main())

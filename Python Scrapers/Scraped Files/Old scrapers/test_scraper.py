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

def extract_staff(html: str, team: str, year: int) -> Optional[Dict[str, str]]:
    soup = BeautifulSoup(html, 'lxml')

    failed_teams = []
    output_rows = []

    try:
        # 1. Find the Staff heading
        staff_h2 = soup.find("h2", id="Staff")
        #print(staff_h2)
        for elem in staff_h2.next_elements:
            if elem.name == "h2":
                break
            if elem.name == "table":
                if "toccolours" in elem.get("class", []):
                    staff_table = elem
                    break
        rows = [[team, year]]
        #print(staff_table)
        for li in staff_table.find_all("li"):
            # List of staff
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

            rows.append({
                "Role": role,
                "Name": name
            })

            output_rows = rows
    except Exception as e:
        print(f"Error extracting staff for {team} {year}: {e}")
        failed_teams.append([team, year, str(e)])
        return failed_teams

    return output_rows

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

def scrape_team_season(team: str, year: int, session: requests.Session) -> Optional[Dict]:
    """Scrape one team-season"""
    team_name = get_team_name_for_year(team, year)
    url = f"https://en.wikipedia.org/wiki/{year}_{team_name.replace(' ', '_')}_season"
    
    try:
        response = session.get(url, timeout=15)
        response.raise_for_status()
        print(f"Successfully retrieved {url}")
    except:
        print(f"Failed to retrieve {url}")
        return None
    
    staff_data = extract_staff(response.text, team, year)
    
    
    #print(staff_data)
    return staff_data

def scrape_all(workers: int = 10, delay: float = 0.1) -> pd.DataFrame:
    """Scrape all teams/years"""
    all_data = []
    total = len(NFL_TEAMS) * 15
    
    # Setup session
    session = requests.Session()
    session.headers['User-Agent'] = 'Mozilla/5.0'
    adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    # Scrape
    tasks = [(team, year, session) for team in NFL_TEAMS for year in range(1980, 2026)]
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(scrape_team_season, *t): t for t in tasks}
        
        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            if result:
                all_data.append(result)
            time.sleep(delay / workers)
    
    session.close()
    
    
    if all_data:
        df = pd.DataFrame(all_data)
    
    return df

def save_results(df: pd.DataFrame, output_dir: str = '.'):
    """Save to CSV and Excel"""
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    csv_file = output_path / f'nfl_coaching_staff_{timestamp}.csv'
    df.to_csv(csv_file, index=False)

if __name__ == "__main__":
    df = scrape_all()
    save_results(df)
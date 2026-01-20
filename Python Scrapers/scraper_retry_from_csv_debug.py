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
        1980: "St. Louis Cardinals (NFL)",
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

# Teams that didn't exist before certain years
TEAM_INCEPTION_YEARS = {
    "Carolina Panthers": 1995,      # Founded 1995
    "Jacksonville Jaguars": 1995,   # Founded 1995
    "Baltimore Ravens": 1996,        # Founded 1996 (after Browns moved)
    "Houston Texans": 2002,          # Founded 2002
}


# ============================================================================
# SCRAPING FUNCTIONS
# ============================================================================

def extract_staff(html: str, team: str, year: int, debug: bool = False, 
                  pause_on_fail: bool = False, interactive_mode: bool = False) -> Tuple[Optional[List], Optional[str]]:
    """
    Extract staff data from HTML. 
    
    Returns:
        (staff_data, failure_reason) tuple
        - staff_data: List if successful, None if failed
        - failure_reason: None if successful, 'step1_failure' if no anchor found, 'other_failure' otherwise
    """
    soup = BeautifulSoup(html, 'lxml')

    try:
        if debug:
            print(f"\n{'='*80}")
            print(f"DEBUG: Extracting staff for {team} {year}")
            print(f"{'='*80}")
        
        # Step 1: Find anchor with staff/coach/personnel in id
        if debug:
            print("\n[STEP 1] Looking for anchor with id matching 'staff', 'coach', or 'personnel'...")
        
        #anchor = soup.find(id=re.compile(r"(?:^|[_\.])(?:staff|coach|personnel)", re.IGNORECASE))
        SECTION_PRIORITY = ["staff", "coaching", "coaches", "personnel"]

        anchor = None
        for key in SECTION_PRIORITY:
            anchor = soup.find(
                id=re.compile(fr"^(?!toc-|staff_changes)(?:^|[_\.]){key}(?:$|[_\.])", re.IGNORECASE)
            )
            if anchor:
                break


        if debug:
            if anchor:
                print(f"  ✓ Found anchor: <{anchor.name} id='{anchor.get('id')}'>")
                print(f"    Text content: {anchor.get_text(strip=True)[:100]}")
            else:
                print("  ✗ No anchor found matching pattern")
                
                # Show all IDs for debugging
                all_ids = [tag.get('id') for tag in soup.find_all(id=True)]
                print(f"\n  All IDs on page ({len(all_ids)}):")
                for i, id_val in enumerate(all_ids[:20], 1):
                    print(f"    {i}. {id_val}")
                if len(all_ids) > 20:
                    print(f"    ... and {len(all_ids) - 20} more")
                
                # This is a step 1 failure - no staff section found at all
                return None, 'step1_failure'
        
        # If anchor not found and not in debug mode
        if not anchor:
            return None, 'step1_failure'
        
        # Step 2: Find parent header element
        if debug:
            print("\n[STEP 2] Finding parent header element (h2, h3, or h4)...")
        
        if anchor and anchor.name in ("h2", "h3", "h4"):
            section_header = anchor
        else:
            section_header = anchor.find_parent(["h2", "h3", "h4"]) if anchor else None
        
        if debug:
            if section_header:
                print(f"  ✓ Found section header: <{section_header.name}>")
                print(f"    Header text: '{section_header.get_text(strip=True)}'")
                print(f"    Header ID: {section_header.get('id')}")
            else:
                print("  ✗ No parent header found")
                
                if interactive_mode:
                    print("\n" + "="*80)
                    print("MANUAL CLASSIFICATION NEEDED")
                    print("="*80)
                    print("No parent header found for the anchor.")
                    print("\nOptions:")
                    print("  [t] True failure - Add to true_failures (don't retry)")
                    print("  [r] Regular failure - Add to failed_teams (will retry)")
                    print("  [Enter] Skip this classification")
                    
                    choice = input("\nYour choice: ").strip().lower()
                    
                    if choice == 't':
                        return None, 'true_failure'
                    elif choice == 'r':
                        return None, 'other_failure'
                    else:
                        return None, 'other_failure'
                elif pause_on_fail:
                    print("\n⚠️  PAUSING - Press Enter to continue...")
                    input()
                
                return None, 'other_failure'
        
        # Step 3: Search for table with 'toccolours' class
        if debug:
            print("\n[STEP 3] Searching for table with 'toccolours' class after header...")
        
        staff_table = None
        elements_checked = 0
        
        for elem in section_header.next_elements:
            elements_checked += 1
            
            # Stop at next h2
            if elem.name == "h2":
                if debug:
                    print(f"  ⚠️  Reached next h2 section after checking {elements_checked} elements")
                break
            
            # Check for table with toccolours
            if elem.name == "table":
                classes = elem.get("class", [])
                if debug:
                    print(f"  → Found table with classes: {classes}")
                
                if "toccolours" in classes:
                    staff_table = elem
                    if debug:
                        print(f"  ✓ Found toccolours table!")
                    break
        
        if debug:
            print(f"  Total elements checked: {elements_checked}")
            
            if not staff_table:
                print("  ✗ No table with 'toccolours' class found")
                
                # Show all tables in section for debugging
                print("\n  All tables found between header and next h2:")
                table_count = 0
                for elem in section_header.next_elements:
                    if elem.name == "h2":
                        break
                    if elem.name == "table":
                        table_count += 1
                        classes = elem.get("class", [])
                        print(f"    Table {table_count}: classes={classes}")
                        # Show first few rows
                        rows = elem.find_all('tr')[:3]
                        for i, row in enumerate(rows, 1):
                            print(f"      Row {i}: {row.get_text(' ', strip=True)[:80]}")
                
                if table_count == 0:
                    print("    (No tables found)")
        
        # Step 3.5: FALLBACK - If no table found, look for <ul> directly after header
        staff_ul = None
        if not staff_table:
            if debug:
                print("\n[STEP 3.5 - FALLBACK] No table found, searching for <ul> list after header...")
            
            elements_checked = 0
            for elem in section_header.next_elements:
                elements_checked += 1
                
                # Stop at next h2
                if elem.name == "h2":
                    if debug:
                        print(f"  ⚠️  Reached next h2 section after checking {elements_checked} elements")
                    break
                
                # Check for ul element
                if elem.name == "ul":
                    # Make sure it has li elements with staff data
                    li_elements = elem.find_all("li", recursive=False)
                    if li_elements:
                        staff_ul = elem
                        if debug:
                            print(f"  ✓ Found <ul> with {len(li_elements)} <li> elements!")
                        break
            
            if debug:
                print(f"  Total elements checked: {elements_checked}")
                
                if not staff_ul:
                    print("  ✗ No <ul> list found either")
                    
                    # Show all ul elements for debugging
                    print("\n  All <ul> elements found between header and next h2:")
                    ul_count = 0
                    for elem in section_header.next_elements:
                        if elem.name == "h2":
                            break
                        if elem.name == "ul":
                            ul_count += 1
                            li_items = elem.find_all("li", recursive=False)
                            print(f"    UL {ul_count}: {len(li_items)} direct <li> items")
                            # Show first few items
                            for i, li in enumerate(li_items[:3], 1):
                                print(f"      Item {i}: {li.get_text(' ', strip=True)[:80]}")
                    
                    if ul_count == 0:
                        print("    (No <ul> elements found)")
        
        # If neither table nor ul found, fail
        if not staff_table and not staff_ul:
            if debug or interactive_mode:
                print("\n  ✗ FAILED: No staff data container found (neither table nor ul)")
                
                if interactive_mode:
                    print("\n" + "="*80)
                    print("MANUAL CLASSIFICATION NEEDED")
                    print("="*80)
                    print("No staff data container found (no table or ul with staff data).")
                    print("\nOptions:")
                    print("  [t] True failure - Add to true_failures (don't retry)")
                    print("  [r] Regular failure - Add to failed_teams (will retry)")
                    print("  [Enter] Regular failure (default)")
                    
                    choice = input("\nYour choice: ").strip().lower()
                    
                    if choice == 't':
                        return None, 'true_failure'
                    else:
                        return None, 'other_failure'
                elif pause_on_fail:
                    print("\n⚠️  PAUSING - Press Enter to continue...")
                    input()
            
            return None, 'other_failure'
        
        # Step 4: Extract staff from list items
        if debug:
            if staff_table:
                print("\n[STEP 4] Extracting staff from <li> elements in table...")
            else:
                print("\n[STEP 4] Extracting staff from <li> elements in <ul>...")
        
        # Use whichever container we found
        container = staff_table if staff_table else staff_ul
        
        rows = [[team, year]]
        li_elements = container.find_all("li")
        
        if debug:
            print(f"  Found {len(li_elements)} <li> elements")
        
        for i, li in enumerate(li_elements, 1):
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

            if debug and i <= 5:  # Show first 5
                print(f"    {i}. '{text}' → Role: '{role}', Name: '{name}'")
            
            rows.append({"Role": role, "Name": name})
        
        if debug:
            if len(li_elements) > 5:
                print(f"    ... and {len(li_elements) - 5} more")
            print(f"\n  ✓ Successfully extracted {len(rows) - 1} staff entries from {'table' if staff_table else 'ul'}")
        
        return rows, None
        
    except Exception as e:
        if debug:
            print(f"\n  ✗ EXCEPTION: {type(e).__name__}: {str(e)}")
            import traceback
            traceback.print_exc()
        
        if pause_on_fail or interactive_mode:
            if interactive_mode:
                print("\n" + "="*80)
                print("MANUAL CLASSIFICATION NEEDED")
                print("="*80)
                print(f"Exception occurred: {type(e).__name__}: {str(e)}")
                print("\nOptions:")
                print("  [t] True failure - Add to true_failures (don't retry)")
                print("  [r] Regular failure - Add to failed_teams (will retry)")
                print("  [Enter] Regular failure (default)")
                
                choice = input("\nYour choice: ").strip().lower()
                
                if choice == 't':
                    return None, 'true_failure'
                else:
                    return None, 'other_failure'
            else:
                print("\n⚠️  PAUSING - Press Enter to continue...")
                input()
        
        return None, 'other_failure'


@lru_cache(maxsize=128)
def team_existed_in_year(team: str, year: int) -> bool:
    """Check if a team existed in the given year"""
    if team in TEAM_INCEPTION_YEARS:
        inception_year = TEAM_INCEPTION_YEARS[team]
        return year >= inception_year
    return True  # All other teams existed


@lru_cache(maxsize=128)
def get_team_name_for_year(team: str, year: int) -> str:
    """Get correct team name for year (handles relocations)"""
    if team not in TEAM_NAME_HISTORY:
        return team

    history = TEAM_NAME_HISTORY[team]
    current_name = team

    for transition_year in sorted(history.keys()):
        if year >= transition_year:
            current_name = history[transition_year]
        else:
            break

    return current_name


def scrape_team_season(team: str, year: int, session: requests.Session, 
                       attempt: int = 1, debug: bool = False, 
                       pause_on_fail: bool = False, interactive_mode: bool = False) -> Tuple[Optional[List], Optional[Tuple], Optional[Tuple], bool]:
    """
    Scrape one team-season.
    
    Returns:
        (staff_data, failure_info, true_failure_info, should_skip) tuple
        - staff_data: List if successful, None if failed
        - failure_info: (team, year, error) if regular failure, None otherwise
        - true_failure_info: (team, year, error) if true failure, None otherwise
        - should_skip: True if team didn't exist (don't add to any list), False otherwise
    """
    # Check if team existed in this year
    if not team_existed_in_year(team, year):
        print(f"⊘ [{attempt}] {team} {year}: Team did not exist in this year (skipped)")
        return None, None, None, True  # Skip this team-year combo
    
    team_name = get_team_name_for_year(team, year)
    url = f"https://en.wikipedia.org/wiki/{year}_{team_name.replace(' ', '_')}_season"
    
    if debug:
        print(f"\n{'='*80}")
        print(f"SCRAPING: {team} {year} (Attempt {attempt})")
        print(f"{'='*80}")
        print(f"URL: {url}")
    
    try:
        if debug:
            print(f"Fetching page...")
        
        response = session.get(url, timeout=15)
        response.raise_for_status()
        
        if debug:
            print(f"✓ Page fetched successfully ({len(response.text)} bytes)")
        
        staff_data, failure_reason = extract_staff(response.text, team, year, debug=debug, 
                                                     pause_on_fail=pause_on_fail, 
                                                     interactive_mode=interactive_mode)
        
        if staff_data:
            print(f"✓ [{attempt}] {team} {year}")
            return staff_data, None, None, False
        else:
            # Determine which type of failure
            if failure_reason == 'step1_failure':
                error_msg = "No staff section found (Step 1 failure)"
                print(f"⚠️  [{attempt}] {team} {year}: {error_msg} (TRUE FAILURE)")
                return None, None, (team, year, error_msg), False  # true_failure
            elif failure_reason == 'true_failure':
                error_msg = "Manually classified as true failure"
                print(f"⚠️  [{attempt}] {team} {year}: {error_msg} (TRUE FAILURE)")
                return None, None, (team, year, error_msg), False  # true_failure
            else:
                error_msg = "No staff data found"
            print(f"✗ [{attempt}] {team} {year}: {error_msg}")
            return None, (team, year, error_msg), None, False
                
    except requests.exceptions.RequestException as e:
        error_msg = f"Request failed: {str(e)[:50]}"
        print(f"✗ [{attempt}] {team} {year}: {error_msg}")
        print(f"    URL: {url}")
        
        if debug and pause_on_fail:
            print("\n⚠️  PAUSING - Press Enter to continue...")
            input()
        
        return None, (team, year, error_msg), None, False
    except Exception as e:
        error_msg = f"Error: {str(e)[:50]}"
        print(f"✗ [{attempt}] {team} {year}: {error_msg}")
        print(f"    URL: {url}")
        
        if debug:
            import traceback
            traceback.print_exc()
        
        if debug and pause_on_fail:
            print("\n⚠️  PAUSING - Press Enter to continue...")
            input()
        
        return None, (team, year, error_msg), None, False


def load_failed_teams(csv_path: str) -> List[Tuple[str, int]]:
    """Load failed teams from CSV file"""
    df = pd.read_csv(csv_path)
    failed_teams = [(row['Team'], int(row['Year'])) for _, row in df.iterrows()]
    return failed_teams


def scrape_failed_teams(failed_csv: str, workers: int = 10, delay: float = 0.1, 
                       max_retries: int = 3, debug: bool = False,
                       pause_on_fail: bool = False, interactive_mode: bool = False) -> Tuple[List, List, List]:
    """
    Scrape only the teams/years from the failed teams CSV.
    
    Returns:
        (successful_data, failed_teams, true_failures) tuple
    """
    all_data = []
    failed_teams = []
    true_failures = []
    
    # Load failed teams from CSV
    print("="*80)
    print("LOADING FAILED TEAMS")
    print("="*80)
    tasks = load_failed_teams(failed_csv)
    total = len(tasks)
    print(f"Loaded {total} failed team-seasons from {failed_csv}\n")
    
    if debug:
        print(f"Debug mode: {'ON' if debug else 'OFF'}")
        print(f"Pause on fail: {'ON' if pause_on_fail else 'OFF'}")
        print(f"Interactive mode: {'ON' if interactive_mode else 'OFF'}")
        if interactive_mode:
            print(f"Note: Interactive mode enabled - you will classify failures manually")
        print(f"Note: Debug mode will disable threading for sequential execution\n")
    
    session = requests.Session()
    session.headers['User-Agent'] = 'Mozilla/5.0'
    adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    # Initial scrape
    print("="*80)
    print("INITIAL RETRY SCRAPE")
    print("="*80)
    print(f"Scraping {total} team-seasons with {workers if not debug else 1} workers...\n")
    
    if debug:
        # Sequential execution in debug mode
        skipped_count = 0
        for team, year in tasks:
            result, failure, true_failure, should_skip = scrape_team_season(
                team, year, session, 1, debug=debug, 
                pause_on_fail=pause_on_fail, 
                interactive_mode=interactive_mode
            )
            if should_skip:
                skipped_count += 1
            elif result:
                all_data.append(result)
            elif true_failure:
                true_failures.append(true_failure)
            elif failure:
                failed_teams.append(failure)
            time.sleep(delay)
        
        if skipped_count > 0:
            print(f"\n⊘ Skipped {skipped_count} team-seasons (team didn't exist in that year)")
    else:
        # Parallel execution
        skipped_count = 0
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(scrape_team_season, team, year, session, 1, debug=False, pause_on_fail=False, interactive_mode=False): (team, year) 
                for team, year in tasks
            }
            
            for future in as_completed(futures):
                result, failure, true_failure, should_skip = future.result()
                if should_skip:
                    skipped_count += 1
                elif result:
                    all_data.append(result)
                elif true_failure:
                    true_failures.append(true_failure)
                elif failure:
                    failed_teams.append(failure)
                time.sleep(delay / workers)
        
        if skipped_count > 0:
            print(f"\n⊘ Skipped {skipped_count} team-seasons (team didn't exist in that year)")
    
    # Retry failed teams (skip in debug mode to avoid repetition)
    # NOTE: true_failures are NEVER retried
    retry_count = 1
    while failed_teams and retry_count <= max_retries and not debug:
        print(f"\n{'='*80}")
        print(f"RETRY ATTEMPT {retry_count}/{max_retries}")
        print(f"{'='*80}")
        print(f"Retrying {len(failed_teams)} failed team-seasons...")
        print(f"(Not retrying {len(true_failures)} true failures)\n")
        
        teams_to_retry = [(team, year) for team, year, _ in failed_teams]
        failed_teams = []
        time.sleep(2)  # Brief pause before retry
        
        with ThreadPoolExecutor(max_workers=max(1, workers // 2)) as executor:
            futures = {
                executor.submit(scrape_team_season, team, year, session, retry_count + 1, interactive_mode=False): (team, year)
                for team, year in teams_to_retry
            }
            
            for future in as_completed(futures):
                result, failure, true_failure, should_skip = future.result()
                if should_skip:
                    # Don't add back to failed teams
                    pass
                elif result:
                    all_data.append(result)
                elif true_failure:
                    # Don't retry true failures
                    true_failures.append(true_failure)
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
    print(f"⚠️  True Failures: {len(true_failures)} ({len(true_failures)/total*100:.1f}%)")
    print(f"✗ Failed: {len(failed_teams)} ({len(failed_teams)/total*100:.1f}%)")
    
    if true_failures:
        print(f"\n⚠️  True Failures (no staff section found - showing first 10):")
        for team, year, error in true_failures[:10]:
            print(f"  - {team} {year}: {error}")
        if len(true_failures) > 10:
            print(f"  ... and {len(true_failures) - 10} more")
    
    if failed_teams:
        print(f"\n✗ Regular Failures (will be retried - showing first 10):")
        for team, year, error in failed_teams[:10]:
            print(f"  - {team} {year}: {error}")
        if len(failed_teams) > 10:
            print(f"  ... and {len(failed_teams) - 10} more")
    
    return all_data, failed_teams, true_failures


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

def save_results(raw_data: List, failed_teams: List, true_failures: List, 
                 output_dir: str = '.', format_type: str = 'both'):
    """Save results in specified format(s)"""
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True, parents=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Save true failures (teams with no staff section found)
    if true_failures:
        true_failures_file = output_path / f'true_failures_{timestamp}.csv'
        true_failures_df = pd.DataFrame(true_failures, columns=['Team', 'Year', 'Error'])
        true_failures_df.to_csv(true_failures_file, index=False)
        print(f"\n⚠️  True Failures: {true_failures_file} ({len(true_failures_df)} rows)")
        print(f"    (These will NOT be retried - no staff section exists)")
    
    # Save regular failed teams (will be retried)
    if failed_teams:
        failed_file = output_path / f'failed_teams_{timestamp}.csv'
        failed_df = pd.DataFrame(failed_teams, columns=['Team', 'Year', 'Error'])
        failed_df.to_csv(failed_file, index=False)
        print(f"\n✗ Regular Failures: {failed_file} ({len(failed_df)} rows)")
        print(f"    (These can be retried)")
    
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
        
        csv_file = output_path / f'nfl_staff_wide_retry_{timestamp}.csv'
        df_wide.to_csv(csv_file, index=False)
        print(f"  ✅ CSV: {csv_file}")
        
        excel_file = output_path / f'nfl_staff_wide_retry_{timestamp}.xlsx'
        df_wide.to_excel(excel_file, index=False, engine='openpyxl')
        print(f"  ✅ Excel: {excel_file}")
        print(f"  Rows: {len(df_wide)}, Columns: {len(df_wide.columns)}")
    
    if format_type in ['long', 'both']:
        print("\n📋 LONG FORMAT")
        df_long = format_to_long(raw_data)
        
        csv_file = output_path / f'nfl_staff_long_retry_{timestamp}.csv'
        df_long.to_csv(csv_file, index=False)
        print(f"  ✅ CSV: {csv_file}")
        
        excel_file = output_path / f'nfl_staff_long_retry_{timestamp}.xlsx'
        df_long.to_excel(excel_file, index=False, engine='openpyxl')
        print(f"  ✅ Excel: {excel_file}")
        print(f"  Rows: {len(df_long)}, Unique roles: {df_long['Role'].nunique()}")


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='NFL Coaching Staff Scraper - Retry Failed Teams from CSV (with Debug Mode)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Retry failed teams with default settings
  python scraper_retry_from_csv_debug.py --failed-csv failed_teams_20260114_111542.csv
  
  # Debug mode with detailed output
  python scraper_retry_from_csv_debug.py --failed-csv failed_teams.csv --debug
  
  # Interactive mode - manually classify failures
  python scraper_retry_from_csv_debug.py --failed-csv failed_teams.csv --debug --interactive
  
  # Debug mode with pause on each failure
  python scraper_retry_from_csv_debug.py --failed-csv failed_teams.csv --debug --pause-on-fail
  
  # Retry with custom settings
  python scraper_retry_from_csv_debug.py --failed-csv failed_teams.csv --workers 20 --max-retries 5
        """
    )
    
    parser.add_argument('--failed-csv', type=str, required=True,
                        help='Path to CSV file with failed teams')
    parser.add_argument('--workers', type=int, default=10, 
                        help='Number of parallel workers (default: 10, ignored in debug mode)')
    parser.add_argument('--delay', type=float, default=0.1, 
                        help='Delay between requests (default: 0.1)')
    parser.add_argument('--max-retries', type=int, default=3, 
                        help='Maximum retry attempts (default: 3, skipped in debug mode)')
    parser.add_argument('--format', type=str, choices=['wide', 'long', 'both'], 
                        default='both', help='Output format (default: both)')
    parser.add_argument('--output', type=str, default='.', 
                        help='Output directory (default: current)')
    parser.add_argument('--debug', action='store_true',
                        help='Enable detailed debug output (disables threading)')
    parser.add_argument('--pause-on-fail', action='store_true',
                        help='Pause and wait for Enter key when scraping fails (requires --debug)')
    parser.add_argument('--interactive', action='store_true',
                        help='Manually classify failures as true failures or regular failures (requires --debug)')
    
    args = parser.parse_args()
    
    if args.pause_on_fail and not args.debug:
        print("⚠️  Warning: --pause-on-fail requires --debug mode. Enabling debug mode.")
        args.debug = True
    
    if args.interactive and not args.debug:
        print("⚠️  Warning: --interactive requires --debug mode. Enabling debug mode.")
        args.debug = True
    
    print("="*80)
    print("NFL COACHING STAFF SCRAPER - RETRY FROM CSV (DEBUG)")
    print("="*80)
    print(f"Failed teams CSV: {args.failed_csv}")
    print(f"Workers: {args.workers}{' (ignored in debug mode)' if args.debug else ''}")
    print(f"Max retries: {args.max_retries}{' (skipped in debug mode)' if args.debug else ''}")
    print(f"Format: {args.format}")
    print(f"Debug mode: {'ON' if args.debug else 'OFF'}")
    print(f"Pause on fail: {'ON' if args.pause_on_fail else 'OFF'}")
    print(f"Interactive mode: {'ON' if args.interactive else 'OFF'}")
    print()
    
    # Scrape with retries
    raw_data, failed_teams, true_failures = scrape_failed_teams(
        failed_csv=args.failed_csv,
        workers=args.workers,
        delay=args.delay,
        max_retries=args.max_retries,
        debug=args.debug,
        pause_on_fail=args.pause_on_fail,
        interactive_mode=args.interactive
    )
    
    # Format and save
    save_results(raw_data, failed_teams, true_failures, args.output, args.format)
   
    print(f"\n{'='*80}")
    print("✅ COMPLETE")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()

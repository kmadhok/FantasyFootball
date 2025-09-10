#!/usr/bin/env python3
"""
RotoBaller Selenium Scraper

Uses Selenium WebDriver to handle JavaScript-rendered content on RotoBaller fantasy football rankings.
This version waits for content to load before scraping.
"""

import argparse
import csv
import re
import sys
import time
from io import StringIO
from typing import List, Dict, Optional

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException


class RotoBallerSeleniumScraper:
    def __init__(self, headless: bool = True):
        self.driver = None
        self.headless = headless
        
    def init_driver(self):
        """Initialize Chrome WebDriver with appropriate options."""
        if self.driver:
            return
            
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.set_page_load_timeout(60)
            print("✅ Chrome WebDriver initialized successfully")
        except WebDriverException as e:
            print(f"❌ Failed to initialize Chrome WebDriver: {e}")
            print("💡 Make sure Chrome browser is installed on your system")
            print("💡 You may need to install ChromeDriver separately")
            raise
    
    def fetch_and_wait_for_content(self, url: str, wait_time: int = 10) -> str:
        """Fetch URL and wait for JavaScript content to load."""
        if not self.driver:
            self.init_driver()
            
        print(f"🌐 Loading: {url}")
        
        # Load the page
        self.driver.get(url)
        
        print(f"⏳ Waiting for JavaScript content to load (up to {wait_time}s)...")
        
        # Wait for the Angular app to initialize
        try:
            # Wait for the rankings container to appear
            WebDriverWait(self.driver, wait_time).until(
                EC.presence_of_element_located((By.CLASS_NAME, "ionutRankings"))
            )
            print("✅ Found rankings container")
            
            # Additional wait for content to populate
            time.sleep(3)
            
            # Wait for any table or list elements that might contain player data
            self.wait_for_player_data(wait_time)
            
        except TimeoutException:
            print("⚠️ Timeout waiting for rankings container - proceeding anyway")
        
        # Get the page source after JavaScript has run
        html = self.driver.page_source
        print(f"📄 Page source length: {len(html)} characters")
        
        return html
    
    def wait_for_player_data(self, wait_time: int):
        """Wait for player data to appear in various possible formats."""
        selectors_to_try = [
            "table tbody tr",  # Standard table rows
            ".player-row",     # Common player row class
            "[data-player]",   # Data attributes
            "div:contains('QB')", "div:contains('RB')", "div:contains('WR')", "div:contains('TE')"  # Position indicators
        ]
        
        for selector in selectors_to_try:
            try:
                WebDriverWait(self.driver, 2).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                )
                print(f"✅ Found player data using selector: {selector}")
                return
            except TimeoutException:
                continue
        
        print("⚠️ No specific player data selectors found - relying on general content")
    
    def find_rankings_in_html(self, html: str) -> List[pd.DataFrame]:
        """Extract tables from the HTML source."""
        print("🔍 Searching for tables in HTML...")
        
        # Try pandas first
        try:
            dfs = pd.read_html(StringIO(html))
            if dfs:
                print(f"✅ Found {len(dfs)} tables with pandas")
                return dfs
        except ValueError as e:
            print(f"❌ Pandas read_html failed: {e}")
        
        # Try BeautifulSoup as fallback
        soup = BeautifulSoup(html, 'html.parser')
        tables = soup.find_all('table')
        print(f"🔍 Found {len(tables)} <table> elements with BeautifulSoup")
        
        dfs = []
        for i, table in enumerate(tables):
            try:
                df = pd.read_html(str(table))[0]
                dfs.append(df)
                print(f"✅ Successfully parsed table {i+1} with shape: {df.shape}")
            except Exception as e:
                print(f"❌ Failed to parse table {i+1}: {e}")
                continue
        
        return dfs
    
    def debug_page_content(self, html: str):
        """Debug what content is available in the page."""
        soup = BeautifulSoup(html, 'html.parser')
        
        print(f"\n=== 🔍 DEBUGGING PAGE CONTENT ===")
        print(f"HTML length: {len(html):,} characters")
        print(f"Page title: {soup.title.string if soup.title else 'No title'}")
        
        # Look for tables
        tables = soup.find_all('table')
        print(f"📊 Found {len(tables)} <table> elements")
        
        # Look for common fantasy elements
        positions = ['QB', 'RB', 'WR', 'TE']
        for pos in positions:
            matches = soup.find_all(string=re.compile(pos, re.I))
            print(f"🏈 Found {len(matches)} mentions of '{pos}'")
        
        # Look for player name patterns
        player_pattern = r'\b[A-Z][a-z]+ [A-Z][a-z]+\b'
        player_matches = re.findall(player_pattern, soup.get_text())
        unique_players = set(match for match in player_matches if len(match) > 5)
        print(f"👤 Found {len(unique_players)} potential player names")
        
        if unique_players:
            print("Sample players found:")
            for i, player in enumerate(list(unique_players)[:10]):
                print(f"  - {player}")
        
        # Look for Angular/JavaScript content containers
        ranking_containers = soup.find_all(class_=re.compile(r'ionut|ranking', re.I))
        print(f"📱 Found {len(ranking_containers)} ranking-related containers")
        
        print("=== END DEBUG ===\n")
    
    def select_best_table(self, dfs: List[pd.DataFrame]) -> Optional[pd.DataFrame]:
        """Select the table that looks most like fantasy rankings."""
        if not dfs:
            return None
        
        best_table = None
        best_score = 0
        
        for i, df in enumerate(dfs):
            score = 0
            cols = [str(c).strip().lower() for c in df.columns]
            
            print(f"📊 Table {i+1} analysis:")
            print(f"   Shape: {df.shape}")
            print(f"   Columns: {cols}")
            
            # Score based on expected fantasy columns
            if any('player' in col or 'name' in col for col in cols):
                score += 5
                print("   ✅ Has player/name column")
            
            if any(col in ['pos', 'position', 'pos.'] for col in cols):
                score += 3
                print("   ✅ Has position column")
            
            if any('tier' in col for col in cols):
                score += 2
                print("   ✅ Has tier column")
            
            if any(col in ['rank', '#', 'ranking', 'rk'] for col in cols):
                score += 2
                print("   ✅ Has rank column")
            
            if any(col in ['team', 'tm'] for col in cols):
                score += 1
                print("   ✅ Has team column")
            
            # Bonus for reasonable size
            if 10 <= len(df) <= 500:  # Reasonable number of players
                score += 1
                print("   ✅ Reasonable row count")
            
            if 3 <= len(df.columns) <= 15:  # Reasonable number of columns
                score += 1
                print("   ✅ Reasonable column count")
            
            print(f"   Score: {score}")
            
            if score > best_score:
                best_table = df
                best_score = score
        
        if best_table is not None:
            print(f"🏆 Selected table with score {best_score}")
        else:
            print("❌ No suitable table found")
        
        return best_table
    
    def parse_players_from_table(self, df: pd.DataFrame) -> List[Dict[str, Optional[str]]]:
        """Extract player data from the rankings table."""
        players = []
        
        print(f"📊 Parsing {len(df)} rows from table...")
        
        # Handle multi-level columns by flattening them
        if hasattr(df.columns, 'levels'):  # MultiIndex columns
            print("🔧 Flattening multi-level column headers...")
            df.columns = [' '.join(col).strip() if isinstance(col, tuple) else col for col in df.columns.values]
        
        print(f"📋 Final columns: {list(df.columns)}")
        
        for idx, row in df.iterrows():
            player = {
                'name': None,
                'position': None,
                'team': None,
                'rank': None,
                'tier': None
            }
            
            # Extract player name - try various column patterns
            name_patterns = [
                r'.*player.*name.*',
                r'.*name.*',
                r'.*player.*',
            ]
            
            for pattern in name_patterns:
                matching_cols = [col for col in df.columns if re.search(pattern, col, re.IGNORECASE)]
                for col in matching_cols:
                    if not pd.isna(row[col]):
                        name_text = str(row[col]).strip()
                        if name_text and len(name_text) > 2 and not name_text.isdigit():
                            # Clean up duplicated names (e.g., "Saquon Barkley Saquon Ba" -> "Saquon Barkley")
                            words = name_text.split()
                            if len(words) >= 2:
                                first_name = words[0]
                                # Check if the name is duplicated
                                if len(words) > 2 and first_name in name_text[len(first_name)+1:]:
                                    # Find where the duplication starts
                                    for i in range(1, len(words)):
                                        if words[i] == first_name:
                                            name_text = ' '.join(words[:i])
                                            break
                            
                            # Handle "Name Team - POS" format
                            match = re.match(r"^(?P<name>.+?)\s+(?P<team>[A-Za-z]{2,4})\s*-\s*(?P<pos>[A-Za-z/]+)$", name_text)
                            if match:
                                player['name'] = match.group('name').strip()
                                player['team'] = match.group('team').upper()
                                player['position'] = match.group('pos').upper()
                            else:
                                player['name'] = name_text.strip()
                            break
                if player['name']:
                    break
            
            # Extract position if not already found
            if not player['position']:
                pos_patterns = [r'.*pos.*', r'.*position.*']
                for pattern in pos_patterns:
                    matching_cols = [col for col in df.columns if re.search(pattern, col, re.IGNORECASE)]
                    for col in matching_cols:
                        if not pd.isna(row[col]):
                            pos_value = str(row[col]).upper().strip()
                            if pos_value in ['QB', 'RB', 'WR', 'TE']:
                                player['position'] = pos_value
                                break
                    if player['position']:
                        break
            
            # Extract team if not already found
            if not player['team']:
                team_patterns = [r'.*team.*', r'.*tm.*']
                for pattern in team_patterns:
                    matching_cols = [col for col in df.columns if re.search(pattern, col, re.IGNORECASE)]
                    for col in matching_cols:
                        if not pd.isna(row[col]):
                            team_value = str(row[col]).upper().strip()
                            if len(team_value) >= 2:  # Valid team abbreviation
                                player['team'] = team_value
                                break
                    if player['team']:
                        break
            
            # Extract rank
            rank_patterns = [r'.*rank.*', r'.*#.*', r'.*rk.*']
            for pattern in rank_patterns:
                matching_cols = [col for col in df.columns if re.search(pattern, col, re.IGNORECASE)]
                for col in matching_cols:
                    if not pd.isna(row[col]):
                        try:
                            rank_value = int(float(row[col]))
                            if 1 <= rank_value <= 1000:  # Reasonable rank range
                                player['rank'] = str(rank_value)
                                break
                        except (ValueError, TypeError):
                            pass
                if player['rank']:
                    break
            
            # Extract tier
            tier_patterns = [r'.*tier.*']
            for pattern in tier_patterns:
                matching_cols = [col for col in df.columns if re.search(pattern, col, re.IGNORECASE)]
                for col in matching_cols:
                    if not pd.isna(row[col]):
                        tier_value = str(row[col]).strip()
                        if tier_value and tier_value != 'nan':
                            player['tier'] = tier_value
                            break
                if player['tier']:
                    break
            
            # Only include if we have a valid player name
            if player['name'] and len(player['name']) > 2 and not player['name'].isdigit():
                # Clean up the name
                player['name'] = re.sub(r'\s+', ' ', player['name']).strip()
                # Skip obvious non-player entries
                if not any(skip_word in player['name'].lower() for skip_word in ['premium', 'betting', 'lineup', 'industry', 'weekly']):
                    players.append(player)
        
        print(f"✅ Extracted {len(players)} valid players")
        return players
    
    def scrape_rankings(self, url: str, limit: Optional[int] = None, wait_time: int = 10) -> List[Dict[str, Optional[str]]]:
        """Main scraping function."""
        try:
            # Get the page content
            html = self.fetch_and_wait_for_content(url, wait_time)
            
            # Debug the content
            self.debug_page_content(html)
            
            # Find tables
            dfs = self.find_rankings_in_html(html)
            
            if not dfs:
                print("❌ No tables found in the page")
                return []
            
            # Select the best table
            table = self.select_best_table(dfs)
            
            if table is None:
                print("❌ Could not identify a suitable rankings table")
                return []
            
            # Parse players
            players = self.parse_players_from_table(table)
            
            # Apply limit
            if limit and len(players) > limit:
                players = players[:limit]
                print(f"🔄 Limited results to {limit} players")
            
            return players
            
        except Exception as e:
            print(f"❌ Error during scraping: {e}")
            raise
    
    def print_rankings(self, players: List[Dict[str, Optional[str]]]):
        """Print rankings in a formatted table."""
        if not players:
            print("❌ No players to display!")
            return
        
        print(f"\n🏈 FANTASY FOOTBALL RANKINGS ({len(players)} players)")
        print(f"{'Rank':<6} {'Player':<25} {'Pos':<4} {'Team':<6} {'Tier':<6}")
        print("=" * 53)
        
        for i, player in enumerate(players, 1):
            rank = player.get('rank') or str(i)
            name = (player.get('name') or 'Unknown')[:24]
            pos = player.get('position') or 'N/A'
            team = player.get('team') or 'N/A'
            tier = player.get('tier') or 'N/A'
            
            print(f"{rank:<6} {name:<25} {pos:<4} {team:<6} {tier:<6}")
    
    def save_to_csv(self, players: List[Dict[str, Optional[str]]], filename: str):
        """Save rankings to CSV file."""
        if not players:
            print("❌ No players to save!")
            return
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['rank', 'name', 'position', 'team', 'tier']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for i, player in enumerate(players, 1):
                row = player.copy()
                if not row.get('rank'):
                    row['rank'] = str(i)
                writer.writerow(row)
        
        print(f"✅ Saved {len(players)} players to {filename}")
    
    def cleanup(self):
        """Close the browser and clean up resources."""
        if self.driver:
            try:
                self.driver.quit()
                print("🔄 Browser closed successfully")
            except Exception as e:
                print(f"⚠️ Error closing browser: {e}")
            finally:
                self.driver = None


def main():
    parser = argparse.ArgumentParser(
        description='Scrape RotoBaller Fantasy Football Rankings using Selenium',
        epilog='This scraper uses Chrome browser automation to handle JavaScript content.'
    )
    parser.add_argument('--url', 
                       default='https://www.rotoballer.com/nfl-fantasy-football-rankings-tiered-ppr/265860',
                       help='RotoBaller rankings URL')
    parser.add_argument('--limit', type=int, default=50,
                       help='Limit number of players to scrape (default: 50)')
    parser.add_argument('--wait', type=int, default=10,
                       help='Seconds to wait for JavaScript to load (default: 10)')
    parser.add_argument('--csv', 
                       help='Save results to CSV file')
    parser.add_argument('--show-browser', action='store_true',
                       help='Show browser window (default: headless)')
    parser.add_argument('--quiet', action='store_true',
                       help='Suppress detailed output')
    
    args = parser.parse_args()
    
    scraper = RotoBallerSeleniumScraper(headless=not args.show_browser)
    
    try:
        print("🚀 Starting Selenium-powered RotoBaller scraping...")
        start_time = time.time()
        
        players = scraper.scrape_rankings(args.url, args.limit, args.wait)
        
        elapsed = time.time() - start_time
        
        if args.csv:
            scraper.save_to_csv(players, args.csv)
        
        if not args.quiet:
            scraper.print_rankings(players)
        
        print(f"\n🎉 Successfully scraped {len(players)} players in {elapsed:.1f} seconds")
        
        if len(players) == 0:
            print("💡 Try increasing --wait time or check if the page structure changed")
            print("💡 Use --show-browser to see what's happening in the browser")
        
        return 0
        
    except KeyboardInterrupt:
        print("\n⏹️ Scraping interrupted by user")
        return 1
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    finally:
        scraper.cleanup()


if __name__ == '__main__':
    sys.exit(main())
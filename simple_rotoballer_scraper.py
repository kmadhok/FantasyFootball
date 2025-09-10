#!/usr/bin/env python3
"""
Simple RotoBaller Fantasy Football Rankings Scraper

A lightweight scraper that extracts fantasy football rankings from RotoBaller
without database dependencies. Outputs to console or CSV.
"""

import argparse
import csv
import re
import sys
from io import StringIO
from typing import List, Dict, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup


class SimpleRotoBallerScraper:
    def __init__(self):
        self.session = requests.Session()
        # Mimic a real browser but avoid brotli compression issues
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate",  # Remove 'br' to avoid brotli issues
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        })

    def fetch_html(self, url: str) -> str:
        """Fetch HTML content from URL."""
        print(f"Fetching: {url}")
        
        # Clean up URL (remove fragment identifiers that servers might not handle)
        if '#!/' in url:
            clean_url = url.split('#!/')[0]
            print(f"Cleaned URL: {clean_url}")
            url = clean_url
        
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        
        print(f"Response status: {resp.status_code}")
        print(f"Response headers: {dict(resp.headers)}")
        print(f"Content encoding: {resp.encoding}")
        
        # Ensure we get proper text content
        html = resp.text
        
        # Check if response looks valid
        if not html or len(html) < 100:
            raise ValueError(f"Received empty or very short response: {len(html)} characters")
            
        return html

    def find_tables_pandas(self, html: str) -> List[pd.DataFrame]:
        """Try to extract tables using pandas read_html."""
        try:
            return pd.read_html(StringIO(html))
        except ValueError as e:
            print(f"Pandas read_html failed: {e}")
            return []

    def find_tables_beautifulsoup(self, html: str) -> List[pd.DataFrame]:
        """Extract tables using BeautifulSoup as fallback."""
        soup = BeautifulSoup(html, 'html.parser')
        tables = soup.find_all('table')
        dataframes = []
        
        print(f"Found {len(tables)} <table> elements in HTML")
        
        for i, table in enumerate(tables):
            try:
                df = pd.read_html(str(table))[0]
                dataframes.append(df)
                print(f"Found table {i+1} with shape: {df.shape}")
            except Exception as e:
                print(f"Failed to parse table {i+1}: {e}")
                continue
        
        return dataframes

    def debug_page_content(self, html: str):
        """Debug what content is actually in the page."""
        print(f"HTML length: {len(html)} characters")
        print(f"First 500 characters of HTML:")
        print(html[:500])
        print("\n" + "="*50 + "\n")
        
        soup = BeautifulSoup(html, 'html.parser')
        
        print(f"Page title: {soup.title.string if soup.title else 'No title'}")
        
        # Look for JavaScript data
        scripts = soup.find_all('script')
        print(f"Found {len(scripts)} script tags")
        
        # Look for API endpoints in script content
        api_patterns = [
            r'https?://[^\s"\']+/api/[^\s"\']*',
            r'https?://[^\s"\']+rankings[^\s"\']*',
            r'/wp-json/[^\s"\']*',
            r'ajax[^"\']*url[^"\']*["\']([^"\']+)["\']',
        ]
        
        potential_apis = set()
        for script in scripts:
            if script.string:
                for pattern in api_patterns:
                    matches = re.findall(pattern, script.string, re.IGNORECASE)
                    for match in matches:
                        potential_apis.add(match)
        
        if potential_apis:
            print(f"Found {len(potential_apis)} potential API endpoints:")
            for api in list(potential_apis)[:10]:  # Show first 10
                print(f"  - {api}")
        
        for i, script in enumerate(scripts):
            if script.string and ('rankings' in script.string.lower() or 'players' in script.string.lower()):
                print(f"Script {i+1} contains potential data (first 500 chars):")
                print(script.string[:500])
                print("-" * 50)
                
        # Look for div elements that might contain data
        divs_with_data = soup.find_all('div', {'id': True})
        print(f"Found {len(divs_with_data)} divs with IDs")
        
        for div in divs_with_data[:5]:  # Check first 5
            if div.get('id') and any(keyword in div.get('id').lower() for keyword in ['ranking', 'table', 'data', 'player']):
                print(f"Potential data div: {div.get('id')}")
                print(f"Content preview: {str(div)[:200]}")
        
        # Look for specific RotoBaller elements
        rotoballer_elements = soup.find_all(class_=re.compile(r'ranking|player|tier', re.I))
        print(f"Found {len(rotoballer_elements)} elements with ranking-related classes")
        
        # Look for Angular/SPA containers
        angular_containers = soup.find_all('div', {'ng-app': True}) or soup.find_all('ui-view') or soup.find_all(class_=re.compile(r'ionut', re.I))
        print(f"Found {len(angular_containers)} Angular/SPA containers")
        for container in angular_containers:
            print(f"Container: {container.name} with attributes {container.attrs}")
            if container.text.strip():
                print(f"Content preview: {container.text.strip()[:200]}")
        
        # Check if page appears to be a redirect or error page
        if len(html) < 1000:
            print("WARNING: Page content is very short - possible redirect or error page")

    def find_rankings_table(self, dfs: List[pd.DataFrame]) -> Optional[pd.DataFrame]:
        """Select the best table that looks like fantasy rankings."""
        best_table = None
        best_score = 0
        
        for i, df in enumerate(dfs):
            score = 0
            cols = [str(c).strip().lower() for c in df.columns]
            print(f"Table {i+1} columns: {cols}")
            
            # Look for key fantasy football columns
            if any('player' in col for col in cols):
                score += 3
            if any(col in ['pos', 'position'] for col in cols):
                score += 2
            if any('tier' in col for col in cols):
                score += 2
            if any(col in ['rank', '#', 'ranking'] for col in cols):
                score += 2
            if any(col in ['team', 'tm'] for col in cols):
                score += 1
            
            print(f"Table {i+1} score: {score}")
            
            if score > best_score:
                best_table = df
                best_score = score
        
        return best_table

    def normalize_player_data(self, row: pd.Series) -> Dict[str, Optional[str]]:
        """Extract and normalize player information from a table row."""
        result = {
            'name': None,
            'position': None, 
            'team': None,
            'rank': None,
            'tier': None
        }
        
        # Try to find player name in various columns
        player_col = None
        for col in ['Player', 'PLAYER', 'Name', 'Player Name']:
            if col in row.index and not pd.isna(row[col]):
                player_col = col
                break
        
        if not player_col:
            return result
            
        player_text = str(row[player_col]).strip()
        
        # Try to parse "Name Team - POS" format
        match = re.match(r"^(?P<name>.+?)\s+(?P<team>[A-Za-z]{2,4})\s*-\s*(?P<pos>[A-Za-z/]+)$", player_text)
        if match:
            result['name'] = match.group('name').strip()
            result['team'] = match.group('team').upper()
            result['position'] = match.group('pos').upper()
        else:
            result['name'] = player_text
            
            # Try separate columns for team and position
            for pos_col in ['POS', 'Pos', 'Position']:
                if pos_col in row.index and not pd.isna(row[pos_col]):
                    result['position'] = str(row[pos_col]).upper()
                    break
                    
            for team_col in ['Team', 'TM', 'Tm']:
                if team_col in row.index and not pd.isna(row[team_col]):
                    result['team'] = str(row[team_col]).upper()
                    break
        
        # Extract rank
        for rank_col in ['Rank', '#', 'Ranking']:
            if rank_col in row.index and not pd.isna(row[rank_col]):
                try:
                    result['rank'] = str(int(float(row[rank_col])))
                except (ValueError, TypeError):
                    pass
                break
        
        # Extract tier
        for tier_col in ['Tier', 'TIER']:
            if tier_col in row.index and not pd.isna(row[tier_col]):
                result['tier'] = str(row[tier_col])
                break
        
        return result

    def scrape_rankings(self, url: str, limit: Optional[int] = None) -> List[Dict[str, Optional[str]]]:
        """Scrape rankings from the given URL."""
        # Fetch the page
        html = self.fetch_html(url)
        
        # Debug page content first
        print("\n=== DEBUGGING PAGE CONTENT ===")
        self.debug_page_content(html)
        print("=== END DEBUG ===\n")
        
        # Try to find tables
        print("Trying pandas read_html...")
        dfs = self.find_tables_pandas(html)
        
        if not dfs:
            print("No tables found with pandas, trying BeautifulSoup...")
            dfs = self.find_tables_beautifulsoup(html)
        
        if not dfs:
            print("No tables found on the page!")
            return []
        
        print(f"Found {len(dfs)} total tables")
        
        # Select the best rankings table
        rankings_table = self.find_rankings_table(dfs)
        
        if rankings_table is None:
            print("Could not identify a rankings table!")
            return []
        
        print(f"Selected rankings table with shape: {rankings_table.shape}")
        print(f"Columns: {list(rankings_table.columns)}")
        
        # Extract player data
        players = []
        for idx, row in rankings_table.iterrows():
            player_data = self.normalize_player_data(row)
            
            if player_data['name']:  # Only add if we have a player name
                players.append(player_data)
                
                if limit and len(players) >= limit:
                    break
        
        return players

    def print_rankings(self, players: List[Dict[str, Optional[str]]]):
        """Print rankings to console in a formatted way."""
        if not players:
            print("No players found!")
            return
            
        print(f"\n{'Rank':<6} {'Player':<25} {'Pos':<4} {'Team':<6} {'Tier':<6}")
        print("-" * 53)
        
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
            print("No players to save!")
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
        
        print(f"Saved {len(players)} players to {filename}")


def main():
    parser = argparse.ArgumentParser(description='Analyze RotoBaller Fantasy Football Rankings Page Structure')
    parser.add_argument('--url', 
                       default='https://www.rotoballer.com/nfl-fantasy-football-rankings-tiered-ppr/265860',
                       help='RotoBaller rankings URL')
    parser.add_argument('--limit', type=int, 
                       help='Limit number of players to scrape')
    parser.add_argument('--csv', 
                       help='Save results to CSV file')
    parser.add_argument('--quiet', action='store_true',
                       help='Suppress detailed output')
    parser.add_argument('--demo', action='store_true',
                       help='Show demo data instead of scraping')
    
    args = parser.parse_args()
    
    if args.demo:
        # Show what the scraper would return with proper browser automation
        demo_players = create_demo_data()
        scraper = SimpleRotoBallerScraper()
        
        if args.csv:
            scraper.save_to_csv(demo_players, args.csv)
        
        if not args.quiet:
            scraper.print_rankings(demo_players)
            
        print(f"\n✅ Demo: Would scrape {len(demo_players)} players with browser automation")
        print("📝 See rotoballer_scraper_analysis.md for implementation details")
        return 0
    
    scraper = SimpleRotoBallerScraper()
    
    try:
        players = scraper.scrape_rankings(args.url, args.limit)
        
        if args.csv:
            scraper.save_to_csv(players, args.csv)
        
        if not args.quiet:
            scraper.print_rankings(players)
        
        if len(players) == 0:
            print("\n❌ No players scraped - RotoBaller uses JavaScript-rendered content")
            print("📋 Analysis complete - see rotoballer_scraper_analysis.md for solutions")
            print("🚀 Try --demo flag to see expected output with browser automation")
        else:
            print(f"\n✅ Successfully scraped {len(players)} players")
        
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0


def create_demo_data():
    """Create demo data showing what would be scraped with browser automation."""
    return [
        {'name': 'Christian McCaffrey', 'position': 'RB', 'team': 'SF', 'rank': '1', 'tier': '1'},
        {'name': 'CeeDee Lamb', 'position': 'WR', 'team': 'DAL', 'rank': '2', 'tier': '1'}, 
        {'name': 'Tyreek Hill', 'position': 'WR', 'team': 'MIA', 'rank': '3', 'tier': '1'},
        {'name': 'Austin Ekeler', 'position': 'RB', 'team': 'LAC', 'rank': '4', 'tier': '1'},
        {'name': 'Stefon Diggs', 'position': 'WR', 'team': 'BUF', 'rank': '5', 'tier': '1'},
        {'name': 'Josh Allen', 'position': 'QB', 'team': 'BUF', 'rank': '6', 'tier': '2'},
        {'name': 'Davante Adams', 'position': 'WR', 'team': 'LV', 'rank': '7', 'tier': '2'},
        {'name': 'Cooper Kupp', 'position': 'WR', 'team': 'LAR', 'rank': '8', 'tier': '2'},
        {'name': 'Travis Kelce', 'position': 'TE', 'team': 'KC', 'rank': '9', 'tier': '2'},
        {'name': 'Jonathan Taylor', 'position': 'RB', 'team': 'IND', 'rank': '10', 'tier': '2'},
    ]


if __name__ == '__main__':
    sys.exit(main())
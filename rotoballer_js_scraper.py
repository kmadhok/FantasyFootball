#!/usr/bin/env python3
"""
RotoBaller JavaScript-Aware Scraper

Uses requests-html to wait for JavaScript to load before scraping fantasy football rankings.
This version can handle dynamically rendered content.
"""

import argparse
import csv
import re
import sys
import time
from typing import List, Dict, Optional

import pandas as pd
from requests_html import HTMLSession
from bs4 import BeautifulSoup


class RotoBallerJSScraper:
    def __init__(self):
        self.session = HTMLSession()
        # Configure session defaults
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        
    def fetch_and_render(self, url: str, wait_time: int = 3, sleep_time: int = 2) -> str:
        """Fetch URL and wait for JavaScript to render content."""
        print(f"Fetching: {url}")
        
        # Clean up URL (remove fragment identifiers)
        if '#!/' in url:
            clean_url = url.split('#!/')[0]
            print(f"Cleaned URL: {clean_url}")
            url = clean_url
        
        try:
            # Get the page
            r = self.session.get(url)
            r.raise_for_status()
            
            print(f"Initial response length: {len(r.html.html)} characters")
            
            # Render JavaScript - wait for content to load
            print(f"Rendering JavaScript (wait={wait_time}s, sleep={sleep_time}s)...")
            r.html.render(wait=wait_time, sleep=sleep_time)
            
            print(f"After JS render length: {len(r.html.html)} characters")
            
            return r.html.html
            
        except Exception as e:
            print(f"Error fetching/rendering page: {e}")
            raise
    
    def find_rankings_in_rendered_html(self, html: str) -> List[pd.DataFrame]:
        """Extract tables from JavaScript-rendered HTML."""
        print("Looking for tables in rendered HTML...")
        
        # Try pandas first
        try:
            dfs = pd.read_html(html)
            if dfs:
                print(f"Found {len(dfs)} tables with pandas")
                return dfs
        except ValueError as e:
            print(f"Pandas read_html failed: {e}")
        
        # Try BeautifulSoup as fallback
        soup = BeautifulSoup(html, 'html.parser')
        tables = soup.find_all('table')
        print(f"Found {len(tables)} <table> elements with BeautifulSoup")
        
        dfs = []
        for i, table in enumerate(tables):
            try:
                df = pd.read_html(str(table))[0]
                dfs.append(df)
                print(f"Successfully parsed table {i+1} with shape: {df.shape}")
            except Exception as e:
                print(f"Failed to parse table {i+1}: {e}")
                continue
        
        return dfs
    
    def debug_rendered_content(self, html: str):
        """Debug what content is in the rendered HTML."""
        soup = BeautifulSoup(html, 'html.parser')
        
        print(f"\n=== DEBUGGING RENDERED CONTENT ===")
        print(f"HTML length: {len(html)} characters")
        print(f"Page title: {soup.title.string if soup.title else 'No title'}")
        
        # Look for table-like structures
        tables = soup.find_all('table')
        print(f"Found {len(tables)} <table> elements")
        
        # Look for divs that might contain player data
        player_divs = soup.find_all('div', string=re.compile(r'\b\w+\s+\w+\b.*\b(QB|RB|WR|TE)\b', re.I))
        print(f"Found {len(player_divs)} divs with potential player data")
        
        # Look for Angular content
        angular_content = soup.find_all(class_=re.compile(r'ionut|ranking', re.I))
        print(f"Found {len(angular_content)} elements with Angular/ranking classes")
        
        for i, elem in enumerate(angular_content[:3]):  # Check first 3
            if elem.get_text(strip=True):
                print(f"Angular element {i+1} text: {elem.get_text(strip=True)[:200]}")
        
        # Look for any text that looks like player names with positions
        all_text = soup.get_text()
        player_matches = re.findall(r'([A-Z][a-z]+ [A-Z][a-z]+).*?(QB|RB|WR|TE)', all_text)
        if player_matches:
            print(f"Found {len(player_matches)} potential player mentions:")
            for name, pos in player_matches[:10]:  # Show first 10
                print(f"  - {name} ({pos})")
        
        print("=== END DEBUG ===\n")
    
    def select_rankings_table(self, dfs: List[pd.DataFrame]) -> Optional[pd.DataFrame]:
        """Select the best table that looks like fantasy rankings."""
        if not dfs:
            return None
            
        best_table = None
        best_score = 0
        
        for i, df in enumerate(dfs):
            score = 0
            cols = [str(c).strip().lower() for c in df.columns]
            
            print(f"Table {i+1} columns: {cols}")
            
            # Score based on expected fantasy football columns
            if any('player' in col or 'name' in col for col in cols):
                score += 3
            if any(col in ['pos', 'position'] for col in cols):
                score += 2
            if any('tier' in col for col in cols):
                score += 2
            if any(col in ['rank', '#', 'ranking'] for col in cols):
                score += 2
            if any(col in ['team', 'tm'] for col in cols):
                score += 1
            
            # Check if table has reasonable size
            if len(df) >= 10:  # At least 10 players
                score += 1
            if len(df.columns) >= 3:  # At least 3 columns
                score += 1
            
            print(f"Table {i+1} score: {score}, shape: {df.shape}")
            
            if score > best_score:
                best_table = df
                best_score = score
        
        if best_table is not None:
            print(f"Selected table with score {best_score}")
        
        return best_table
    
    def parse_player_data(self, df: pd.DataFrame) -> List[Dict[str, Optional[str]]]:
        """Parse player data from the rankings table."""
        players = []
        
        for idx, row in df.iterrows():
            player = {
                'name': None,
                'position': None,
                'team': None,
                'rank': None,
                'tier': None
            }
            
            # Extract player name
            for col in ['Player', 'PLAYER', 'Name', 'Player Name']:
                if col in row.index and not pd.isna(row[col]):
                    name_text = str(row[col]).strip()
                    
                    # Try to parse "Name Team - POS" format
                    match = re.match(r"^(?P<name>.+?)\s+(?P<team>[A-Za-z]{2,4})\s*-\s*(?P<pos>[A-Za-z/]+)$", name_text)
                    if match:
                        player['name'] = match.group('name').strip()
                        player['team'] = match.group('team').upper()
                        player['position'] = match.group('pos').upper()
                    else:
                        player['name'] = name_text
                    break
            
            # Extract separate position column
            if not player['position']:
                for col in ['POS', 'Pos', 'Position']:
                    if col in row.index and not pd.isna(row[col]):
                        player['position'] = str(row[col]).upper()
                        break
            
            # Extract separate team column  
            if not player['team']:
                for col in ['Team', 'TM', 'Tm']:
                    if col in row.index and not pd.isna(row[col]):
                        player['team'] = str(row[col]).upper()
                        break
            
            # Extract rank
            for col in ['Rank', '#', 'Ranking']:
                if col in row.index and not pd.isna(row[col]):
                    try:
                        player['rank'] = str(int(float(row[col])))
                    except (ValueError, TypeError):
                        pass
                    break
            
            # Extract tier
            for col in ['Tier', 'TIER']:
                if col in row.index and not pd.isna(row[col]):
                    player['tier'] = str(row[col])
                    break
            
            # Only add if we have a name and it looks like a real player
            if player['name'] and len(player['name']) > 2 and not player['name'].isdigit():
                players.append(player)
        
        return players
    
    def scrape_rankings(self, url: str, limit: Optional[int] = None, 
                       wait_time: int = 3, sleep_time: int = 2) -> List[Dict[str, Optional[str]]]:
        """Scrape rankings from RotoBaller with JavaScript rendering."""
        
        # Fetch and render the page
        html = self.fetch_and_render(url, wait_time, sleep_time)
        
        # Debug what we got
        self.debug_rendered_content(html)
        
        # Look for tables in rendered content
        dfs = self.find_rankings_in_rendered_html(html)
        
        if not dfs:
            print("No tables found in rendered HTML!")
            return []
        
        # Select the best rankings table
        table = self.select_rankings_table(dfs)
        
        if table is None:
            print("Could not identify a rankings table!")
            return []
        
        # Parse player data
        players = self.parse_player_data(table)
        
        # Apply limit
        if limit and len(players) > limit:
            players = players[:limit]
        
        return players
    
    def print_rankings(self, players: List[Dict[str, Optional[str]]]):
        """Print rankings in a formatted table."""
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
    parser = argparse.ArgumentParser(description='Scrape RotoBaller Fantasy Football Rankings with JavaScript Support')
    parser.add_argument('--url', 
                       default='https://www.rotoballer.com/nfl-fantasy-football-rankings-tiered-ppr/265860',
                       help='RotoBaller rankings URL')
    parser.add_argument('--limit', type=int, default=50,
                       help='Limit number of players to scrape (default: 50)')
    parser.add_argument('--csv', 
                       help='Save results to CSV file')
    parser.add_argument('--wait', type=int, default=3,
                       help='Seconds to wait before rendering JavaScript (default: 3)')
    parser.add_argument('--sleep', type=int, default=2,
                       help='Seconds to sleep after rendering JavaScript (default: 2)')
    parser.add_argument('--quiet', action='store_true',
                       help='Suppress detailed output')
    
    args = parser.parse_args()
    
    scraper = RotoBallerJSScraper()
    
    try:
        print(f"🚀 Starting JavaScript-aware scraping...")
        start_time = time.time()
        
        players = scraper.scrape_rankings(
            args.url, 
            limit=args.limit,
            wait_time=args.wait,
            sleep_time=args.sleep
        )
        
        elapsed = time.time() - start_time
        
        if args.csv:
            scraper.save_to_csv(players, args.csv)
        
        if not args.quiet:
            scraper.print_rankings(players)
        
        print(f"\n✅ Successfully scraped {len(players)} players in {elapsed:.1f} seconds")
        
        if len(players) == 0:
            print("💡 Try increasing --wait and --sleep times if no data was found")
            print("💡 The page might need more time for JavaScript to load rankings")
        
        return 0
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    
    finally:
        # Clean up the session
        if hasattr(scraper.session, 'browser') and scraper.session.browser:
            scraper.session.browser.close()


if __name__ == '__main__':
    sys.exit(main())
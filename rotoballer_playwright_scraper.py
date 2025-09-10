#!/usr/bin/env python3
"""
RotoBaller Playwright Scraper

Based on successful Google Colab implementation. Uses Playwright async API to handle
JavaScript-rendered content on RotoBaller fantasy football rankings.
"""

import argparse
import asyncio
import csv
import re
import sys
import time
from datetime import datetime
from io import StringIO
from typing import List, Dict, Optional

import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError


class RotoBallerPlaywrightScraper:
    def __init__(self, headless: bool = True, timeout: int = 90_000):
        self.headless = headless
        self.timeout = timeout
        self.user_agent = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        )
    
    async def get_largest_table_html(self, page) -> str:
        """Extract the largest table from the page after waiting for content."""
        print("⏳ Waiting for tables to load...")
        
        # Wait for any table; use generous timeout
        await page.wait_for_selector("table", timeout=self.timeout)
        
        print("🔍 Looking for consent buttons...")
        # Best-effort accept common consent buttons
        for text in ["Accept", "I Agree", "Agree", "Got it", "OK", "Accept All"]:
            try:
                await page.get_by_role("button", name=text, exact=False).first.click(timeout=1500)
                print(f"✅ Clicked consent button: {text}")
                break
            except Exception:
                pass
        
        # Count available tables
        tables = page.locator("table")
        count = await tables.count()
        print(f"📊 Found {count} tables on page")
        
        if count == 0:
            raise RuntimeError("No tables found on the page.")
        
        # Find the largest table by row × column count
        best_html, best_score, best_index = None, -1, -1
        
        for i in range(count):
            table_locator = tables.nth(i)
            html = await table_locator.evaluate("el => el.outerHTML")
            
            try:
                dfs = pd.read_html(StringIO(html))
                if not dfs:
                    continue
                    
                # Get the largest DataFrame from this table
                df = max(dfs, key=lambda d: d.shape[0] * d.shape[1])
                score = df.shape[0] * df.shape[1]
                
                print(f"   Table {i+1}: {df.shape[0]} rows × {df.shape[1]} cols = score {score}")
                
                if score > best_score:
                    best_score = score
                    best_html = html
                    best_index = i
                    
            except ValueError as e:
                print(f"   Table {i+1}: Could not parse - {e}")
                continue
        
        if not best_html:
            raise RuntimeError("Couldn't parse any table with pandas.")
            
        print(f"🏆 Selected table {best_index+1} with score {best_score}")
        return best_html
    
    async def navigate_and_wait(self, page, url: str):
        """Navigate to URL and wait for content to load."""
        print(f"🌐 Navigating to: {url}")
        
        # Use domcontentloaded instead of networkidle (more reliable for SPAs)
        await page.goto(url, wait_until="domcontentloaded", timeout=self.timeout)
        
        print("⏸️ Waiting for SPA to initialize...")
        # SPAs need time to populate after DOMContentLoaded
        await page.wait_for_timeout(1500)
        
        print("🎯 Waiting for meaningful content...")
        # Wait until the page has a decent number of table rows
        try:
            await page.wait_for_function(
                """() => {
                    const tables = document.querySelectorAll('table');
                    for (let table of tables) {
                        const rows = table.querySelectorAll('tr').length;
                        if (rows > 25) return true;  // Found a substantial table
                    }
                    return false;
                }""",
                timeout=60_000
            )
            print("✅ Found substantial table content")
        except PWTimeoutError:
            print("⚠️ Timeout waiting for large table - proceeding with available content")
    
    async def scrape_url(self, url: str, max_retries: int = 3) -> pd.DataFrame:
        """Main scraping function with retry logic."""
        async with async_playwright() as p:
            print(f"🚀 Starting Playwright browser (headless={self.headless})...")
            
            browser = await p.chromium.launch(headless=self.headless)
            context = await browser.new_context(
                user_agent=self.user_agent,
                locale="en-US",
                timezone_id="America/New_York",
                viewport={"width": 1400, "height": 900}
            )
            page = await context.new_page()
            
            # Retry logic for flaky loads
            last_error = None
            for attempt in range(max_retries):
                try:
                    print(f"📡 Attempt {attempt + 1}/{max_retries}")
                    
                    await self.navigate_and_wait(page, url)
                    html = await self.get_largest_table_html(page)
                    
                    # Parse the HTML into DataFrame
                    dfs = pd.read_html(StringIO(html))
                    df = max(dfs, key=lambda d: d.shape[0] * d.shape[1])
                    
                    print(f"📊 Successfully extracted table: {df.shape[0]} rows × {df.shape[1]} columns")
                    break
                    
                except Exception as e:
                    print(f"❌ Attempt {attempt + 1} failed: {e}")
                    last_error = e
                    if attempt < max_retries - 1:
                        wait_time = 1 + attempt
                        print(f"⏳ Waiting {wait_time}s before retry...")
                        await asyncio.sleep(wait_time)
            else:
                await browser.close()
                raise last_error
            
            await browser.close()
            return df
    
    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean up the DataFrame columns and structure."""
        # Handle multi-level columns
        if hasattr(df.columns, 'levels'):  # MultiIndex columns
            print("🔧 Flattening multi-level column headers...")
            df.columns = [' '.join(col).strip() if isinstance(col, tuple) else str(col).strip() 
                         for col in df.columns.values]
        else:
            df.columns = [str(c).strip() for c in df.columns]
        
        print(f"📋 Columns after cleaning: {list(df.columns)}")
        return df
    
    def extract_players(self, df: pd.DataFrame) -> List[Dict[str, Optional[str]]]:
        """Extract player data from the cleaned DataFrame."""
        print(f"👤 Extracting player data from {len(df)} rows...")
        
        players = []
        
        # Find column indices for key data
        player_col = self.find_column_by_patterns(df.columns, [
            r'.*player.*name.*', r'.*name.*', r'.*player.*'
        ])
        pos_col = self.find_column_by_patterns(df.columns, [
            r'.*pos.*', r'.*position.*'
        ])
        rank_col = self.find_column_by_patterns(df.columns, [
            r'.*rank.*', r'.*#.*', r'.*rk.*'
        ])
        
        print(f"📍 Found columns - Player: {player_col}, Position: {pos_col}, Rank: {rank_col}")
        
        for idx, row in df.iterrows():
            player_data = {
                'name': None,
                'position': None,
                'team': None,
                'rank': None,
                'tier': None
            }
            
            # Extract player name
            if player_col and not pd.isna(row[player_col]):
                name = str(row[player_col]).strip()
                name = self.clean_player_name(name)
                
                # Skip tier headers and non-player entries
                if self.is_valid_player_name(name):
                    player_data['name'] = name
            
            # Extract position
            if pos_col and not pd.isna(row[pos_col]):
                pos = str(row[pos_col]).upper().strip()
                if pos in ['QB', 'RB', 'WR', 'TE', 'K', 'DST', 'DEF']:
                    player_data['position'] = pos
            
            # Extract rank
            if rank_col and not pd.isna(row[rank_col]):
                try:
                    rank_value = int(float(row[rank_col]))
                    if 1 <= rank_value <= 1000:  # Reasonable range
                        player_data['rank'] = str(rank_value)
                except (ValueError, TypeError):
                    pass
            
            # Only include valid players
            if player_data['name'] and player_data['position']:
                players.append(player_data)
        
        print(f"✅ Extracted {len(players)} valid players")
        return players
    
    def find_column_by_patterns(self, columns: List[str], patterns: List[str]) -> Optional[str]:
        """Find the first column matching any of the given regex patterns."""
        for pattern in patterns:
            for col in columns:
                if re.search(pattern, col, re.IGNORECASE):
                    return col
        return None
    
    def clean_player_name(self, name: str) -> str:
        """Clean up player name (remove duplicates, extra spaces, etc.)."""
        # Handle duplicated names like "Saquon Barkley Saquon Ba"
        words = name.split()
        if len(words) >= 2:
            first_name = words[0]
            # Check for duplication
            if len(words) > 2 and first_name in ' '.join(words[1:]):
                for i in range(1, len(words)):
                    if words[i] == first_name:
                        name = ' '.join(words[:i])
                        break
        
        return re.sub(r'\s+', ' ', name).strip()
    
    def is_valid_player_name(self, name: str) -> bool:
        """Check if the name looks like a valid player name."""
        if not name or len(name) < 3:
            return False
            
        # Skip tier headers and common non-player entries
        skip_patterns = [
            r'tier\s*\d+', r'premium', r'betting', r'lineup', r'industry', 
            r'weekly', r'rankings', r'projections', r'stats', r'news',
            r'waiver', r'trade', r'start.*sit', r'^[0-9\s\-]+$'
        ]
        
        for pattern in skip_patterns:
            if re.search(pattern, name, re.IGNORECASE):
                return False
        
        # Should look like a person's name (at least 2 words, starts with capital)
        words = name.split()
        return (len(words) >= 2 and 
                words[0][0].isupper() and 
                all(word.replace("'", "").replace("-", "").isalpha() for word in words[:2]))
    
    def print_rankings(self, players: List[Dict[str, Optional[str]]]):
        """Print player rankings in a formatted table."""
        if not players:
            print("❌ No players found!")
            return
        
        print(f"\n🏈 ROTOBALLER FANTASY FOOTBALL RANKINGS ({len(players)} players)")
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


async def main():
    parser = argparse.ArgumentParser(
        description='Scrape RotoBaller Fantasy Football Rankings using Playwright',
        epilog='Uses the successful Google Colab approach with async Playwright.'
    )
    parser.add_argument('--url', 
                       default='https://www.rotoballer.com/nfl-fantasy-football-rankings-tiered-ppr/265860#!/rankings?spreadsheet=ppr&league=Overall',
                       help='RotoBaller rankings URL (includes fragment)')
    parser.add_argument('--limit', type=int,
                       help='Limit number of players to display/save')
    parser.add_argument('--timeout', type=int, default=90,
                       help='Timeout in seconds for page operations (default: 90)')
    parser.add_argument('--retries', type=int, default=3,
                       help='Number of retry attempts (default: 3)')
    parser.add_argument('--csv', 
                       help='Save results to CSV file')
    parser.add_argument('--show-browser', action='store_true',
                       help='Show browser window (default: headless)')
    parser.add_argument('--quiet', action='store_true',
                       help='Suppress detailed output')
    
    args = parser.parse_args()
    
    scraper = RotoBallerPlaywrightScraper(
        headless=not args.show_browser,
        timeout=args.timeout * 1000  # Convert to milliseconds
    )
    
    try:
        print("🚀 Starting Playwright-powered RotoBaller scraping...")
        start_time = time.time()
        
        # Scrape the data
        df = await scraper.scrape_url(args.url, args.retries)
        df = scraper.clean_dataframe(df)
        players = scraper.extract_players(df)
        
        # Apply limit
        if args.limit and len(players) > args.limit:
            players = players[:args.limit]
            print(f"🔄 Limited results to {args.limit} players")
        
        elapsed = time.time() - start_time
        
        # Save to CSV if requested
        if args.csv:
            scraper.save_to_csv(players, args.csv)
        
        # Display results unless quiet
        if not args.quiet:
            scraper.print_rankings(players)
        
        print(f"\n🎉 Successfully scraped {len(players)} players in {elapsed:.1f} seconds")
        
        if len(players) == 0:
            print("💡 Try increasing --timeout or check if the page structure changed")
            print("💡 Use --show-browser to debug what's happening")
        
        return 0
        
    except KeyboardInterrupt:
        print("\n⏹️ Scraping interrupted by user")
        return 1
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))
#!/usr/bin/env python3
"""
NFL Next Gen Stats Rushing Scraper with Auto Header Fix

This script scrapes NFL Next Gen Stats rushing data and automatically fixes CSV headers
if they are numeric (0,1,2,3...) by replacing them with proper rushing stat names.

Usage:
    python tests/nfl_stats_scraper_rushing.py
    python tests/nfl_stats_scraper_rushing.py --year 2025 --week 2
"""

import asyncio
import csv
import argparse
from pathlib import Path
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
import pandas as pd

def ngs_rushing_url(year=2025, phase="REG", week=1, sort_hash="yards"):
    """Generate NFL Next Gen Stats URL"""
    return f"https://nextgenstats.nfl.com/stats/rushing/{year}/{phase}/{week}#{sort_hash}"

def read_headers(headers_file):
    """Read proper column headers from headers file"""
    try:
        with open(headers_file, 'r') as f:
            reader = csv.reader(f)
            headers = next(reader)
            return headers
    except FileNotFoundError:
        print(f"Warning: Headers file '{headers_file}' not found")
        return None
    except Exception as e:
        print(f"Warning: Error reading headers file: {e}")
        return None

def fix_csv_headers(csv_file, headers):
    """Replace numeric headers with proper stat names"""
    if not headers:
        print(f"Skipping header fix for '{csv_file}' - no headers available")
        return True
        
    try:
        # Read the CSV file
        with open(csv_file, 'r') as f:
            lines = f.readlines()
        
        if not lines:
            print(f"Error: CSV file '{csv_file}' is empty")
            return False
        
        # Split first line to check current headers
        first_line = lines[0].strip()
        current_headers = first_line.split(',')
        
        # Check if headers are numeric (0,1,2,3...)
        try:
            # If all headers can be converted to integers, they're numeric
            [int(h) for h in current_headers]
            is_numeric = True
        except ValueError:
            is_numeric = False
        
        if not is_numeric:
            print(f"✓ CSV file '{csv_file}' already has proper headers")
            return True
        
        # Ensure we have the right number of headers
        if len(current_headers) != len(headers):
            print(f"Warning: Header count mismatch - CSV has {len(current_headers)}, headers file has {len(headers)}")
            print(f"Using first {min(len(current_headers), len(headers))} headers")
            headers = headers[:len(current_headers)]
        
        # Replace first line with proper headers
        new_first_line = ','.join(headers) + '\n'
        lines[0] = new_first_line
        
        # Write back to file
        with open(csv_file, 'w') as f:
            f.writelines(lines)
        
        print(f"✓ Fixed headers in '{csv_file}'")
        print(f"  Old: {current_headers[:5]}...")
        print(f"  New: {headers[:5]}...")
        return True
        
    except Exception as e:
        print(f"Error fixing CSV file '{csv_file}': {e}")
        return False

async def fetch_ngs_rushing_table(year=2025, week=1, phase="REG", sort_hash="yards"):
    """Fetch NFL Next Gen Stats rushing table"""
    browser_cfg = BrowserConfig(
        browser_type="chromium",
        headless=True,
        java_script_enabled=True,
        # enable_stealth=True,  # uncomment if you hit bot checks
        # user_agent_mode="random",
    )

    warm_cfg = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        page_timeout=60_000,
        wait_until="domcontentloaded",
        wait_for="js:() => !!document.body",
        js_code=(
            "() => {"
            "  const sel=['#onetrust-accept-btn-handler','button#onetrust-accept-btn-handler',"
            "             'button[aria-label=\"I Accept\"]','#truste-consent-button'];"
            "  for (const s of sel){const b=document.querySelector(s); if(b){b.click(); break;}}"
            "}"
        ),
    )

    run_cfg = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        page_timeout=120_000,
        wait_until="domcontentloaded",
        wait_for=(
            f"js:() => "
            f"location.pathname.endsWith('/stats/rushing/{year}/{phase}/{week}') && "
            f"!!document.querySelector('table')"
        ),
        css_selector="table",
        scan_full_page=True,
        word_count_threshold=1,
    )

    async with AsyncWebCrawler(config=browser_cfg) as crawler:
        await crawler.arun(url="https://nextgenstats.nfl.com/", config=warm_cfg)
        res = await crawler.arun(url=ngs_rushing_url(year, phase, week, sort_hash), config=run_cfg)
        if not res.success:
            raise RuntimeError(res.error_message)

        dfs = pd.read_html(res.cleaned_html)  # requires lxml
        if not dfs:
            raise ValueError("No tables found after render.")
        df = max(dfs, key=lambda d: (len(d.index) * max(1, len(d.columns))))
        df.columns = [str(c).strip() for c in df.columns]
        df = df.loc[:, ~df.columns.astype(str).str.match(r"^Unnamed")]
        return df

def main():
    """Main function to scrape NFL stats and fix headers"""
    parser = argparse.ArgumentParser(description='Scrape NFL Next Gen Stats rushing data with header fixing')
    parser.add_argument('--year', type=int, default=2025, help='NFL season year (default: 2025)')
    parser.add_argument('--week', type=int, default=1, help='Week number (default: 1)')
    parser.add_argument('--phase', default='REG', help='Season phase (default: REG)')
    parser.add_argument('--sort', default='yards', help='Sort metric (default: yards)')
    parser.add_argument('--output', help='Output CSV filename (default: auto-generated)')
    
    args = parser.parse_args()
    
    # Generate output filename if not provided
    if args.output:
        output_file = Path(args.output)
    else:
        script_dir = Path(__file__).parent
        output_file = script_dir / f"ngs_rushing_{args.year}_wk{args.week}_{args.sort}.csv"
    
    headers_file = Path(__file__).parent / "rushing_headers.csv"
    
    print("=" * 80)
    print("NFL NEXT GEN STATS RUSHING SCRAPER")
    print("=" * 80)
    print(f"Year: {args.year}")
    print(f"Week: {args.week}")
    print(f"Phase: {args.phase}")
    print(f"Sort by: {args.sort}")
    print(f"Output: {output_file}")
    print(f"Headers file: {headers_file}")
    print("=" * 80)
    
    try:
        # Scrape the data
        print(f"\n🏈 Scraping NFL Next Gen Stats rushing data for {args.year} Week {args.week}...")
        df = asyncio.run(fetch_ngs_rushing_table(
            year=args.year, 
            week=args.week, 
            phase=args.phase, 
            sort_hash=args.sort
        ))
        
        print(f"✓ Successfully scraped {len(df)} player records")
        print(f"✓ Found {len(df.columns)} columns")
        
        # Display sample data
        print(f"\n📊 Sample data (first 5 players):")
        print(df.head())
        
        # Save to CSV
        print(f"\n💾 Saving data to {output_file}...")
        df.to_csv(output_file, index=False)
        print(f"✓ Data saved successfully")
        
        # Fix headers if needed
        print(f"\n🔧 Checking and fixing headers...")
        headers = read_headers(headers_file)
        
        if fix_csv_headers(output_file, headers):
            print("✓ Headers processed successfully")
        else:
            print("⚠️  Header fixing had issues, but data is still saved")
        
        # Show final result
        print(f"\n📋 Final CSV structure:")
        final_df = pd.read_csv(output_file)
        print(f"  Columns: {list(final_df.columns)}")
        print(f"  Records: {len(final_df)}")
        
        print(f"\n✅ NFL Rushing Stats scraping completed successfully!")
        print(f"📁 Output file: {output_file}")
        
    except Exception as e:
        print(f"\n❌ Error during scraping: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
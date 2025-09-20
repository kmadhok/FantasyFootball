#!/usr/bin/env python3
"""
Fix CSV Headers Script

This script replaces numeric column headers (0,1,2,3...) with proper stat names
from a headers file for NFL Next Gen Stats data.

Usage:
    python tests/fix_csv_headers.py
"""

import csv
import os
import sys
import argparse
import re
from pathlib import Path

def read_headers(headers_file):
    """Read proper column headers from headers file"""
    try:
        with open(headers_file, 'r') as f:
            reader = csv.reader(f)
            headers = next(reader)
            return headers
    except FileNotFoundError:
        print(f"Error: Headers file '{headers_file}' not found")
        return None
    except Exception as e:
        print(f"Error reading headers file: {e}")
        return None

def fix_csv_headers(csv_file, headers):
    """Replace numeric headers with proper stat names"""
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
            print(f"CSV file '{csv_file}' already has proper headers")
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

def detect_stat_type_from_filename(filename):
    """Detect stat type from filename pattern like ngs_{stat_type}_week{N}_{year}.csv"""
    pattern = r'ngs_(rushing|receiving|passing)_week\d+_\d+\.csv'
    match = re.search(pattern, str(filename))
    if match:
        return match.group(1)
    return None

def get_headers_file(stat_type, script_dir):
    """Get the appropriate headers file for the stat type"""
    headers_file = script_dir / f"{stat_type}_headers.csv"
    if headers_file.exists():
        return headers_file
    return None

def main():
    """Main function to fix CSV headers"""
    parser = argparse.ArgumentParser(description="Fix CSV headers with proper stat names")
    parser.add_argument("--input-file", help="CSV file to process (default: scrape_test.csv)")
    parser.add_argument("--stat-type", choices=["rushing", "receiving", "passing"], 
                        help="Type of stats (auto-detected from filename if not specified)")
    
    args = parser.parse_args()
    
    # Define file paths
    script_dir = Path(__file__).parent
    
    # Determine input file
    if args.input_file:
        csv_file = Path(args.input_file)
        if not csv_file.is_absolute():
            csv_file = script_dir / csv_file
    else:
        csv_file = script_dir / "scrape_test.csv"
    
    # Determine stat type
    stat_type = args.stat_type
    if not stat_type:
        stat_type = detect_stat_type_from_filename(csv_file.name)
        if not stat_type:
            print("Error: Could not detect stat type from filename. Please specify --stat-type")
            print("Filename should match pattern: ngs_{stat_type}_week{N}_{year}.csv")
            return 1
    
    # Get headers file
    headers_file = get_headers_file(stat_type, script_dir)
    if not headers_file:
        print(f"Error: Headers file '{stat_type}_headers.csv' not found")
        return 1
    
    csv_files = [csv_file]
    
    print("=== CSV Header Fix Tool ===\n")
    print(f"Input file: {csv_file}")
    print(f"Stat type: {stat_type}")
    
    # Read headers
    print(f"Reading headers from: {headers_file}")
    headers = read_headers(headers_file)
    
    if not headers:
        print("Failed to read headers file")
        return 1
    
    print(f"✓ Found {len(headers)} headers: {headers[:5]}...\n")
    
    # Fix each CSV file
    success_count = 0
    for csv_file in csv_files:
        if csv_file.exists():
            print(f"Processing: {csv_file}")
            if fix_csv_headers(csv_file, headers):
                success_count += 1
        else:
            print(f"Warning: CSV file '{csv_file}' not found")
    
    print(f"\n=== Summary ===")
    print(f"Files processed successfully: {success_count}/{len(csv_files)}")
    
    return 0 if success_count == len(csv_files) else 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
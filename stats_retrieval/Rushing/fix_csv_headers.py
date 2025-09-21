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

def main():
    """Main function to fix CSV headers"""
    import argparse
    parser = argparse.ArgumentParser(description="Fix CSV headers for NFL rushing stats")
    parser.add_argument("--input", default="scrape_test.csv", help="Input CSV filename to fix")
    args = parser.parse_args()
    
    # Define file paths
    script_dir = Path(__file__).parent
    headers_file = script_dir / "rushing_headers.csv"
    csv_files = [
        script_dir / args.input
    ]
    
    print("=== CSV Header Fix Tool ===\n")
    
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
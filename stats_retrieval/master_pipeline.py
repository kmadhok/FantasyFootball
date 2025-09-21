#!/usr/bin/env python3
"""
Master Pipeline Script

This script orchestrates all three NFL stats pipelines (Passing, Rushing, Receiving)
for multiple weeks. Simply edit the WEEKS list below to configure which weeks to process.

Usage:
    python master_pipeline.py
"""

import sys
import subprocess
from pathlib import Path

# =============================================================================
# CONFIGURATION - Edit the weeks list as needed
# =============================================================================
WEEKS = [1, 2, 3, 4, 5]  # List of weeks to process
YEAR = 2025              # Season year
PHASE = "REG"            # Season phase (REG, PRE, POST)  
SORT_HASH = "yards"      # Sort parameter
UPLOAD_TO_BIGQUERY = False  # Set to True to automatically upload to BigQuery after CSV generation

def is_csv_valid(csv_path):
    """
    Check if a CSV file exists and has valid data (at least 3 lines: header + 2 data rows)
    
    Args:
        csv_path: Path object to the CSV file
        
    Returns:
        bool: True if file exists and has at least 3 lines, False otherwise
    """
    try:
        if not csv_path.exists():
            return False
            
        with open(csv_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()
            # Need at least 3 lines: header + 2 data rows
            return len(lines) >= 3
            
    except Exception:
        # If we can't read the file, consider it invalid
        return False

def run_pipeline(pipeline_folder, stat_type, week, year=YEAR, phase=PHASE, sort_hash=SORT_HASH):
    """
    Run a specific pipeline with given parameters
    
    Args:
        pipeline_folder: Directory name (e.g., "Passing", "Rushing", "Recieving")
        stat_type: Stat type for filename (e.g., "passing", "rushing", "receiving")
        week: Week number to process
        year: Season year
        phase: Season phase
        sort_hash: Sort parameter
        
    Returns:
        bool: True if successful, False if failed
    """
    # Set up paths
    script_dir = Path(__file__).parent
    data_dir = script_dir / "data"
    pipeline_dir = script_dir / pipeline_folder
    pipeline_script = pipeline_dir / f"{stat_type}_pipeline.py"
    
    # Ensure data directory exists
    data_dir.mkdir(exist_ok=True)
    
    # Check if pipeline script exists
    if not pipeline_script.exists():
        print(f"❌ Pipeline script not found: {pipeline_script}")
        return False
    
    # Generate output filename in data directory
    output_file = data_dir / f"ngs_{stat_type}_{year}_wk{week}.csv"
    
    # Check if valid data already exists
    if is_csv_valid(output_file):
        print(f"⏭️ Skipping {pipeline_folder} week {week} - valid data already exists")
        print(f"   📄 File: {output_file.name}")
        return True
    
    # Build command
    cmd = [
        sys.executable, 
        pipeline_script.name,  # Use relative name since we're changing directory
        "--year", str(year),
        "--week", str(week), 
        "--phase", phase,
        "--sort_hash", sort_hash,
        "--output", f"../data/{output_file.name}"  # Relative path to data directory
    ]
    
    try:
        print(f"▶ Running {pipeline_folder} pipeline for week {week}...")
        result = subprocess.run(
            cmd, 
            cwd=pipeline_dir,  # Run from pipeline directory for header files
            capture_output=True, 
            text=True,
            check=False  # Don't raise exception on non-zero exit
        )
        
        if result.returncode == 0:
            print(f"✅ {pipeline_folder} week {week} completed successfully")
            print(f"   📄 Output: {output_file.name}")
            return True
        else:
            print(f"❌ {pipeline_folder} week {week} failed (exit code {result.returncode})")
            if result.stderr:
                # Print first few lines of error
                error_lines = result.stderr.strip().split('\n')[:3]
                for line in error_lines:
                    print(f"   {line}")
            return False
            
    except Exception as e:
        print(f"❌ Error running {pipeline_folder} week {week}: {e}")
        return False

def main():
    """Run all pipelines for all configured weeks"""
    # Define pipeline configurations
    pipelines = [
        ("Passing", "passing"),
        ("Rushing", "rushing"), 
        ("Recieving", "receiving")  # Note: folder is "Recieving", script is "receiving"
    ]
    
    total_tasks = len(WEEKS) * len(pipelines)
    completed_tasks = 0
    failed_tasks = 0
    
    print("🚀 Master NFL Stats Pipeline")
    print("=" * 50)
    print(f"📅 Weeks to process: {WEEKS}")
    print(f"📊 Total tasks: {total_tasks} ({len(pipelines)} pipelines × {len(WEEKS)} weeks)")
    print(f"📁 Output directory: {Path(__file__).parent}")
    print()
    
    # Process each week
    for week_idx, week in enumerate(WEEKS, 1):
        print(f"📅 Processing Week {week} ({week_idx}/{len(WEEKS)})")
        print("-" * 40)
        
        week_successes = 0
        
        # Run each pipeline for this week
        for pipeline_folder, stat_type in pipelines:
            if run_pipeline(pipeline_folder, stat_type, week):
                completed_tasks += 1
                week_successes += 1
            else:
                failed_tasks += 1
            print()  # Blank line between pipelines
        
        print(f"Week {week} summary: {week_successes}/{len(pipelines)} pipelines succeeded")
        print()
    
    # Final summary
    print("=" * 50)
    print(f"🏁 Master pipeline completed!")
    print(f"✅ Successful: {completed_tasks}/{total_tasks}")
    
    if failed_tasks > 0:
        print(f"❌ Failed: {failed_tasks}/{total_tasks}")
        print("⚠️  Some tasks failed. Check the output above for details.")
        return 1
    else:
        print("🎉 All tasks completed successfully!")
        
        # List generated files
        script_dir = Path(__file__).parent
        data_dir = script_dir / "data"
        generated_files = []
        for week in WEEKS:
            for _, stat_type in pipelines:
                output_file = data_dir / f"ngs_{stat_type}_{YEAR}_wk{week}.csv"
                if output_file.exists():
                    generated_files.append(output_file.name)
        
        if generated_files:
            print(f"\n📄 Generated files ({len(generated_files)}):")
            for filename in sorted(generated_files):
                print(f"   {filename}")
        
        # Optional BigQuery upload
        if UPLOAD_TO_BIGQUERY and generated_files:
            print("\n" + "=" * 50)
            print("🚀 Starting BigQuery Upload...")
            try:
                import subprocess
                result = subprocess.run([
                    sys.executable, 
                    "bigquery_upload.py"
                ], capture_output=True, text=True, cwd=script_dir)
                
                if result.returncode == 0:
                    print("✅ BigQuery upload completed successfully!")
                    print(result.stdout)
                else:
                    print("❌ BigQuery upload failed!")
                    print(result.stderr)
                    
            except Exception as e:
                print(f"❌ Error running BigQuery upload: {e}")
                print("💡 To upload manually, run: python bigquery_upload.py")
        
        return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
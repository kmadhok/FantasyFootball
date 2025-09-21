#!/usr/bin/env python3
"""
BigQuery Upload Script for NFL Stats

This script uploads CSV files from the data/ folder to Google BigQuery.
All columns are treated as strings for simplicity.

Usage:
    python bigquery_upload.py
    python bigquery_upload.py --dataset custom_dataset_name
"""

import os
import sys
import csv
import argparse
from pathlib import Path
from typing import List, Dict, Optional
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from dotenv import load_dotenv
load_dotenv()


# Configuration
DEFAULT_DATASET = "nfl_stats_2025"
PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID","brainrot-453319")
print(f"PROJECT_ID: {PROJECT_ID}")
CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# Table mapping for different stat types
TABLE_MAPPING = {
    "passing": "ngs_passing_weekly",
    "rushing": "ngs_rushing_weekly", 
    "receiving": "ngs_receiving_weekly"
}

class BigQueryUploader:
    def __init__(self, project_id: str, dataset_name: str = DEFAULT_DATASET):
        """Initialize BigQuery client and dataset"""
        self.project_id = project_id
        self.dataset_name = dataset_name
        self.client = bigquery.Client(project=project_id)
        self.dataset_ref = self.client.dataset(dataset_name)
        
        # Ensure dataset exists
        self._ensure_dataset_exists()
    
    def _ensure_dataset_exists(self):
        """Create dataset if it doesn't exist"""
        try:
            self.client.get_dataset(self.dataset_ref)
            print(f"✅ Dataset '{self.dataset_name}' already exists")
        except NotFound:
            print(f"📂 Creating dataset '{self.dataset_name}'...")
            dataset = bigquery.Dataset(self.dataset_ref)
            dataset.location = "US"  # or your preferred location
            self.client.create_dataset(dataset)
            print(f"✅ Dataset '{self.dataset_name}' created successfully")
    
    def _get_csv_schema(self, csv_file_path: Path) -> List[bigquery.SchemaField]:
        """Generate BigQuery schema from CSV headers (all strings)"""
        with open(csv_file_path, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            headers = next(reader)
        
        # Create schema with all string fields + metadata fields
        schema = []
        for header in headers:
            # Clean header name for BigQuery (replace special chars)
            clean_header = header.replace('%', '_PCT').replace('+', '_PLUS').replace('-', '_MINUS').replace('/', '_PER')
            schema.append(bigquery.SchemaField(clean_header, "STRING"))
        
        # Add metadata fields
        schema.extend([
            bigquery.SchemaField("week", "STRING"),
            bigquery.SchemaField("year", "STRING"),
            bigquery.SchemaField("stat_type", "STRING"),
            bigquery.SchemaField("upload_timestamp", "TIMESTAMP")
        ])
        
        return schema
    
    def _extract_metadata_from_filename(self, filename: str) -> Dict[str, str]:
        """Extract week, year, and stat_type from filename"""
        # Expected format: ngs_{stat_type}_{year}_wk{week}.csv
        parts = filename.replace('.csv', '').split('_')
        
        try:
            stat_type = parts[1]  # passing, rushing, receiving
            year = parts[2]       # 2025
            week = parts[3].replace('wk', '')  # wk4 -> 4
            
            return {
                "stat_type": stat_type,
                "year": year,
                "week": week
            }
        except (IndexError, ValueError) as e:
            raise ValueError(f"Invalid filename format: {filename}. Expected: ngs_{{stat_type}}_{{year}}_wk{{week}}.csv")
    
    def _table_exists(self, table_name: str) -> bool:
        """Check if table exists in the dataset"""
        try:
            table_ref = self.dataset_ref.table(table_name)
            self.client.get_table(table_ref)
            return True
        except NotFound:
            return False
    
    def _check_data_exists(self, table_name: str, week: str, year: str, stat_type: str) -> bool:
        """Check if data for this week/year/stat_type already exists"""
        if not self._table_exists(table_name):
            return False
        
        query = f"""
        SELECT COUNT(*) as count
        FROM `{self.project_id}.{self.dataset_name}.{table_name}`
        WHERE week = '{week}' AND year = '{year}' AND stat_type = '{stat_type}'
        """
        
        try:
            result = self.client.query(query).result()
            count = next(iter(result)).count
            return count > 0
        except Exception:
            return False
    
    def upload_csv(self, csv_file_path: Path) -> bool:
        """Upload a single CSV file to BigQuery"""
        try:
            # Extract metadata from filename
            metadata = self._extract_metadata_from_filename(csv_file_path.name)
            stat_type = metadata["stat_type"]
            
            # Get target table name
            if stat_type not in TABLE_MAPPING:
                print(f"❌ Unknown stat type: {stat_type}")
                return False
            
            table_name = TABLE_MAPPING[stat_type]
            table_ref = self.dataset_ref.table(table_name)
            
            # Check if data already exists
            if self._check_data_exists(table_name, metadata["week"], metadata["year"], stat_type):
                print(f"⏭️ Skipping {csv_file_path.name} - data already exists in BigQuery")
                return True
            
            print(f"▶ Uploading {csv_file_path.name} to table '{table_name}'...")
            
            # Create or get table
            if not self._table_exists(table_name):
                schema = self._get_csv_schema(csv_file_path)
                table = bigquery.Table(table_ref, schema=schema)
                
                # Add partitioning by week for better performance
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="upload_timestamp"
                )
                
                self.client.create_table(table)
                print(f"✅ Created table '{table_name}'")
            
            # Configure job for JSON upload
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
            )
            
            # Transform CSV data to JSON format with metadata
            import datetime
            temp_data = []
            with open(csv_file_path, 'r', encoding='utf-8') as file:
                reader = csv.reader(file)
                headers = next(reader)
                
                # Clean headers for BigQuery compatibility
                clean_headers = []
                for header in headers:
                    clean_header = header.replace('%', '_PCT').replace('+', '_PLUS').replace('-', '_MINUS').replace('/', '_PER')
                    clean_headers.append(clean_header)
                
                # Add metadata headers
                clean_headers.extend(["week", "year", "stat_type", "upload_timestamp"])
                
                for row in reader:
                    # Create dictionary from row data
                    row_dict = {}
                    
                    # Map CSV data to clean headers
                    for i, value in enumerate(row):
                        if i < len(clean_headers) - 4:  # Exclude metadata columns
                            row_dict[clean_headers[i]] = str(value) if value else ""
                    
                    # Add metadata
                    row_dict["week"] = str(metadata["week"])
                    row_dict["year"] = str(metadata["year"])
                    row_dict["stat_type"] = str(metadata["stat_type"])
                    row_dict["upload_timestamp"] = datetime.datetime.utcnow().isoformat()
                    
                    temp_data.append(row_dict)
            
            # Upload data
            if temp_data:
                job = self.client.load_table_from_json(
                    temp_data, table_ref, job_config=job_config
                )
                job.result()  # Wait for completion
                
                print(f"✅ Successfully uploaded {len(temp_data)} rows to '{table_name}'")
                print(f"   📊 Week {metadata['week']}, Year {metadata['year']}")
                return True
            else:
                print(f"⚠️ No data rows found in {csv_file_path.name}")
                return False
                
        except Exception as e:
            print(f"❌ Error uploading {csv_file_path.name}: {e}")
            return False
    
    def upload_all_csvs(self, data_dir: Path) -> Dict[str, int]:
        """Upload all CSV files from data directory"""
        csv_files = list(data_dir.glob("ngs_*.csv"))
        
        if not csv_files:
            print("⚠️ No CSV files found in data directory")
            return {"success": 0, "failed": 0, "skipped": 0}
        
        print(f"🚀 BigQuery Upload Starting")
        print("=" * 50)
        print(f"📁 Data directory: {data_dir}")
        print(f"📊 Found {len(csv_files)} CSV files")
        print(f"🎯 Target dataset: {self.project_id}.{self.dataset_name}")
        print()
        
        results = {"success": 0, "failed": 0, "skipped": 0}
        
        for csv_file in sorted(csv_files):
            print(f"Processing {csv_file.name}...")
            
            try:
                if self.upload_csv(csv_file):
                    results["success"] += 1
                else:
                    results["failed"] += 1
            except Exception as e:
                print(f"❌ Failed to process {csv_file.name}: {e}")
                results["failed"] += 1
            
            print()  # Blank line between files
        
        # Final summary
        print("=" * 50)
        print(f"🏁 BigQuery upload completed!")
        print(f"✅ Successful: {results['success']}")
        print(f"❌ Failed: {results['failed']}")
        if results["success"] > 0:
            print(f"\n📋 Query your data:")
            for stat_type, table_name in TABLE_MAPPING.items():
                print(f"   SELECT * FROM `{self.project_id}.{self.dataset_name}.{table_name}` LIMIT 10;")
        
        return results


def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description="Upload NFL stats CSV files to BigQuery")
    parser.add_argument("--dataset", default=DEFAULT_DATASET, help="BigQuery dataset name")
    parser.add_argument("--project", default=PROJECT_ID, help="Google Cloud project ID")
    args = parser.parse_args()
    
    # Validate configuration
    if not args.project:
        print("❌ Error: BIGQUERY_PROJECT_ID environment variable not set")
        print("   Please set: export BIGQUERY_PROJECT_ID='your-project-id'")
        return 1
    
    if not CREDENTIALS_PATH or not Path(CREDENTIALS_PATH).exists():
        print("❌ Error: Google Cloud credentials not found")
        print("   Please set: export GOOGLE_APPLICATION_CREDENTIALS='/path/to/service-account.json'")
        return 1
    
    # Set up paths
    script_dir = Path(__file__).parent
    data_dir = script_dir / "data"
    
    if not data_dir.exists():
        print(f"❌ Error: Data directory not found: {data_dir}")
        return 1
    
    try:
        # Initialize uploader and upload files
        uploader = BigQueryUploader(args.project, args.dataset)
        results = uploader.upload_all_csvs(data_dir)
        
        # Return appropriate exit code
        return 0 if results["failed"] == 0 else 1
        
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
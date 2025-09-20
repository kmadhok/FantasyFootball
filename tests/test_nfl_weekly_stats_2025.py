#!/usr/bin/env python3
"""
NFL Weekly Statistics Test Script for 2025 Season

This script pulls all available weekly NFL statistics for the 2025 season
using the nfl_data_py package, limited to 50 players for testing purposes.

Usage:
    python tests/test_nfl_weekly_stats_2025.py
"""

import os
import sys
import logging
import traceback
from datetime import datetime
from typing import Dict, List, Any, Optional

import pandas as pd
import nfl_data_py as nfl

# Add project root to Python path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class NFLWeeklyStatsTest:
    """Test class for fetching 2025 NFL weekly statistics"""
    
    def __init__(self):
        self.season = 2025
        self.player_limit = 50
        
        print("=" * 80)
        print("NFL WEEKLY STATISTICS TEST - 2025 SEASON")
        print(f"Target Season: {self.season}")
        print(f"Player Limit: {self.player_limit}")
        print("=" * 80)
    
    def run_weekly_stats_test(self) -> Dict[str, Any]:
        """
        Execute the weekly statistics test for 2025 season
        
        Returns:
            Dictionary with test results and data summary
        """
        test_results = {
            'success': False,
            'timestamp': datetime.utcnow().isoformat(),
            'season': self.season,
            'player_limit': self.player_limit,
            'data_available': False,
            'total_records': 0,
            'weeks_available': [],
            'unique_players': 0,
            'positions_found': [],
            'teams_found': [],
            'sample_data': {},
            'errors': []
        }
        
        try:
            logger.info(f"Starting NFL weekly stats test for {self.season} season")
            print(f"\n🏈 Fetching NFL weekly statistics for {self.season}...")
            
            # Fetch weekly data for 2025
            weekly_df = self._fetch_weekly_data()
            
            if weekly_df is None or weekly_df.empty:
                test_results['errors'].append("No weekly data available for 2025 season")
                logger.warning("No weekly data returned from nfl_data_py")
                return test_results
            
            # Process and analyze the data
            test_results['data_available'] = True
            test_results['total_records'] = len(weekly_df)
            
            logger.info(f"✓ Fetched {len(weekly_df)} total records")
            
            # Limit to first 50 players for testing
            limited_df = self._limit_to_players(weekly_df)
            
            # Analyze the data
            analysis_results = self._analyze_weekly_data(limited_df)
            test_results.update(analysis_results)
            
            # Generate sample data
            sample_data = self._generate_sample_data(limited_df)
            test_results['sample_data'] = sample_data
            
            test_results['success'] = True
            logger.info("✓ Weekly stats test completed successfully")
            
        except Exception as e:
            error_msg = f"Weekly stats test failed: {e}"
            test_results['errors'].append(error_msg)
            logger.error(error_msg)
            traceback.print_exc()
        
        finally:
            self._print_test_summary(test_results)
        
        return test_results
    
    def _fetch_weekly_data(self) -> Optional[pd.DataFrame]:
        """Fetch weekly NFL data using nfl_data_py"""
        try:
            logger.info(f"Calling nfl.import_weekly_data([{self.season}])...")
            
            # Fetch weekly data for the 2025 season
            weekly_df = nfl.import_weekly_data([self.season])
            
            if weekly_df.empty:
                logger.warning(f"No weekly data available for {self.season} season")
                print(f"   ⚠️  No data available for {self.season} season yet")
                return None
            
            logger.info(f"✓ Successfully fetched weekly data: {len(weekly_df)} records")
            print(f"   ✅ Found {len(weekly_df):,} total weekly stat records")
            
            return weekly_df
            
        except Exception as e:
            logger.error(f"Failed to fetch weekly data: {e}")
            print(f"   ❌ Error fetching data: {e}")
            return None
    
    def _limit_to_players(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limit dataset to first N players for testing"""
        try:
            if 'player_id' in df.columns:
                # Get unique player IDs and limit to first 50
                unique_players = df['player_id'].dropna().unique()[:self.player_limit]
                limited_df = df[df['player_id'].isin(unique_players)]
            elif 'player_name' in df.columns:
                # Fallback to player names if player_id not available
                unique_players = df['player_name'].dropna().unique()[:self.player_limit]
                limited_df = df[df['player_name'].isin(unique_players)]
            else:
                # If no player identifier, just take first N records
                limited_df = df.head(self.player_limit)
            
            logger.info(f"✓ Limited dataset to {len(limited_df)} records from {len(unique_players)} players")
            print(f"   📊 Limited to {len(unique_players)} players ({len(limited_df)} total records)")
            
            return limited_df
            
        except Exception as e:
            logger.warning(f"Failed to limit players, using full dataset: {e}")
            return df
    
    def _analyze_weekly_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze the weekly data and extract key metrics"""
        analysis = {
            'weeks_available': [],
            'unique_players': 0,
            'positions_found': [],
            'teams_found': [],
            'columns_available': list(df.columns),
            'key_stats_columns': []
        }
        
        try:
            # Analyze weeks
            if 'week' in df.columns:
                weeks = sorted(df['week'].dropna().unique())
                analysis['weeks_available'] = [int(w) for w in weeks]
                logger.info(f"✓ Weeks available: {analysis['weeks_available']}")
                print(f"   📅 Weeks available: {analysis['weeks_available']}")
            
            # Analyze players
            if 'player_id' in df.columns:
                analysis['unique_players'] = df['player_id'].nunique()
            elif 'player_name' in df.columns:
                analysis['unique_players'] = df['player_name'].nunique()
            
            logger.info(f"✓ Unique players: {analysis['unique_players']}")
            print(f"   👥 Unique players: {analysis['unique_players']}")
            
            # Analyze positions
            if 'position' in df.columns:
                positions = df['position'].dropna().unique().tolist()
                analysis['positions_found'] = sorted(positions)
                logger.info(f"✓ Positions found: {analysis['positions_found']}")
                print(f"   🏃 Positions: {analysis['positions_found']}")
            
            # Analyze teams
            team_col = None
            for col in ['recent_team', 'team', 'current_team']:
                if col in df.columns:
                    team_col = col
                    break
            
            if team_col:
                teams = df[team_col].dropna().unique().tolist()
                analysis['teams_found'] = sorted(teams)
                logger.info(f"✓ Teams found: {len(analysis['teams_found'])} teams")
                print(f"   🏈 Teams: {len(analysis['teams_found'])} teams")
            
            # Identify key fantasy stats columns
            fantasy_stats = [
                'fantasy_points', 'fantasy_points_ppr', 'passing_yards', 'passing_tds',
                'rushing_yards', 'rushing_tds', 'receiving_yards', 'receiving_tds',
                'receptions', 'targets', 'attempts', 'completions'
            ]
            
            key_stats = [col for col in fantasy_stats if col in df.columns]
            analysis['key_stats_columns'] = key_stats
            
            logger.info(f"✓ Key fantasy stats available: {key_stats}")
            print(f"   📈 Key fantasy stats: {len(key_stats)} available")
            
        except Exception as e:
            logger.warning(f"Error during data analysis: {e}")
        
        return analysis
    
    def _generate_sample_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generate sample data for display"""
        sample_data = {}
        
        try:
            if not df.empty:
                # Get top 5 records as sample
                sample_records = df.head(5).to_dict('records')
                sample_data['sample_records'] = sample_records
                sample_data['sample_count'] = len(sample_records)
                
                # Generate basic statistics
                if 'fantasy_points_ppr' in df.columns:
                    ppr_stats = df['fantasy_points_ppr'].dropna().describe()
                    sample_data['ppr_fantasy_stats'] = ppr_stats.to_dict()
                
                logger.info(f"✓ Generated sample data: {len(sample_records)} records")
            
        except Exception as e:
            logger.warning(f"Error generating sample data: {e}")
        
        return sample_data
    
    def _print_test_summary(self, results: Dict[str, Any]):
        """Print comprehensive test summary"""
        print("\n" + "=" * 80)
        print("NFL WEEKLY STATISTICS TEST SUMMARY")
        print("=" * 80)
        
        success = results['success']
        status_icon = "✅" if success else "❌"
        
        print(f"\n{status_icon} TEST STATUS: {'PASS' if success else 'FAIL'}")
        print(f"📅 Test Date: {results['timestamp']}")
        print(f"🏈 Season: {results['season']}")
        
        if results['data_available']:
            print(f"\n📊 DATA SUMMARY:")
            print(f"  • Total Records: {results['total_records']:,}")
            print(f"  • Unique Players: {results['unique_players']}")
            print(f"  • Weeks Available: {results['weeks_available']}")
            print(f"  • Positions: {results.get('positions_found', [])}")
            print(f"  • Teams: {len(results.get('teams_found', []))} teams")
            print(f"  • Key Stats Columns: {len(results.get('key_stats_columns', []))}")
            
            if results.get('sample_data', {}).get('sample_records'):
                print(f"\n📝 SAMPLE DATA (First 3 Players):")
                for i, record in enumerate(results['sample_data']['sample_records'][:3]):
                    player_name = record.get('player_name', record.get('player_id', f'Player {i+1}'))
                    position = record.get('position', 'N/A')
                    team = record.get('recent_team', record.get('team', 'N/A'))
                    week = record.get('week', 'N/A')
                    fantasy_pts = record.get('fantasy_points_ppr', record.get('fantasy_points', 'N/A'))
                    
                    print(f"    {i+1}. {player_name} ({position}, {team}) - Week {week}: {fantasy_pts} pts")
        else:
            print(f"\n⚠️  NO DATA AVAILABLE:")
            print(f"  • 2025 NFL season data may not be available yet")
            print(f"  • Try again during/after the 2025 NFL season")
        
        if results.get('errors'):
            print(f"\n❌ ERRORS:")
            for error in results['errors']:
                print(f"  • {error}")
        
        print(f"\n{'🎉 NFL Weekly Stats Test PASSED!' if success else '⚠️  NFL Weekly Stats Test needs attention'}")
        print("=" * 80)


def main():
    """Run the NFL weekly statistics test"""
    print("Starting NFL Weekly Statistics Test for 2025...")
    
    try:
        tester = NFLWeeklyStatsTest()
        results = tester.run_weekly_stats_test()
        
        # Return appropriate exit code
        exit_code = 0 if results['success'] else 1
        print(f"\nExiting with code {exit_code}")
        return exit_code
        
    except Exception as e:
        print(f"❌ CRITICAL ERROR: {e}")
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
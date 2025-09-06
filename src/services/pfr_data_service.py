import logging
import pandas as pd
import time
import random
from typing import Dict, List, Optional, Any
from datetime import datetime
from dataclasses import dataclass

import requests
from bs4 import BeautifulSoup
# from pro_football_reference_web_scraper import player_game_log as pfr_player
# from pro_football_reference_web_scraper import team_game_log as pfr_team
from src.config.config import get_config
from src.database import SessionLocal, Player, PlayerUsage
from src.utils.player_id_mapper import PlayerIDMapper

logger = logging.getLogger(__name__)

@dataclass
class PFRRedZoneData:
    """Container for Pro Football Reference red zone statistics"""
    player_id: int
    name: str
    position: str
    team: str
    week: int
    season: int
    rz_touches: Optional[int] = None
    ez_targets: Optional[int] = None
    rz_carries: Optional[int] = None
    rz_targets: Optional[int] = None
    rz_receptions: Optional[int] = None
    goal_line_carries: Optional[int] = None

class PFRDataService:
    """Service for integrating with Pro Football Reference via web scraper for specialized stats"""
    
    def __init__(self):
        self.config = get_config()
        self.player_mapper = PlayerIDMapper()
        self.current_season = 2025
        self.request_count = 0
        self.last_request_time = 0
        self.min_delay = 3.0  # Minimum 3 seconds between requests (20 req/min limit)
        self.max_delay = 5.0  # Maximum 5 seconds for randomization
        
    def is_available(self) -> bool:
        """Check if PFR scraper is available (always true for web scraper)"""
        return True
        
    def _respect_rate_limits(self):
        """Implement rate limiting to comply with PFR's 20 requests/minute limit"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        # Ensure minimum delay between requests
        if time_since_last < self.min_delay:
            sleep_time = random.uniform(self.min_delay, self.max_delay)
            logger.info(f"Rate limiting: waiting {sleep_time:.1f} seconds...")
            time.sleep(sleep_time)
        
        self.request_count += 1
        self.last_request_time = time.time()
        
        # Log rate limit status
        if self.request_count % 5 == 0:
            logger.info(f"PFR requests made: {self.request_count} (respecting 20/minute limit)")
        
    def _build_pfr_url(self, player_name: str, season: int) -> str:
        """Build PFR URL for player game log (simplified approach)"""
        # For now, use known URLs for common players
        # This could be enhanced with a player name -> URL mapping
        name_mappings = {
            'Josh Allen': 'AlleJo02',
            'Christian McCaffrey': 'McCaCh01', 
            'Cooper Kupp': 'KuppCo00',
            'Travis Kelce': 'KelcTr01',
            'Derrick Henry': 'HenrDe00',
            'CeeDee Lamb': 'LambCe00'
        }
        
        url_code = name_mappings.get(player_name)
        if url_code:
            return f"https://www.pro-football-reference.com/players/{url_code[0]}/{url_code}/gamelog/{season}/"
        
        # For unknown players, return empty string (will be handled gracefully)
        return ""
    
    def fetch_player_game_log(self, player_name: str, position: str, season: int) -> pd.DataFrame:
        """
        Fetch player game log from Pro Football Reference using manual scraping
        
        Args:
            player_name: Full player name
            position: Player position (QB, RB, WR, TE)
            season: NFL season year
        
        Returns:
            DataFrame with player game log data
        """
        try:
            logger.info(f"Fetching game log for {player_name} ({position}) - {season}")
            
            # Build PFR URL
            url = self._build_pfr_url(player_name, season)
            if not url:
                logger.warning(f"No URL mapping available for {player_name}")
                return pd.DataFrame()
            
            # Respect rate limits before making request
            self._respect_rate_limits()
            
            # Make direct request to PFR
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            # Parse HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find game log table
            table = soup.find('table', {'id': 'stats'})
            if not table:
                logger.warning(f"No game log table found for {player_name}")
                return pd.DataFrame()
            
            # Extract table data
            rows = table.find_all('tr')
            if len(rows) < 2:
                logger.warning(f"No game data found for {player_name}")
                return pd.DataFrame()
            
            # Get headers
            header_row = rows[0]
            headers = [th.get_text().strip() for th in header_row.find_all(['th', 'td'])]
            
            # Get data rows (skip header)
            data_rows = []
            for row in rows[1:]:
                cells = row.find_all(['td', 'th'])
                if len(cells) > 0:
                    row_data = [cell.get_text().strip() for cell in cells]
                    data_rows.append(row_data)
            
            if not data_rows:
                logger.warning(f"No valid game data rows for {player_name}")
                return pd.DataFrame()
            
            # Create DataFrame - ensure column count matches data
            max_cols = max(len(row) for row in data_rows) if data_rows else 0
            if len(headers) > max_cols:
                headers = headers[:max_cols]
            elif len(headers) < max_cols:
                # Pad headers if needed
                for i in range(len(headers), max_cols):
                    headers.append(f'col_{i}')
            
            # Pad rows to match header count
            for i, row in enumerate(data_rows):
                if len(row) < len(headers):
                    data_rows[i] = row + [''] * (len(headers) - len(row))
                elif len(row) > len(headers):
                    data_rows[i] = row[:len(headers)]
            
            game_log_df = pd.DataFrame(data_rows, columns=headers)
            
            logger.info(f"✓ Fetched {len(game_log_df)} games for {player_name} via manual scraping")
            logger.info(f"   Columns: {len(headers)}, Sample: {headers[:5]}...")
            return game_log_df
            
        except Exception as e:
            logger.error(f"Failed to fetch game log for {player_name}: {e}")
            # If we get a 429 or similar error, log it specifically
            if "429" in str(e) or "rate" in str(e).lower():
                logger.warning("Rate limit encountered - may need longer delays or waiting period")
            return pd.DataFrame()
    
    def extract_red_zone_stats(self, game_log_df: pd.DataFrame, player_name: str, 
                              position: str, team: str, season: int) -> List[PFRRedZoneData]:
        """
        Extract red zone statistics from player game log
        
        Args:
            game_log_df: Player game log DataFrame
            player_name: Player name
            position: Player position
            team: Player team
            season: NFL season
        
        Returns:
            List of PFRRedZoneData objects
        """
        try:
            if game_log_df.empty:
                return []
            
            # Get or create player ID
            canonical_id = self.player_mapper.generate_canonical_id(player_name, position, team)
            player_id = self._get_or_create_player(canonical_id, player_name, position, team)
            
            if not player_id:
                return []
            
            red_zone_data = []
            
            for idx, game_row in game_log_df.iterrows():
                try:
                    week = game_row.get('week', idx + 1)  # Fallback to game number
                    
                    # Extract red zone statistics based on position
                    rz_data = PFRRedZoneData(
                        player_id=player_id,
                        name=player_name,
                        position=position,
                        team=team,
                        week=int(week) if week else idx + 1,
                        season=season
                    )
                    
                    if position == 'RB':
                        # For RBs, focus on carries and targets in red zone
                        rz_data.rz_carries = self._estimate_rz_carries(game_row)
                        rz_data.rz_targets = game_row.get('targets_rz', 0) or 0
                        rz_data.rz_touches = (rz_data.rz_carries or 0) + (rz_data.rz_targets or 0)
                        rz_data.goal_line_carries = self._estimate_goal_line_carries(game_row)
                    
                    elif position in ['WR', 'TE']:
                        # For receivers, focus on targets and receptions
                        rz_data.rz_targets = game_row.get('targets_rz', 0) or 0
                        rz_data.rz_receptions = game_row.get('receptions_rz', 0) or 0
                        rz_data.ez_targets = self._estimate_end_zone_targets(game_row)
                        rz_data.rz_touches = rz_data.rz_receptions or 0
                    
                    # Only include weeks with meaningful red zone activity
                    if (rz_data.rz_touches and rz_data.rz_touches > 0) or \
                       (rz_data.ez_targets and rz_data.ez_targets > 0):
                        red_zone_data.append(rz_data)
                        
                except Exception as e:
                    logger.warning(f"Failed to process game {idx} for {player_name}: {e}")
                    continue
            
            logger.info(f"✓ Extracted red zone data for {len(red_zone_data)} games")
            return red_zone_data
            
        except Exception as e:
            logger.error(f"Failed to extract red zone stats for {player_name}: {e}")
            return []
    
    def fetch_team_red_zone_stats(self, team: str, season: int) -> Dict[str, Any]:
        """
        Fetch team-level red zone statistics
        
        Args:
            team: Team name or abbreviation
            season: NFL season
        
        Returns:
            Dictionary with team red zone statistics
        """
        try:
            logger.info(f"Fetching team red zone stats for {team} - {season}")
            
            # Respect rate limits before making request
            self._respect_rate_limits()
            
            # Get team game log
            team_log_df = pfr_team.get_team_game_log(team=team, season=season)
            
            if team_log_df.empty:
                logger.warning(f"No team data found for {team}")
                return {}
            
            # Calculate team red zone efficiency
            total_rz_attempts = team_log_df.get('red_zone_attempts', pd.Series()).sum() or 0
            total_rz_scores = team_log_df.get('red_zone_scores', pd.Series()).sum() or 0
            
            rz_efficiency = (total_rz_scores / total_rz_attempts) if total_rz_attempts > 0 else 0.0
            
            team_stats = {
                'team': team,
                'season': season,
                'red_zone_attempts': total_rz_attempts,
                'red_zone_scores': total_rz_scores,
                'red_zone_efficiency': rz_efficiency,
                'games_played': len(team_log_df)
            }
            
            logger.info(f"✓ Team {team} RZ efficiency: {rz_efficiency:.1%}")
            return team_stats
            
        except Exception as e:
            logger.error(f"Failed to fetch team red zone stats for {team}: {e}")
            return {}
    
    def build_red_zone_data_for_players(self, player_list: List[Dict], season: int) -> List[PFRRedZoneData]:
        """
        Build red zone data for a list of players
        
        Args:
            player_list: List of player dictionaries with name, position, team
            season: NFL season
        
        Returns:
            List of PFRRedZoneData objects
        """
        try:
            logger.info(f"Building red zone data for {len(player_list)} players")
            
            all_red_zone_data = []
            
            for player_info in player_list:
                try:
                    name = player_info.get('name', '')
                    position = player_info.get('position', '')
                    team = player_info.get('team', '')
                    
                    if not all([name, position, team]):
                        logger.warning(f"Incomplete player info: {player_info}")
                        continue
                    
                    # Skip positions that don't have meaningful red zone usage
                    if position not in ['QB', 'RB', 'WR', 'TE']:
                        continue
                    
                    # Fetch player game log (rate limiting handled in fetch_player_game_log)
                    game_log_df = self.fetch_player_game_log(name, position, season)
                    
                    if not game_log_df.empty:
                        # Extract red zone stats
                        player_rz_data = self.extract_red_zone_stats(
                            game_log_df, name, position, team, season
                        )
                        all_red_zone_data.extend(player_rz_data)
                    
                    # Additional delay handled by _respect_rate_limits() method
                    
                except Exception as e:
                    logger.warning(f"Failed to process player {player_info.get('name', 'unknown')}: {e}")
                    continue
            
            logger.info(f"✓ Built red zone data for {len(all_red_zone_data)} player-games")
            return all_red_zone_data
            
        except Exception as e:
            logger.error(f"Failed to build red zone data: {e}")
            return []
    
    def sync_red_zone_data_to_database(self, red_zone_data: List[PFRRedZoneData]) -> bool:
        """
        Sync red zone data to the database by updating PlayerUsage records
        
        Args:
            red_zone_data: List of red zone data to sync
        
        Returns:
            Success boolean
        """
        try:
            if not red_zone_data:
                logger.warning("No red zone data to sync")
                return False
            
            db = SessionLocal()
            updated_count = 0
            
            try:
                for rz_data in red_zone_data:
                    # Find existing PlayerUsage record
                    usage_record = db.query(PlayerUsage).filter(
                        PlayerUsage.player_id == rz_data.player_id,
                        PlayerUsage.week == rz_data.week,
                        PlayerUsage.season == rz_data.season
                    ).first()
                    
                    if usage_record:
                        # Update red zone stats
                        usage_record.rz_touches = rz_data.rz_touches
                        usage_record.ez_targets = rz_data.ez_targets
                        usage_record.updated_at = datetime.utcnow()
                        updated_count += 1
                    else:
                        # Create new usage record with red zone data
                        new_usage = PlayerUsage(
                            player_id=rz_data.player_id,
                            week=rz_data.week,
                            season=rz_data.season,
                            rz_touches=rz_data.rz_touches,
                            ez_targets=rz_data.ez_targets
                        )
                        db.add(new_usage)
                        updated_count += 1
                
                db.commit()
                logger.info(f"✓ Synced red zone data for {updated_count} player-weeks")
                return True
                
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"Failed to sync red zone data: {e}")
            return False
    
    def weekly_red_zone_sync_job(self, week: Optional[int] = None, season: Optional[int] = None) -> Dict[str, Any]:
        """
        Weekly sync job for red zone statistics
        
        Args:
            week: Specific week to sync
            season: Specific season to sync
        
        Returns:
            Dictionary with sync results
        """
        try:
            if season is None:
                season = self.current_season
            
            if week is None:
                week = self._get_current_nfl_week()
            
            logger.info(f"Starting weekly PFR red zone sync for week {week}, season {season}")
            
            # Get active players from database
            db = SessionLocal()
            try:
                active_players = db.query(Player).filter(
                    Player.position.in_(['QB', 'RB', 'WR', 'TE'])
                ).limit(20).all()  # Limit to avoid overwhelming PFR
                
                player_list = [
                    {
                        'name': player.name,
                        'position': player.position,
                        'team': player.team
                    }
                    for player in active_players
                ]
                
            finally:
                db.close()
            
            if not player_list:
                return {
                    'pfr_red_zone_sync': False,
                    'error': 'No active players found',
                    'week': week,
                    'season': season
                }
            
            # Build red zone data
            red_zone_data = self.build_red_zone_data_for_players(player_list, season)
            
            if not red_zone_data:
                return {
                    'pfr_red_zone_sync': False,
                    'error': 'No red zone data available',
                    'week': week,
                    'season': season
                }
            
            # Sync to database
            sync_success = self.sync_red_zone_data_to_database(red_zone_data)
            
            return {
                'pfr_red_zone_sync': sync_success,
                'records_processed': len(red_zone_data),
                'players_processed': len(player_list),
                'week': week,
                'season': season
            }
            
        except Exception as e:
            logger.error(f"Weekly PFR sync job failed: {e}")
            return {
                'pfr_red_zone_sync': False,
                'error': str(e)
            }
    
    def _estimate_rz_carries(self, game_row: pd.Series) -> int:
        """Estimate red zone carries from game stats"""
        total_carries = game_row.get('carries', 0) or 0
        touchdowns = game_row.get('rushing_tds', 0) or 0
        
        # Rough estimate: TDs + some percentage of total carries
        estimated_rz = touchdowns + max(0, int(total_carries * 0.15))
        return min(estimated_rz, total_carries)
    
    def _estimate_goal_line_carries(self, game_row: pd.Series) -> int:
        """Estimate goal line carries"""
        rushing_tds = game_row.get('rushing_tds', 0) or 0
        # Simple estimate: assume most rushing TDs are from goal line
        return max(0, rushing_tds)
    
    def _estimate_end_zone_targets(self, game_row: pd.Series) -> int:
        """Estimate end zone targets for receivers"""
        rec_tds = game_row.get('receiving_tds', 0) or 0
        targets = game_row.get('targets', 0) or 0
        
        # Estimate: TDs + small percentage of targets
        estimated_ez = rec_tds + max(0, int(targets * 0.1))
        return min(estimated_ez, targets)
    
    def _get_or_create_player(self, canonical_id: str, name: str, position: str, team: str) -> Optional[int]:
        """Get or create player in database"""
        try:
            db = SessionLocal()
            try:
                player = db.query(Player).filter(Player.nfl_id == canonical_id).first()
                
                if player:
                    return player.id
                
                new_player = Player(
                    nfl_id=canonical_id,
                    name=name,
                    position=position,
                    team=team,
                    is_starter=position in ['QB', 'RB', 'WR', 'TE']
                )
                
                db.add(new_player)
                db.commit()
                
                return new_player.id
                
            finally:
                db.close()
                
        except Exception as e:
            logger.warning(f"Failed to get/create player {name}: {e}")
            return None
    
    def _get_current_nfl_week(self) -> int:
        """Get current NFL week"""
        now = datetime.now()
        if now.month >= 9:
            return min(max(now.isocalendar()[1] - 35, 1), 18)
        else:
            return 1

def test_pfr_rate_limiting():
    """Test PFR with minimal requests to verify rate limiting works"""
    print("Testing PFR Rate Limiting Strategy...")
    print("=" * 60)
    
    service = PFRDataService()
    
    try:
        # Test with just one well-known player and 2023 data (more stable)
        print("\n1. Testing single player with rate limiting...")
        print("   Player: Josh Allen (QB, BUF) - 2023 season")
        print("   This will take ~5 seconds due to rate limiting...")
        
        game_log = service.fetch_player_game_log('Josh Allen', 'QB', 2023)
        print(f"   Result: {len(game_log)} games fetched")
        
        if not game_log.empty:
            print(f"   ✓ SUCCESS: Got real data from PFR!")
            print(f"   Columns available: {len(game_log.columns)} columns")
            print(f"   Sample columns: {list(game_log.columns)[:8]}...")
            
            # Test red zone extraction
            print("\n2. Testing red zone data extraction...")
            rz_data = service.extract_red_zone_stats(game_log, 'Josh Allen', 'QB', 'BUF', 2023)
            print(f"   Red zone data points: {len(rz_data)}")
            
            if rz_data:
                sample = rz_data[0]
                print(f"   Sample: Week {sample.week}, RZ touches: {sample.rz_touches}")
        else:
            print("   ❌ Still blocked or no data available")
            
        return len(game_log) > 0
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False

# Test function
def test_pfr_data_service():
    """Test the PFR data service"""
    print("Testing PFR Data Service...")
    print("=" * 60)
    
    service = PFRDataService()
    
    try:
        # First test rate limiting approach
        print("Phase 1: Testing rate limiting strategy...")
        rate_limit_success = test_pfr_rate_limiting()
        
        if not rate_limit_success:
            print("\n⚠️  Rate limiting test failed - PFR may still be blocking")
            print("   Recommendation: Wait 24 hours for rate limit jail to expire")
            return False
        
        # If rate limiting works, test with more data
        print("\nPhase 2: Testing with additional players...")
        test_players = [
            {'name': 'Christian McCaffrey', 'position': 'RB', 'team': 'SF'},
            {'name': 'Cooper Kupp', 'position': 'WR', 'team': 'LAR'}
        ]
        
        print("\n1. Testing player game log fetch...")
        player = test_players[0]
        game_log = service.fetch_player_game_log(
            player['name'], player['position'], 2024
        )
        print(f"   Fetched game log with {len(game_log)} games")
        
        if not game_log.empty:
            print(f"   Columns: {list(game_log.columns)[:10]}...")  # Show first 10 columns
        
        print("\n2. Testing red zone data extraction...")
        if not game_log.empty:
            rz_data = service.extract_red_zone_stats(
                game_log, player['name'], player['position'], player['team'], 2024
            )
            print(f"   Extracted red zone data for {len(rz_data)} games")
            
            if rz_data:
                sample = rz_data[0]
                print(f"   Sample RZ data: Week {sample.week}")
                print(f"     RZ touches: {sample.rz_touches}")
                print(f"     EZ targets: {sample.ez_targets}")
        
        print("\n3. Testing team red zone stats...")
        team_stats = service.fetch_team_red_zone_stats('San Francisco 49ers', 2024)
        if team_stats:
            print(f"   Team RZ efficiency: {team_stats.get('red_zone_efficiency', 0):.1%}")
        
        print("\n4. Testing weekly sync job...")
        result = service.weekly_red_zone_sync_job(1, 2024)
        print(f"   Sync job result: {result.get('pfr_red_zone_sync', False)}")
        
        return True
        
    except Exception as e:
        print(f"   ✗ PFR data service test failed: {e}")
        return False

if __name__ == "__main__":
    test_pfr_data_service()
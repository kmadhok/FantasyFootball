import logging
import pandas as pd
import nfl_data_py as nfl
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from dataclasses import dataclass

from src.config.config import get_config
from src.database import SessionLocal, Player, PlayerUsage, PlayerInjuryReport, DepthChart, BettingLine, NFLSchedule, DefensiveStats
from src.utils.player_id_mapper import PlayerIDMapper

logger = logging.getLogger(__name__)

@dataclass
class NFLUsageData:
    """Container for NFL usage statistics"""
    player_id: int
    name: str
    position: str
    team: str
    week: int
    season: int
    snap_pct: Optional[float] = None
    targets: Optional[int] = None
    carries: Optional[int] = None
    receptions: Optional[int] = None
    receiving_yards: Optional[float] = None
    rushing_yards: Optional[float] = None
    touchdowns: Optional[int] = None
    target_share: Optional[float] = None
    carry_share: Optional[float] = None
    route_pct: Optional[float] = None
    rz_touches: Optional[int] = None
    ez_targets: Optional[int] = None

@dataclass
class NFLInjuryData:
    """Container for NFL injury report data"""
    player_id: int
    name: str
    position: str
    team: str
    week: int
    season: int
    report_status: Optional[str] = None  # Out, IR, Doubtful, Questionable, Probable
    practice_status: Optional[str] = None  # DNP, LP, FP
    practice_participation_pct: Optional[float] = None
    injury_description: Optional[str] = None
    days_on_report: int = 0

@dataclass
class NFLDepthChartData:
    """Container for NFL depth chart data"""
    player_id: int
    name: str
    position: str
    team: str
    week: int
    season: int
    depth_rank: int
    formation: Optional[str] = None

@dataclass
class NFLBettingData:
    """Container for NFL betting lines data"""
    game_id: str
    home_team: str
    away_team: str
    week: int
    season: int
    total_line: Optional[float] = None
    spread_line: Optional[float] = None
    home_implied_total: Optional[float] = None
    away_implied_total: Optional[float] = None
    home_moneyline: Optional[int] = None
    away_moneyline: Optional[int] = None
    sportsbook: str = 'consensus'

class NFLDataService:
    """Service for integrating with nfl-data-py package for real NFL statistics"""
    
    def __init__(self):
        self.config = get_config()
        self.player_mapper = PlayerIDMapper()
        self.current_season = 2025
        
    def fetch_snap_counts(self, years: List[int] = None, weeks: List[int] = None) -> pd.DataFrame:
        """
        Fetch snap count data from NFL
        
        Args:
            years: List of years to fetch (defaults to current season)
            weeks: List of weeks to fetch (optional)
        
        Returns:
            DataFrame with snap count data
        """
        try:
            if years is None:
                years = [self.current_season]
            
            logger.info(f"Fetching snap counts for years: {years}")
            
            # Get snap counts data
            snap_df = nfl.import_snap_counts(years)
            
            if snap_df.empty:
                logger.warning(f"No snap count data available for years: {years}")
                return pd.DataFrame()
            
            # Filter by weeks if specified
            if weeks:
                snap_df = snap_df[snap_df['week'].isin(weeks)]
            
            logger.info(f"✓ Fetched {len(snap_df)} snap count records")
            return snap_df
            
        except Exception as e:
            logger.error(f"Failed to fetch snap counts: {e}")
            return pd.DataFrame()
    
    def fetch_weekly_advanced_stats(self, stat_type: str, years: List[int] = None) -> pd.DataFrame:
        """
        Fetch weekly advanced statistics from Pro Football Reference
        
        Args:
            stat_type: Type of stats ('pass', 'rec', 'rush')
            years: List of years to fetch
        
        Returns:
            DataFrame with advanced weekly stats
        """
        try:
            if years is None:
                years = [self.current_season]
            
            if stat_type not in ['pass', 'rec', 'rush']:
                raise ValueError("stat_type must be 'pass', 'rec', or 'rush'")
            
            logger.info(f"Fetching weekly {stat_type} stats for years: {years}")
            
            # Get weekly advanced stats
            stats_df = nfl.import_weekly_pfr(stat_type, years)
            
            if stats_df.empty:
                logger.warning(f"No {stat_type} stats available for years: {years}")
                return pd.DataFrame()
            
            logger.info(f"✓ Fetched {len(stats_df)} {stat_type} stat records")
            return stats_df
            
        except Exception as e:
            logger.error(f"Failed to fetch {stat_type} stats: {e}")
            return pd.DataFrame()
    
    def build_usage_data_from_nfl_stats(self, week: int, season: int = None) -> List[NFLUsageData]:
        """
        Build comprehensive usage data by combining snap counts and advanced stats
        
        Args:
            week: NFL week to process
            season: NFL season (defaults to current)
        
        Returns:
            List of NFLUsageData objects
        """
        try:
            if season is None:
                season = self.current_season
            
            logger.info(f"Building usage data for week {week}, season {season}")
            
            # Fetch snap counts
            snap_df = self.fetch_snap_counts([season], [week])
            
            # Fetch receiving stats
            rec_df = self.fetch_weekly_advanced_stats('rec', [season])
            if not rec_df.empty:
                rec_df = rec_df[rec_df['week'] == week]
            
            # Fetch rushing stats  
            rush_df = self.fetch_weekly_advanced_stats('rush', [season])
            if not rush_df.empty:
                rush_df = rush_df[rush_df['week'] == week]
            
            if snap_df.empty:
                logger.warning(f"No snap count data available for week {week}")
                return []
            
            usage_data = []
            
            # Process each player in snap counts
            for _, snap_row in snap_df.iterrows():
                try:
                    # Get player info
                    player_name = snap_row.get('player', '')
                    position = snap_row.get('position', '')
                    team = snap_row.get('team', '')
                    
                    if not all([player_name, position, team]):
                        continue
                    
                    # Generate canonical player ID
                    canonical_id = self.player_mapper.generate_canonical_id(
                        player_name, position, team
                    )
                    
                    # Get or create player in database
                    player_id = self._get_or_create_player(
                        canonical_id, player_name, position, team
                    )
                    
                    if not player_id:
                        continue
                    
                    # Calculate snap percentage
                    offense_snaps = snap_row.get('offense_snaps', 0)
                    offense_pct = snap_row.get('offense_pct', 0.0)
                    
                    # Find matching receiving stats
                    rec_stats = None
                    if not rec_df.empty:
                        rec_match = rec_df[
                            (rec_df['pfr_player_name'] == player_name) |
                            (rec_df['team'] == team)  # Match by team as fallback
                        ]
                        if not rec_match.empty:
                            rec_stats = rec_match.iloc[0]
                    
                    # Find matching rushing stats
                    rush_stats = None  
                    if not rush_df.empty:
                        rush_match = rush_df[
                            (rush_df['pfr_player_name'] == player_name) |
                            (rush_df['team'] == team)  # Match by team as fallback
                        ]
                        if not rush_match.empty:
                            rush_stats = rush_match.iloc[0]
                    
                    # Build usage data
                    usage = NFLUsageData(
                        player_id=player_id,
                        name=player_name,
                        position=position,
                        team=team,
                        week=week,
                        season=season,
                        snap_pct=offense_pct / 100.0 if offense_pct else None,
                        targets=None,  # Not available in this dataset
                        carries=rush_stats.get('carries') if rush_stats is not None else None,
                        receptions=None,  # Not available in this dataset
                        receiving_yards=None,  # Not available in this dataset 
                        rushing_yards=None,  # Not available in this dataset
                        touchdowns=self._calculate_total_tds(rec_stats, rush_stats)
                    )
                    
                    # Calculate derived metrics
                    usage = self._calculate_usage_metrics(usage, snap_row, rec_stats, rush_stats)
                    
                    usage_data.append(usage)
                    
                except Exception as e:
                    logger.warning(f"Failed to process player {snap_row.get('player', 'unknown')}: {e}")
                    continue
            
            logger.info(f"✓ Built usage data for {len(usage_data)} players")
            return usage_data
            
        except Exception as e:
            logger.error(f"Failed to build usage data: {e}")
            return []
    
    def sync_usage_to_database(self, usage_data: List[NFLUsageData]) -> bool:
        """
        Sync usage data to the database
        
        Args:
            usage_data: List of usage data to sync
        
        Returns:
            Success boolean
        """
        try:
            if not usage_data:
                logger.warning("No usage data to sync")
                return False
            
            db = SessionLocal()
            synced_count = 0
            
            try:
                for usage in usage_data:
                    # Check if usage record already exists
                    existing = db.query(PlayerUsage).filter(
                        PlayerUsage.player_id == usage.player_id,
                        PlayerUsage.week == usage.week,
                        PlayerUsage.season == usage.season
                    ).first()
                    
                    if existing:
                        # Update existing record
                        existing.snap_pct = usage.snap_pct
                        existing.targets = usage.targets
                        existing.carries = usage.carries
                        existing.receptions = usage.receptions
                        existing.receiving_yards = usage.receiving_yards
                        existing.rushing_yards = usage.rushing_yards
                        existing.touchdowns = usage.touchdowns
                        existing.target_share = usage.target_share
                        existing.carry_share = usage.carry_share
                        existing.route_pct = usage.route_pct
                        existing.rz_touches = usage.rz_touches
                        existing.ez_targets = usage.ez_targets
                        existing.updated_at = datetime.utcnow()
                    else:
                        # Create new record
                        new_usage = PlayerUsage(
                            player_id=usage.player_id,
                            week=usage.week,
                            season=usage.season,
                            snap_pct=usage.snap_pct,
                            targets=usage.targets,
                            carries=usage.carries,
                            receptions=usage.receptions,
                            receiving_yards=usage.receiving_yards,
                            rushing_yards=usage.rushing_yards,
                            touchdowns=usage.touchdowns,
                            target_share=usage.target_share,
                            carry_share=usage.carry_share,
                            route_pct=usage.route_pct,
                            rz_touches=usage.rz_touches,
                            ez_targets=usage.ez_targets
                        )
                        db.add(new_usage)
                    
                    synced_count += 1
                
                db.commit()
                logger.info(f"✓ Synced {synced_count} usage records to database")
                return True
                
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"Failed to sync usage data to database: {e}")
            return False
    
    def daily_sync_job(self, week: Optional[int] = None, season: Optional[int] = None) -> Dict[str, Any]:
        """
        Daily sync job to update NFL usage statistics
        
        Args:
            week: Specific week to sync (defaults to current NFL week)
            season: Specific season to sync (defaults to current season)
        
        Returns:
            Dictionary with sync results
        """
        try:
            if season is None:
                season = self.current_season
                
            if week is None:
                week = self._get_current_nfl_week()
            
            logger.info(f"Starting daily NFL data sync for week {week}, season {season}")
            
            # Build usage data from NFL stats
            usage_data = self.build_usage_data_from_nfl_stats(week, season)
            
            if not usage_data:
                return {
                    'nfl_usage_sync': False,
                    'error': 'No usage data available',
                    'week': week,
                    'season': season
                }
            
            # Sync to database
            sync_success = self.sync_usage_to_database(usage_data)
            
            return {
                'nfl_usage_sync': sync_success,
                'records_processed': len(usage_data),
                'week': week,
                'season': season
            }
            
        except Exception as e:
            logger.error(f"Daily NFL sync job failed: {e}")
            return {
                'nfl_usage_sync': False,
                'error': str(e)
            }
    
    def fetch_injury_reports(self, years: List[int] = None) -> pd.DataFrame:
        """
        Fetch injury report data from NFL
        
        Args:
            years: List of years to fetch (defaults to current season)
        
        Returns:
            DataFrame with injury report data
        """
        try:
            if years is None:
                years = [self.current_season]
            
            logger.info(f"Fetching injury reports for years: {years}")
            
            # Get injury data
            injury_df = nfl.import_injuries(years)
            
            if injury_df.empty:
                logger.warning(f"No injury report data available for years: {years}")
                return pd.DataFrame()
            
            logger.info(f"✓ Fetched {len(injury_df)} injury report records")
            return injury_df
            
        except Exception as e:
            logger.error(f"Failed to fetch injury reports: {e}")
            return pd.DataFrame()
    
    def build_injury_data_from_reports(self, week: int, season: int = None) -> List[NFLInjuryData]:
        """
        Build injury data by processing NFL injury reports
        
        Args:
            week: NFL week to process
            season: NFL season (defaults to current)
        
        Returns:
            List of NFLInjuryData objects
        """
        try:
            if season is None:
                season = self.current_season
            
            logger.info(f"Building injury data for week {week}, season {season}")
            
            # Fetch injury reports
            injury_df = self.fetch_injury_reports([season])
            
            if injury_df.empty:
                logger.warning(f"No injury report data available for week {week}")
                return []
            
            # Filter by week if column exists
            if 'week' in injury_df.columns:
                injury_df = injury_df[injury_df['week'] == week]
            
            injury_data = []
            
            # Process each injury report
            for _, injury_row in injury_df.iterrows():
                try:
                    # Get player info
                    player_name = injury_row.get('full_name', injury_row.get('player', ''))
                    position = injury_row.get('position', '')
                    team = injury_row.get('team', '')
                    
                    if not all([player_name, position, team]):
                        continue
                    
                    # Generate canonical player ID
                    canonical_id = self.player_mapper.generate_canonical_id(
                        player_name, position, team
                    )
                    
                    # Get or create player in database
                    player_id = self._get_or_create_player(
                        canonical_id, player_name, position, team
                    )
                    
                    if not player_id:
                        continue
                    
                    # Calculate practice participation percentage
                    practice_pct = self._calculate_practice_participation(injury_row)
                    
                    # Count days on report (if date_modified exists)
                    days_on_report = 0
                    if 'date_modified' in injury_row:
                        try:
                            modified_date = pd.to_datetime(injury_row['date_modified'])
                            days_on_report = (datetime.now() - modified_date).days
                        except:
                            days_on_report = 0
                    
                    # Build injury data
                    injury = NFLInjuryData(
                        player_id=player_id,
                        name=player_name,
                        position=position,
                        team=team,
                        week=week,
                        season=season,
                        report_status=injury_row.get('report_status'),
                        practice_status=injury_row.get('practice_status'),
                        practice_participation_pct=practice_pct,
                        injury_description=injury_row.get('injury_description', injury_row.get('injury', '')),
                        days_on_report=days_on_report
                    )
                    
                    injury_data.append(injury)
                    
                except Exception as e:
                    logger.warning(f"Failed to process injury report for {injury_row.get('full_name', 'unknown')}: {e}")
                    continue
            
            logger.info(f"✓ Built injury data for {len(injury_data)} players")
            return injury_data
            
        except Exception as e:
            logger.error(f"Failed to build injury data: {e}")
            return []
    
    def sync_injury_data_to_database(self, injury_data: List[NFLInjuryData]) -> bool:
        """
        Sync injury data to the database
        
        Args:
            injury_data: List of injury data to sync
        
        Returns:
            Success boolean
        """
        try:
            if not injury_data:
                logger.warning("No injury data to sync")
                return False
            
            db = SessionLocal()
            synced_count = 0
            
            try:
                for injury in injury_data:
                    # Check if injury record already exists
                    existing = db.query(PlayerInjuryReport).filter(
                        PlayerInjuryReport.player_id == injury.player_id,
                        PlayerInjuryReport.week == injury.week,
                        PlayerInjuryReport.season == injury.season
                    ).first()
                    
                    if existing:
                        # Update existing record
                        existing.report_status = injury.report_status
                        existing.practice_status = injury.practice_status
                        existing.practice_participation_pct = injury.practice_participation_pct
                        existing.injury_description = injury.injury_description
                        existing.days_on_report = injury.days_on_report
                        existing.updated_at = datetime.utcnow()
                    else:
                        # Create new record
                        new_injury = PlayerInjuryReport(
                            player_id=injury.player_id,
                            week=injury.week,
                            season=injury.season,
                            report_status=injury.report_status,
                            practice_status=injury.practice_status,
                            practice_participation_pct=injury.practice_participation_pct,
                            injury_description=injury.injury_description,
                            days_on_report=injury.days_on_report
                        )
                        db.add(new_injury)
                    
                    synced_count += 1
                
                db.commit()
                logger.info(f"✓ Synced {synced_count} injury records to database")
                return True
                
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"Failed to sync injury data to database: {e}")
            return False
    
    def fetch_depth_charts(self, years: List[int] = None) -> pd.DataFrame:
        """
        Fetch depth chart data from NFL
        
        Args:
            years: List of years to fetch (defaults to current season)
        
        Returns:
            DataFrame with depth chart data
        """
        try:
            if years is None:
                years = [self.current_season]
            
            logger.info(f"Fetching depth charts for years: {years}")
            
            # Get depth chart data
            depth_df = nfl.import_depth_charts(years)
            
            if depth_df.empty:
                logger.warning(f"No depth chart data available for years: {years}")
                return pd.DataFrame()
            
            logger.info(f"✓ Fetched {len(depth_df)} depth chart records")
            return depth_df
            
        except Exception as e:
            logger.error(f"Failed to fetch depth charts: {e}")
            return pd.DataFrame()
    
    def build_depth_chart_data(self, week: int, season: int = None) -> List[NFLDepthChartData]:
        """
        Build depth chart data from NFL depth charts
        
        Args:
            week: NFL week to process
            season: NFL season (defaults to current)
        
        Returns:
            List of NFLDepthChartData objects
        """
        try:
            if season is None:
                season = self.current_season
            
            logger.info(f"Building depth chart data for week {week}, season {season}")
            
            # Fetch depth charts
            depth_df = self.fetch_depth_charts([season])
            
            if depth_df.empty:
                logger.warning(f"No depth chart data available for week {week}")
                return []
            
            depth_data = []
            
            # Process each depth chart entry
            for _, depth_row in depth_df.iterrows():
                try:
                    # Get player info
                    player_name = depth_row.get('full_name', depth_row.get('player', ''))
                    position = depth_row.get('position', '')
                    team = depth_row.get('team', '')
                    
                    if not all([player_name, position, team]):
                        continue
                    
                    # Generate canonical player ID
                    canonical_id = self.player_mapper.generate_canonical_id(
                        player_name, position, team
                    )
                    
                    # Get or create player in database
                    player_id = self._get_or_create_player(
                        canonical_id, player_name, position, team
                    )
                    
                    if not player_id:
                        continue
                    
                    # Get depth rank (default to 3 if not specified)
                    depth_rank = depth_row.get('depth', depth_row.get('depth_rank', 3))
                    if isinstance(depth_rank, str):
                        try:
                            depth_rank = int(depth_rank)
                        except ValueError:
                            depth_rank = 3
                    
                    # Build depth chart data
                    depth = NFLDepthChartData(
                        player_id=player_id,
                        name=player_name,
                        position=position,
                        team=team,
                        week=week,
                        season=season,
                        depth_rank=depth_rank,
                        formation=depth_row.get('formation', '11 Personnel')  # Default formation
                    )
                    
                    depth_data.append(depth)
                    
                except Exception as e:
                    logger.warning(f"Failed to process depth chart for {depth_row.get('full_name', 'unknown')}: {e}")
                    continue
            
            logger.info(f"✓ Built depth chart data for {len(depth_data)} players")
            return depth_data
            
        except Exception as e:
            logger.error(f"Failed to build depth chart data: {e}")
            return []
    
    def sync_depth_chart_data_to_database(self, depth_data: List[NFLDepthChartData]) -> bool:
        """
        Sync depth chart data to the database
        
        Args:
            depth_data: List of depth chart data to sync
        
        Returns:
            Success boolean
        """
        try:
            if not depth_data:
                logger.warning("No depth chart data to sync")
                return False
            
            db = SessionLocal()
            synced_count = 0
            
            try:
                for depth in depth_data:
                    # Check if depth chart record already exists
                    existing = db.query(DepthChart).filter(
                        DepthChart.player_id == depth.player_id,
                        DepthChart.week == depth.week,
                        DepthChart.season == depth.season,
                        DepthChart.formation == depth.formation
                    ).first()
                    
                    if existing:
                        # Update existing record
                        existing.depth_rank = depth.depth_rank
                        existing.updated_at = datetime.utcnow()
                    else:
                        # Create new record
                        new_depth = DepthChart(
                            player_id=depth.player_id,
                            team=depth.team,
                            position=depth.position,
                            depth_rank=depth.depth_rank,
                            week=depth.week,
                            season=depth.season,
                            formation=depth.formation
                        )
                        db.add(new_depth)
                    
                    synced_count += 1
                
                db.commit()
                logger.info(f"✓ Synced {synced_count} depth chart records to database")
                return True
                
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"Failed to sync depth chart data to database: {e}")
            return False
    
    def fetch_betting_lines(self, years: List[int] = None) -> pd.DataFrame:
        """
        Fetch betting lines data from NFL
        
        Args:
            years: List of years to fetch (defaults to current season)
        
        Returns:
            DataFrame with betting lines data
        """
        try:
            if years is None:
                years = [self.current_season]
            
            logger.info(f"Fetching betting lines for years: {years}")
            
            # Get betting lines data
            lines_df = nfl.import_sc_lines(years)
            
            if lines_df.empty:
                logger.warning(f"No betting lines data available for years: {years}")
                return pd.DataFrame()
            
            logger.info(f"✓ Fetched {len(lines_df)} betting lines records")
            return lines_df
            
        except Exception as e:
            logger.error(f"Failed to fetch betting lines: {e}")
            return pd.DataFrame()
    
    def build_betting_data_from_lines(self, week: int, season: int = None) -> List[NFLBettingData]:
        """
        Build betting data from NFL betting lines
        
        Args:
            week: NFL week to process
            season: NFL season (defaults to current)
        
        Returns:
            List of NFLBettingData objects
        """
        try:
            if season is None:
                season = self.current_season
            
            logger.info(f"Building betting data for week {week}, season {season}")
            
            # Fetch betting lines
            lines_df = self.fetch_betting_lines([season])
            
            if lines_df.empty:
                logger.warning(f"No betting lines data available for week {week}")
                return []
            
            # Filter by week
            if 'week' in lines_df.columns:
                lines_df = lines_df[lines_df['week'] == week]
            
            betting_data = []
            
            # Process each betting line
            for _, line_row in lines_df.iterrows():
                try:
                    # Get game info
                    game_id = line_row.get('game_id', f"{line_row.get('home_team', '')}_{line_row.get('away_team', '')}_{week}")
                    home_team = line_row.get('home_team', '')
                    away_team = line_row.get('away_team', '')
                    
                    if not all([home_team, away_team]):
                        continue
                    
                    # Get betting metrics
                    total_line = line_row.get('total', line_row.get('total_line'))
                    spread_line = line_row.get('spread', line_row.get('spread_line'))
                    
                    # Calculate implied totals
                    home_implied = None
                    away_implied = None
                    if total_line and spread_line:
                        try:
                            total_line = float(total_line)
                            spread_line = float(spread_line)
                            
                            # Spread is typically from home team perspective
                            # If spread is -3.5, home team is favored by 3.5
                            home_implied = (total_line - spread_line) / 2
                            away_implied = (total_line + spread_line) / 2
                        except (ValueError, TypeError):
                            pass
                    
                    # Build betting data
                    betting = NFLBettingData(
                        game_id=game_id,
                        home_team=home_team,
                        away_team=away_team,
                        week=week,
                        season=season,
                        total_line=float(total_line) if total_line else None,
                        spread_line=float(spread_line) if spread_line else None,
                        home_implied_total=home_implied,
                        away_implied_total=away_implied,
                        home_moneyline=line_row.get('home_moneyline'),
                        away_moneyline=line_row.get('away_moneyline'),
                        sportsbook=line_row.get('sportsbook', 'consensus')
                    )
                    
                    betting_data.append(betting)
                    
                except Exception as e:
                    logger.warning(f"Failed to process betting line for {line_row.get('game_id', 'unknown')}: {e}")
                    continue
            
            logger.info(f"✓ Built betting data for {len(betting_data)} games")
            return betting_data
            
        except Exception as e:
            logger.error(f"Failed to build betting data: {e}")
            return []
    
    def sync_betting_data_to_database(self, betting_data: List[NFLBettingData]) -> bool:
        """
        Sync betting data to the database
        
        Args:
            betting_data: List of betting data to sync
        
        Returns:
            Success boolean
        """
        try:
            if not betting_data:
                logger.warning("No betting data to sync")
                return False
            
            db = SessionLocal()
            synced_count = 0
            
            try:
                for betting in betting_data:
                    # Check if betting record already exists
                    existing = db.query(BettingLine).filter(
                        BettingLine.game_id == betting.game_id,
                        BettingLine.week == betting.week,
                        BettingLine.season == betting.season,
                        BettingLine.sportsbook == betting.sportsbook
                    ).first()
                    
                    if existing:
                        # Update existing record
                        existing.total_line = betting.total_line
                        existing.spread_line = betting.spread_line
                        existing.home_implied_total = betting.home_implied_total
                        existing.away_implied_total = betting.away_implied_total
                        existing.home_moneyline = betting.home_moneyline
                        existing.away_moneyline = betting.away_moneyline
                        existing.updated_at = datetime.utcnow()
                    else:
                        # Create new record
                        new_betting = BettingLine(
                            game_id=betting.game_id,
                            home_team=betting.home_team,
                            away_team=betting.away_team,
                            week=betting.week,
                            season=betting.season,
                            total_line=betting.total_line,
                            spread_line=betting.spread_line,
                            home_implied_total=betting.home_implied_total,
                            away_implied_total=betting.away_implied_total,
                            home_moneyline=betting.home_moneyline,
                            away_moneyline=betting.away_moneyline,
                            sportsbook=betting.sportsbook
                        )
                        db.add(new_betting)
                    
                    synced_count += 1
                
                db.commit()
                logger.info(f"✓ Synced {synced_count} betting records to database")
                return True
                
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"Failed to sync betting data to database: {e}")
            return False
    
    def fetch_schedules(self, years: List[int] = None) -> pd.DataFrame:
        """
        Fetch NFL schedule data
        
        Args:
            years: List of years to fetch (defaults to current season)
        
        Returns:
            DataFrame with schedule data
        """
        try:
            if years is None:
                years = [self.current_season]
            
            logger.info(f"Fetching NFL schedules for years: {years}")
            
            # Get schedule data
            schedule_df = nfl.import_schedules(years)
            
            if schedule_df.empty:
                logger.warning(f"No schedule data available for years: {years}")
                return pd.DataFrame()
            
            logger.info(f"✓ Fetched {len(schedule_df)} schedule records")
            return schedule_df
            
        except Exception as e:
            logger.error(f"Failed to fetch schedules: {e}")
            return pd.DataFrame()
    
    def build_schedule_data(self, season: int = None) -> List[Dict[str, Any]]:
        """
        Build schedule data from NFL schedules
        
        Args:
            season: NFL season (defaults to current)
        
        Returns:
            List of schedule data dictionaries
        """
        try:
            if season is None:
                season = self.current_season
            
            logger.info(f"Building schedule data for season {season}")
            
            # Fetch schedules
            schedule_df = self.fetch_schedules([season])
            
            if schedule_df.empty:
                logger.warning(f"No schedule data available for season {season}")
                return []
            
            schedule_data = []
            
            # Process each game
            for _, game_row in schedule_df.iterrows():
                try:
                    # Get game info
                    game_id = game_row.get('game_id', f"{game_row.get('home_team', '')}_{game_row.get('away_team', '')}_{game_row.get('week', '')}")
                    home_team = game_row.get('home_team', '')
                    away_team = game_row.get('away_team', '')
                    week = game_row.get('week', 0)
                    game_date = game_row.get('gameday', game_row.get('game_date'))
                    
                    if not all([home_team, away_team, week]):
                        continue
                    
                    # Parse game date if it's a string
                    parsed_date = None
                    if game_date:
                        try:
                            parsed_date = pd.to_datetime(game_date)
                        except:
                            pass
                    
                    # Build schedule data
                    schedule = {
                        'game_id': game_id,
                        'home_team': home_team,
                        'away_team': away_team,
                        'week': int(week),
                        'season': season,
                        'game_date': parsed_date,
                        'is_playoff': week > 18 if isinstance(week, (int, float)) else False,
                        'completed': game_row.get('result', '') != '' or game_row.get('home_score', 0) > 0
                    }
                    
                    schedule_data.append(schedule)
                    
                except Exception as e:
                    logger.warning(f"Failed to process schedule for {game_row.get('game_id', 'unknown')}: {e}")
                    continue
            
            logger.info(f"✓ Built schedule data for {len(schedule_data)} games")
            return schedule_data
            
        except Exception as e:
            logger.error(f"Failed to build schedule data: {e}")
            return []
    
    def sync_schedule_data_to_database(self, schedule_data: List[Dict[str, Any]]) -> bool:
        """
        Sync schedule data to the database
        
        Args:
            schedule_data: List of schedule data to sync
        
        Returns:
            Success boolean
        """
        try:
            if not schedule_data:
                logger.warning("No schedule data to sync")
                return False
            
            db = SessionLocal()
            synced_count = 0
            
            try:
                for schedule in schedule_data:
                    # Check if schedule record already exists
                    existing = db.query(NFLSchedule).filter(
                        NFLSchedule.game_id == schedule['game_id']
                    ).first()
                    
                    if existing:
                        # Update existing record
                        existing.home_team = schedule['home_team']
                        existing.away_team = schedule['away_team']
                        existing.week = schedule['week']
                        existing.season = schedule['season']
                        existing.game_date = schedule['game_date']
                        existing.is_playoff = schedule['is_playoff']
                        existing.completed = schedule['completed']
                        existing.updated_at = datetime.utcnow()
                    else:
                        # Create new record
                        new_schedule = NFLSchedule(
                            game_id=schedule['game_id'],
                            home_team=schedule['home_team'],
                            away_team=schedule['away_team'],
                            week=schedule['week'],
                            season=schedule['season'],
                            game_date=schedule['game_date'],
                            is_playoff=schedule['is_playoff'],
                            completed=schedule['completed']
                        )
                        db.add(new_schedule)
                    
                    synced_count += 1
                
                db.commit()
                logger.info(f"✓ Synced {synced_count} schedule records to database")
                return True
                
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"Failed to sync schedule data to database: {e}")
            return False
    
    def get_bye_weeks_for_teams(self, teams: List[str], season: int = None) -> Dict[str, int]:
        """
        Get bye weeks for specified teams
        
        Args:
            teams: List of team abbreviations
            season: NFL season (defaults to current)
        
        Returns:
            Dictionary mapping team to bye week
        """
        try:
            if season is None:
                season = self.current_season
            
            logger.info(f"Getting bye weeks for {len(teams)} teams in season {season}")
            
            # Get schedule data
            schedule_df = self.fetch_schedules([season])
            
            if schedule_df.empty:
                return {}
            
            bye_weeks = {}
            
            # For each team, find which weeks they don't play
            for team in teams:
                team_games = schedule_df[
                    (schedule_df['home_team'] == team) | 
                    (schedule_df['away_team'] == team)
                ]
                
                if not team_games.empty:
                    # Get all weeks they play
                    played_weeks = set(team_games['week'].tolist())
                    
                    # Find bye week (week 1-18 that they don't play)
                    for week in range(1, 19):
                        if week not in played_weeks:
                            bye_weeks[team] = week
                            break
            
            logger.info(f"✓ Found bye weeks for {len(bye_weeks)} teams")
            return bye_weeks
            
        except Exception as e:
            logger.error(f"Failed to get bye weeks: {e}")
            return {}
    
    def fetch_play_by_play_data(self, years: List[int] = None) -> pd.DataFrame:
        """
        Fetch play-by-play data from NFL
        
        Args:
            years: List of years to fetch (defaults to current season)
        
        Returns:
            DataFrame with play-by-play data
        """
        try:
            if years is None:
                years = [self.current_season]
            
            logger.info(f"Fetching play-by-play data for years: {years}")
            
            # Get play-by-play data
            pbp_df = nfl.import_pbp_data(years)
            
            if pbp_df.empty:
                logger.warning(f"No play-by-play data available for years: {years}")
                return pd.DataFrame()
            
            logger.info(f"✓ Fetched {len(pbp_df)} play-by-play records")
            return pbp_df
            
        except Exception as e:
            logger.error(f"Failed to fetch play-by-play data: {e}")
            return pd.DataFrame()
    
    def build_defensive_stats_from_pbp(self, week: int, season: int = None) -> List[Dict[str, Any]]:
        """
        Build defensive stats by aggregating play-by-play data
        
        Args:
            week: NFL week to process
            season: NFL season (defaults to current)
        
        Returns:
            List of defensive stats dictionaries
        """
        try:
            if season is None:
                season = self.current_season
            
            logger.info(f"Building defensive stats for week {week}, season {season}")
            
            # Fetch play-by-play data
            pbp_df = self.fetch_play_by_play_data([season])
            
            if pbp_df.empty:
                logger.warning(f"No play-by-play data available for week {week}")
                return []
            
            # Filter by week
            if 'week' in pbp_df.columns:
                pbp_df = pbp_df[pbp_df['week'] == week]
            
            if pbp_df.empty:
                logger.warning(f"No play-by-play data available for week {week}")
                return []
            
            defensive_stats = []
            
            # Get unique games
            games = pbp_df[['game_id', 'home_team', 'away_team']].drop_duplicates()
            
            for _, game in games.iterrows():
                try:
                    game_id = game['game_id']
                    home_team = game['home_team']
                    away_team = game['away_team']
                    
                    if not all([home_team, away_team]):
                        continue
                    
                    # Get plays for this game
                    game_plays = pbp_df[pbp_df['game_id'] == game_id]
                    
                    # Calculate defensive stats for home team (defending against away team offense)
                    home_def_stats = self._calculate_team_defensive_stats(
                        game_plays, home_team, away_team, week, season
                    )
                    if home_def_stats:
                        defensive_stats.append(home_def_stats)
                    
                    # Calculate defensive stats for away team (defending against home team offense)
                    away_def_stats = self._calculate_team_defensive_stats(
                        game_plays, away_team, home_team, week, season
                    )
                    if away_def_stats:
                        defensive_stats.append(away_def_stats)
                    
                except Exception as e:
                    logger.warning(f"Failed to process defensive stats for game {game.get('game_id', 'unknown')}: {e}")
                    continue
            
            logger.info(f"✓ Built defensive stats for {len(defensive_stats)} team-games")
            return defensive_stats
            
        except Exception as e:
            logger.error(f"Failed to build defensive stats: {e}")
            return []
    
    def _calculate_team_defensive_stats(self, game_plays: pd.DataFrame, defending_team: str, 
                                      offensive_team: str, week: int, season: int) -> Optional[Dict[str, Any]]:
        """Calculate defensive stats for a team in a specific game"""
        try:
            # Filter to plays where this team is defending (opponent has possession)
            opponent_plays = game_plays[game_plays['posteam'] == offensive_team]
            
            if opponent_plays.empty:
                return None
            
            # Initialize stats
            stats = {
                'team': defending_team,
                'week': week,
                'season': season,
                'opponent': offensive_team,
                'sacks_allowed': 0,
                'qb_hits_allowed': 0,
                'passing_yards_allowed': 0.0,
                'passing_tds_allowed': 0,
                'interceptions': 0,
                'pass_attempts_allowed': 0,
                'rushing_yards_allowed': 0.0,
                'rushing_tds_allowed': 0,
                'rush_attempts_allowed': 0,
                'epa_per_pass_allowed': None,
                'epa_per_rush_allowed': None,
                'success_rate_allowed': None,
                'red_zone_td_pct_allowed': None,
                'qb_streaming_rank': None,
                'dst_streaming_rank': None
            }
            
            # Passing defense stats
            pass_plays = opponent_plays[opponent_plays['play_type'] == 'pass']
            if not pass_plays.empty:
                stats['pass_attempts_allowed'] = len(pass_plays)
                stats['passing_yards_allowed'] = pass_plays['passing_yards'].fillna(0).sum()
                stats['passing_tds_allowed'] = (pass_plays['touchdown'] == 1).sum()
                stats['sacks_allowed'] = (pass_plays['sack'] == 1).sum()
                
                # EPA per pass (negative because it's from defensive perspective)
                pass_epa = pass_plays['epa'].fillna(0)
                if len(pass_epa) > 0:
                    stats['epa_per_pass_allowed'] = float(pass_epa.mean())
                
                # QB hits (if available)
                if 'qb_hit' in pass_plays.columns:
                    stats['qb_hits_allowed'] = (pass_plays['qb_hit'] == 1).sum()
            
            # Rushing defense stats
            rush_plays = opponent_plays[opponent_plays['play_type'] == 'run']
            if not rush_plays.empty:
                stats['rush_attempts_allowed'] = len(rush_plays)
                stats['rushing_yards_allowed'] = rush_plays['rushing_yards'].fillna(0).sum()
                stats['rushing_tds_allowed'] = (rush_plays['touchdown'] == 1).sum()
                
                # EPA per rush
                rush_epa = rush_plays['epa'].fillna(0)
                if len(rush_epa) > 0:
                    stats['epa_per_rush_allowed'] = float(rush_epa.mean())
            
            # Defensive turnovers
            if 'interception' in opponent_plays.columns:
                stats['interceptions'] = (opponent_plays['interception'] == 1).sum()
            
            # Success rate allowed (percentage of successful plays by opponent)
            if 'success' in opponent_plays.columns:
                success_plays = opponent_plays['success'].fillna(0)
                if len(success_plays) > 0:
                    stats['success_rate_allowed'] = float(success_plays.mean())
            
            # Red zone touchdown rate allowed
            if 'yardline_100' in opponent_plays.columns:
                rz_plays = opponent_plays[opponent_plays['yardline_100'] <= 20]
                if not rz_plays.empty:
                    rz_tds = (rz_plays['touchdown'] == 1).sum()
                    rz_td_rate = rz_tds / len(rz_plays) if len(rz_plays) > 0 else 0
                    stats['red_zone_td_pct_allowed'] = float(rz_td_rate)
            
            return stats
            
        except Exception as e:
            logger.warning(f"Failed to calculate defensive stats for {defending_team}: {e}")
            return None
    
    def sync_defensive_stats_to_database(self, defensive_stats: List[Dict[str, Any]]) -> bool:
        """
        Sync defensive stats to the database
        
        Args:
            defensive_stats: List of defensive stats to sync
        
        Returns:
            Success boolean
        """
        try:
            if not defensive_stats:
                logger.warning("No defensive stats to sync")
                return False
            
            db = SessionLocal()
            synced_count = 0
            
            try:
                for stats in defensive_stats:
                    # Check if defensive stats record already exists
                    existing = db.query(DefensiveStats).filter(
                        DefensiveStats.team == stats['team'],
                        DefensiveStats.week == stats['week'],
                        DefensiveStats.season == stats['season']
                    ).first()
                    
                    if existing:
                        # Update existing record
                        for key, value in stats.items():
                            if hasattr(existing, key):
                                setattr(existing, key, value)
                        existing.updated_at = datetime.utcnow()
                    else:
                        # Create new record
                        new_stats = DefensiveStats(**stats)
                        db.add(new_stats)
                    
                    synced_count += 1
                
                db.commit()
                logger.info(f"✓ Synced {synced_count} defensive stats records to database")
                return True
                
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"Failed to sync defensive stats to database: {e}")
            return False
    
    def _calculate_practice_participation(self, injury_row: Any) -> Optional[float]:
        """Calculate practice participation percentage from practice status"""
        try:
            practice_status = injury_row.get('practice_status', '')
            
            # Map practice status to percentage
            status_map = {
                'FP': 100.0,    # Full Participation
                'LP': 50.0,     # Limited Participation
                'DNP': 0.0,     # Did Not Participate
                'REST': 0.0,    # Rest day
                '': None        # No data
            }
            
            return status_map.get(practice_status.upper() if practice_status else '', None)
            
        except Exception as e:
            logger.warning(f"Failed to calculate practice participation: {e}")
            return None
    
    def _get_or_create_player(self, canonical_id: str, name: str, position: str, team: str) -> Optional[int]:
        """Get or create player in database and return player_id"""
        try:
            db = SessionLocal()
            try:
                # Look for existing player
                player = db.query(Player).filter(Player.nfl_id == canonical_id).first()
                
                if player:
                    return player.id
                
                # Create new player
                new_player = Player(
                    nfl_id=canonical_id,
                    name=name,
                    position=position,
                    team=team,
                    is_starter=position in ['QB', 'RB', 'WR', 'TE']  # Simple starter detection
                )
                
                db.add(new_player)
                db.commit()
                
                return new_player.id
                
            finally:
                db.close()
                
        except Exception as e:
            logger.warning(f"Failed to get/create player {name}: {e}")
            return None
    
    def _calculate_usage_metrics(self, usage: NFLUsageData, snap_row: Any, rec_stats: Any, rush_stats: Any) -> NFLUsageData:
        """Calculate derived usage metrics"""
        try:
            # Estimate target share (simplified - would need team totals for accuracy)
            if usage.targets and usage.targets > 0:
                usage.target_share = min(1.0, usage.targets / 35.0)  # Rough estimate
            
            # Estimate carry share (simplified)
            if usage.carries and usage.carries > 0:
                usage.carry_share = min(1.0, usage.carries / 25.0)  # Rough estimate
            
            # Estimate route percentage (based on position and snap percentage)
            if usage.position in ['WR', 'TE'] and usage.snap_pct:
                usage.route_pct = usage.snap_pct * 0.85  # Approximate route running rate
            
            return usage
            
        except Exception as e:
            logger.warning(f"Failed to calculate usage metrics for {usage.name}: {e}")
            return usage
    
    def _calculate_total_tds(self, rec_stats: Any, rush_stats: Any) -> Optional[int]:
        """Calculate total touchdowns from receiving and rushing stats"""
        # The current PFR weekly data doesn't include basic stats like TDs
        # This data is more about advanced metrics (broken tackles, etc.)
        # TDs would need to come from a different nfl-data-py function
        return None
    
    def _get_current_nfl_week(self) -> int:
        """Get current NFL week based on date"""
        now = datetime.now()
        if now.month >= 9:
            return min(max(now.isocalendar()[1] - 35, 1), 18)
        else:
            return 1

# Test function
def test_nfl_data_service():
    """Test the NFL data service"""
    print("Testing NFL Data Service...")
    print("=" * 60)
    
    service = NFLDataService()
    
    try:
        # Test snap counts fetch
        print("\n1. Testing snap counts fetch...")
        snap_df = service.fetch_snap_counts([2024], [1])  # Use 2024 data for testing
        print(f"   Fetched {len(snap_df)} snap count records")
        
        if not snap_df.empty:
            print(f"   Columns: {list(snap_df.columns)}")
            print(f"   Sample data:\n{snap_df.head(3)}")
        
        # Test advanced stats fetch
        print("\n2. Testing receiving stats fetch...")
        rec_df = service.fetch_weekly_advanced_stats('rec', [2024])
        print(f"   Fetched {len(rec_df)} receiving stat records")
        
        # Test building usage data
        print("\n3. Testing usage data building...")
        usage_data = service.build_usage_data_from_nfl_stats(1, 2024)  # Week 1, 2024
        print(f"   Built usage data for {len(usage_data)} players")
        
        if usage_data:
            sample = usage_data[0]
            print(f"   Sample usage: {sample.name} ({sample.position}, {sample.team})")
            print(f"     Snap %: {sample.snap_pct}")
            print(f"     Targets: {sample.targets}")
            print(f"     Carries: {sample.carries}")
        
        # Test database sync (with small subset)
        if usage_data:
            print("\n4. Testing database sync...")
            success = service.sync_usage_to_database(usage_data[:5])  # Sync first 5 players
            print(f"   Sync success: {success}")
        
        return True
        
    except Exception as e:
        print(f"   ✗ NFL data service test failed: {e}")
        return False

if __name__ == "__main__":
    test_nfl_data_service()
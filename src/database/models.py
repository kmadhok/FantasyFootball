from sqlalchemy import Column, Integer, String, DateTime, Boolean, Text, Float, ForeignKey, create_engine, Index, UniqueConstraint
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from datetime import datetime
import os

Base = declarative_base()

class Player(Base):
    __tablename__ = 'players'
    
    id = Column(Integer, primary_key=True)
    nfl_id = Column(String(50), unique=True, nullable=False)
    sleeper_id = Column(String(50), unique=True, nullable=True)
    mfl_id = Column(String(50), unique=True, nullable=True)
    espn_id = Column(String(50), unique=True, nullable=True)
    name = Column(String(100), nullable=False)
    position = Column(String(10), nullable=False)
    team = Column(String(10), nullable=False)
    depth_chart_position = Column(String(20), nullable=True)
    is_starter = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    roster_entries = relationship("RosterEntry", back_populates="player")
    news_items = relationship("NewsItem", back_populates="player")
    alerts = relationship("Alert", back_populates="player")
    waiver_claims = relationship("WaiverClaim", foreign_keys="[WaiverClaim.player_id]")
    dropped_claims = relationship("WaiverClaim", foreign_keys="[WaiverClaim.dropped_player_id]")
    usage_stats = relationship("PlayerUsage", back_populates="player")
    projections = relationship("PlayerProjections", back_populates="player")

class PlayerUsage(Base):
    __tablename__ = 'player_usage'
    
    id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey('players.id'), nullable=False)
    week = Column(Integer, nullable=False)
    season = Column(Integer, nullable=False, default=2025)
    snap_pct = Column(Float, nullable=True)
    route_pct = Column(Float, nullable=True)
    target_share = Column(Float, nullable=True)
    carry_share = Column(Float, nullable=True)
    rz_touches = Column(Integer, nullable=True)
    ez_targets = Column(Integer, nullable=True)
    targets = Column(Integer, nullable=True)
    carries = Column(Integer, nullable=True)
    receptions = Column(Integer, nullable=True)
    receiving_yards = Column(Float, nullable=True)
    rushing_yards = Column(Float, nullable=True)
    touchdowns = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    player = relationship("Player", back_populates="usage_stats")
    
    __table_args__ = (
        UniqueConstraint('player_id', 'week', 'season', name='unique_player_week_season_usage'),
        Index('idx_usage_week_season', 'week', 'season'),
        Index('idx_usage_player_week', 'player_id', 'week'),
        {'sqlite_autoincrement': True}
    )

class PlayerProjections(Base):
    __tablename__ = 'player_projections'
    
    id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey('players.id'), nullable=False)
    week = Column(Integer, nullable=False)
    season = Column(Integer, nullable=False, default=2025)
    projected_points = Column(Float, nullable=True)
    floor = Column(Float, nullable=True)
    ceiling = Column(Float, nullable=True)
    mean = Column(Float, nullable=True)
    stdev = Column(Float, nullable=True)
    source = Column(String(50), nullable=False, default='espn')
    scoring_format = Column(String(10), nullable=False, default='ppr')
    confidence = Column(Float, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships  
    player = relationship("Player", back_populates="projections")
    
    __table_args__ = (
        UniqueConstraint('player_id', 'week', 'season', 'source', name='unique_player_week_season_source'),
        Index('idx_proj_week_season', 'week', 'season'),
        Index('idx_proj_player_week', 'player_id', 'week'),
        Index('idx_proj_source', 'source'),
        {'sqlite_autoincrement': True}
    )

class RosterEntry(Base):
    __tablename__ = 'roster_entries'
    
    id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey('players.id'), nullable=False)
    platform = Column(String(20), nullable=False)  # 'sleeper' or 'mfl'
    league_id = Column(String(50), nullable=False)
    user_id = Column(String(50), nullable=False)
    roster_slot = Column(String(20), nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    player = relationship("Player", back_populates="roster_entries")

class RosterSnapshot(Base):
    __tablename__ = 'roster_snapshots'

    id = Column(Integer, primary_key=True)
    platform = Column(String(20), nullable=False)  # 'sleeper' or 'mfl'
    league_id = Column(String(50), nullable=False)
    team_id = Column(String(50), nullable=False)  # roster_id/owner_id (Sleeper) or franchise_id (MFL)
    player_id = Column(Integer, ForeignKey('players.id'), nullable=False)
    week = Column(Integer, nullable=False)
    season = Column(Integer, nullable=False, default=2025)
    slot = Column(String(20), nullable=True)
    synced_at = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    player = relationship("Player")

    __table_args__ = (
        UniqueConstraint('platform', 'league_id', 'team_id', 'week', 'player_id', name='uq_roster_snapshot_unique'),
        Index('idx_roster_snap_league_week', 'league_id', 'week'),
        Index('idx_roster_snap_team_week', 'team_id', 'week'),
        {'sqlite_autoincrement': True}
    )

class WaiverState(Base):
    __tablename__ = 'waiver_states'
    
    id = Column(Integer, primary_key=True)
    platform = Column(String(20), nullable=False)  # 'sleeper' or 'mfl'
    league_id = Column(String(50), nullable=False)
    user_id = Column(String(50), nullable=False)
    waiver_order = Column(Integer, nullable=True)  # For Sleeper waiver priority
    faab_balance = Column(Float, nullable=True)  # For MFL FAAB balance
    initial_faab = Column(Float, nullable=True)  # Starting FAAB balance for calculations
    waiver_budget_used = Column(Float, default=0.0)  # Amount of FAAB used
    priority_type = Column(String(20), nullable=True)  # 'rolling', 'reset', 'faab'
    is_active = Column(Boolean, default=True)  # Whether user is actively in waiver system
    last_waiver_claim = Column(DateTime, nullable=True)  # Last successful claim
    total_claims = Column(Integer, default=0)  # Total number of waiver claims made
    successful_claims = Column(Integer, default=0)  # Number of successful claims
    timestamp = Column(DateTime, default=datetime.utcnow)  # When this state was recorded
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        # Unique constraint on platform, league_id, user_id combination for current state
        UniqueConstraint('platform', 'league_id', 'user_id', name='unique_waiver_state'),
        # Index for efficient querying
        Index('idx_waiver_platform_league', 'platform', 'league_id'),
        Index('idx_waiver_timestamp', 'timestamp'),
        {'sqlite_autoincrement': True}
    )

class WaiverClaim(Base):
    __tablename__ = 'waiver_claims'
    
    id = Column(Integer, primary_key=True)
    platform = Column(String(20), nullable=False)  # 'sleeper' or 'mfl'
    league_id = Column(String(50), nullable=False)
    user_id = Column(String(50), nullable=False)
    player_id = Column(Integer, ForeignKey('players.id'), nullable=True)  # Player being claimed
    claim_type = Column(String(20), nullable=False)  # 'add', 'drop', 'add_drop'
    waiver_order_at_claim = Column(Integer, nullable=True)  # Waiver order when claim was made
    faab_bid = Column(Float, nullable=True)  # FAAB bid amount
    status = Column(String(20), nullable=False)  # 'pending', 'successful', 'failed', 'cancelled'
    processed_at = Column(DateTime, nullable=True)  # When the claim was processed
    priority = Column(Integer, nullable=True)  # Priority within the waiver period
    dropped_player_id = Column(Integer, ForeignKey('players.id'), nullable=True)  # Player dropped in add/drop
    error_reason = Column(String(200), nullable=True)  # Reason for failed claim
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    player = relationship("Player", foreign_keys=[player_id])
    dropped_player = relationship("Player", foreign_keys=[dropped_player_id])
    
    __table_args__ = (
        # Index for efficient querying
        Index('idx_claim_platform_league', 'platform', 'league_id'),
        Index('idx_claim_user_status', 'user_id', 'status'),
        Index('idx_claim_processed', 'processed_at'),
        {'sqlite_autoincrement': True}
    )

class NewsItem(Base):
    __tablename__ = 'news_items'
    
    id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey('players.id'), nullable=False)
    headline = Column(String(500), nullable=False)
    headline_hash = Column(String(64), nullable=False)  # SHA256 hash for deduplication
    content = Column(Text, nullable=True)
    source = Column(String(50), nullable=False)  # 'reddit', 'twitter', 'espn', 'sleeper'
    source_url = Column(String(500), nullable=True)
    source_weight = Column(Float, default=1.0)
    keywords_matched = Column(Text, nullable=True)  # JSON string of matched keywords
    confidence_score = Column(Float, default=0.0)
    event_type = Column(String(50), nullable=True)  # 'injury', 'trade', 'promotion', etc.
    created_at = Column(DateTime, default=datetime.utcnow)
    processed_at = Column(DateTime, nullable=True)
    
    # Relationships
    player = relationship("Player", back_populates="news_items")
    alerts = relationship("Alert", back_populates="news_item")

class Alert(Base):
    __tablename__ = 'alerts'
    
    id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey('players.id'), nullable=False)
    news_item_id = Column(Integer, ForeignKey('news_items.id'), nullable=False)
    alert_type = Column(String(50), nullable=False)  # 'roster_player', 'starter_promotion'
    is_rostered = Column(Boolean, default=False)
    waiver_info = Column(Text, nullable=True)  # JSON string with comprehensive waiver context
    roster_context = Column(Text, nullable=True)  # JSON string with who has the player rostered
    waiver_recommendation = Column(String(100), nullable=True)  # 'high_priority', 'medium_priority', 'low_priority'
    faab_suggestion = Column(Float, nullable=True)  # Suggested FAAB bid amount
    waiver_urgency = Column(String(20), nullable=True)  # 'immediate', 'next_cycle', 'monitor'
    sent_at = Column(DateTime, nullable=True)
    delivery_status = Column(String(20), default='pending')  # 'pending', 'sent', 'failed'
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    player = relationship("Player", back_populates="alerts")
    news_item = relationship("NewsItem", back_populates="alerts")

class DeduplicationLog(Base):
    __tablename__ = 'deduplication_logs'
    
    id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey('players.id'), nullable=False)
    headline_hash = Column(String(64), nullable=False)
    first_seen = Column(DateTime, default=datetime.utcnow)
    last_seen = Column(DateTime, default=datetime.utcnow)
    occurrence_count = Column(Integer, default=1)
    
    # For 24-hour deduplication window
    expires_at = Column(DateTime, nullable=False)

class PlayerInjuryReport(Base):
    __tablename__ = 'player_injury_reports'
    
    id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey('players.id'), nullable=False)
    week = Column(Integer, nullable=False)
    season = Column(Integer, nullable=False, default=2025)
    report_status = Column(String(20), nullable=True)  # 'Out', 'IR', 'Doubtful', 'Questionable', 'Probable'
    practice_status = Column(String(10), nullable=True)  # 'DNP', 'LP', 'FP' (Did Not Participate, Limited, Full)
    practice_participation_pct = Column(Float, nullable=True)  # Calculated from daily practice status
    injury_description = Column(String(200), nullable=True)
    days_on_report = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    player = relationship("Player")
    
    __table_args__ = (
        UniqueConstraint('player_id', 'week', 'season', name='unique_player_injury_week_season'),
        Index('idx_injury_week_season', 'week', 'season'),
        Index('idx_injury_status', 'report_status'),
        Index('idx_injury_player_week', 'player_id', 'week'),
        {'sqlite_autoincrement': True}
    )

class DepthChart(Base):
    __tablename__ = 'depth_charts'
    
    id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey('players.id'), nullable=False)
    team = Column(String(10), nullable=False)
    position = Column(String(10), nullable=False)
    depth_rank = Column(Integer, nullable=False)  # 1, 2, 3, etc. (1 = starter)
    week = Column(Integer, nullable=False)
    season = Column(Integer, nullable=False, default=2025)
    formation = Column(String(20), nullable=True)  # '11 Personnel', '12 Personnel', etc.
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    player = relationship("Player")
    
    __table_args__ = (
        UniqueConstraint('player_id', 'week', 'season', 'formation', name='unique_player_depth_week_season_formation'),
        Index('idx_depth_team_pos_rank', 'team', 'position', 'depth_rank'),
        Index('idx_depth_week_season', 'week', 'season'),
        Index('idx_depth_player_week', 'player_id', 'week'),
        {'sqlite_autoincrement': True}
    )

class BettingLine(Base):
    __tablename__ = 'betting_lines'
    
    id = Column(Integer, primary_key=True)
    game_id = Column(String(50), nullable=False)  # NFL game identifier
    home_team = Column(String(10), nullable=False)
    away_team = Column(String(10), nullable=False)
    week = Column(Integer, nullable=False)
    season = Column(Integer, nullable=False, default=2025)
    total_line = Column(Float, nullable=True)  # Over/under total
    spread_line = Column(Float, nullable=True)  # Point spread (negative = favorite)
    home_moneyline = Column(Integer, nullable=True)
    away_moneyline = Column(Integer, nullable=True)
    home_implied_total = Column(Float, nullable=True)  # Calculated from total + spread
    away_implied_total = Column(Float, nullable=True)  # Calculated from total + spread
    sportsbook = Column(String(50), default='consensus')  # Data source
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        UniqueConstraint('game_id', 'week', 'season', 'sportsbook', name='unique_game_week_season_book'),
        Index('idx_betting_week_season', 'week', 'season'),
        Index('idx_betting_teams', 'home_team', 'away_team'),
        Index('idx_betting_total', 'total_line'),
        {'sqlite_autoincrement': True}
    )

class NFLSchedule(Base):
    __tablename__ = 'nfl_schedule'
    
    id = Column(Integer, primary_key=True)
    game_id = Column(String(50), nullable=False, unique=True)
    home_team = Column(String(10), nullable=False)
    away_team = Column(String(10), nullable=False)
    week = Column(Integer, nullable=False)
    season = Column(Integer, nullable=False, default=2025)
    game_date = Column(DateTime, nullable=True)
    is_playoff = Column(Boolean, default=False)
    completed = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_schedule_week_season', 'week', 'season'),
        Index('idx_schedule_team_week', 'home_team', 'week'),
        Index('idx_schedule_team_week_away', 'away_team', 'week'),
        Index('idx_schedule_date', 'game_date'),
        {'sqlite_autoincrement': True}
    )

class DefensiveStats(Base):
    __tablename__ = 'defensive_stats'
    
    id = Column(Integer, primary_key=True)
    team = Column(String(10), nullable=False)
    week = Column(Integer, nullable=False)
    season = Column(Integer, nullable=False, default=2025)
    opponent = Column(String(10), nullable=False)
    
    # Passing defense
    sacks_allowed = Column(Integer, default=0)
    qb_hits_allowed = Column(Integer, default=0)
    passing_yards_allowed = Column(Float, default=0.0)
    passing_tds_allowed = Column(Integer, default=0)
    interceptions = Column(Integer, default=0)
    pass_attempts_allowed = Column(Integer, default=0)
    
    # Rushing defense  
    rushing_yards_allowed = Column(Float, default=0.0)
    rushing_tds_allowed = Column(Integer, default=0)
    rush_attempts_allowed = Column(Integer, default=0)
    
    # Advanced metrics
    epa_per_pass_allowed = Column(Float, nullable=True)  # Expected Points Added per pass
    epa_per_rush_allowed = Column(Float, nullable=True)  # Expected Points Added per rush
    success_rate_allowed = Column(Float, nullable=True)  # Successful play rate allowed
    red_zone_td_pct_allowed = Column(Float, nullable=True)  # RZ touchdown rate allowed
    
    # Derived metrics for streaming
    qb_streaming_rank = Column(Integer, nullable=True)  # 1-32 rank for QB streaming
    dst_streaming_rank = Column(Integer, nullable=True)  # 1-32 rank for DST streaming
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        UniqueConstraint('team', 'week', 'season', name='unique_team_defensive_week_season'),
        Index('idx_def_week_season', 'week', 'season'),
        Index('idx_def_team_week', 'team', 'week'),
        Index('idx_def_streaming_qb', 'qb_streaming_rank'),
        Index('idx_def_streaming_dst', 'dst_streaming_rank'),
        {'sqlite_autoincrement': True}
    )

class WaiverCandidates(Base):
    __tablename__ = 'waiver_candidates'
    
    # Composite primary key for materialized view
    league_id = Column(String(50), nullable=False, primary_key=True)
    week = Column(Integer, nullable=False, primary_key=True)
    player_id = Column(Integer, ForeignKey('players.id'), nullable=False, primary_key=True)
    
    # Basic player info (for fast queries without joins)
    pos = Column(String(10), nullable=False)
    rostered = Column(Boolean, nullable=False, default=False)
    
    # Week-over-week deltas (Epic A requirement)
    snap_delta = Column(Float, nullable=True)  # snap_pct change from previous week
    route_delta = Column(Float, nullable=True)  # route_pct change from previous week
    
    # Advanced metrics (Epic A requirement)
    tprr = Column(Float, nullable=True)  # targets per route run
    rz_last2 = Column(Integer, nullable=True)  # red zone touches last 2 games
    ez_last2 = Column(Integer, nullable=True)  # end zone targets last 2 games
    
    # Schedule and projections
    opp_next = Column(String(10), nullable=True)  # opponent next week
    proj_next = Column(Float, nullable=True)  # next week projection
    
    # Trend analysis (Epic A requirement)
    trend_slope = Column(Float, nullable=True)  # 3-week trend slope
    
    # League context (Epic A requirement)
    roster_fit = Column(Float, nullable=True)  # fit for user's roster needs
    market_heat = Column(Float, nullable=True)  # interest from other teams
    scarcity = Column(Float, nullable=True)  # positional scarcity in league
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    player = relationship("Player")
    
    __table_args__ = (
        # Indexes for fast querying (< 1 minute requirement)
        Index('idx_wc_league_week', 'league_id', 'week'),
        Index('idx_wc_league_pos', 'league_id', 'pos'),
        Index('idx_wc_rostered', 'league_id', 'week', 'rostered'),
        Index('idx_wc_proj_next', 'league_id', 'week', 'proj_next'),
        {'sqlite_autoincrement': True}
    )

# Database configuration
from ..config import get_database_url, ensure_data_directory

# Ensure data directory exists before creating database
ensure_data_directory()

DATABASE_URL = get_database_url()
engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    """Get database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def create_tables():
    """Create all tables"""
    # Ensure data directory exists before creating tables
    ensure_data_directory()
    Base.metadata.create_all(bind=engine)

def drop_tables():
    """Drop all tables (for testing)"""
    Base.metadata.drop_all(bind=engine)

def initialize_database():
    """Initialize the database by creating all tables if they don't exist"""
    try:
        ensure_data_directory()
        create_tables()
        return True
    except Exception as e:
        print(f"Failed to initialize database: {e}")
        return False

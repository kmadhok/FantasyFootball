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
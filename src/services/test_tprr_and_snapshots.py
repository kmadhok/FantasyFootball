import pytest
from datetime import datetime

from src.database import SessionLocal, create_tables, drop_tables
from src.database import Player, PlayerUsage, RosterSnapshot
from src.services.enhanced_waiver_candidates_builder import EnhancedWaiverCandidatesBuilder
from src.database.roster_storage import RosterStorageService


def setup_module(module):
    # Ensure tables exist
    create_tables()


def teardown_module(module):
    # Do not drop global tables; just cleanup our inserts in tests
    pass


def test_tprr_calculation_basic():
    db = SessionLocal()
    try:
        # Create player
        player = Player(nfl_id="NFL_TEST1234", name="Test WR", position="WR", team="DEN")
        db.add(player)
        db.commit()
        db.refresh(player)

        # Insert two weeks of usage with route_pct on 0-1 scale
        for w, (targets, route_pct) in enumerate([(6, 0.6), (8, 0.7)], start=1):
            db.add(
                PlayerUsage(
                    player_id=player.id,
                    week=w,
                    season=2025,
                    targets=targets,
                    route_pct=route_pct,
                )
            )
        db.commit()

        builder = EnhancedWaiverCandidatesBuilder()
        # Access internal method via known signature
        tprr = builder._calculate_tprr(db, player.id, week=2)
        assert tprr is not None
        # With est 35 routes per game: (6+8)/(0.6*35 + 0.7*35) ~ 14/(45.5) ~ 0.307
        assert 0.25 < tprr < 0.40

    finally:
        # Cleanup
        db.query(PlayerUsage).filter(PlayerUsage.player_id == player.id).delete()
        db.query(Player).filter(Player.id == player.id).delete()
        db.commit()
        db.close()


def test_roster_snapshot_upsert_idempotent():
    db = SessionLocal()
    try:
        # Create a player to reference
        player = Player(nfl_id="NFL_SNAP0001", name="Snap Guy", position="RB", team="DAL")
        db.add(player)
        db.commit()
        db.refresh(player)

        svc = RosterStorageService()
        ok1 = svc.upsert_roster_snapshot(
            platform="sleeper",
            league_id="L1",
            team_id="T1",
            player_id=player.id,
            week=1,
            season=2025,
            slot="BN",
        )
        assert ok1

        ok2 = svc.upsert_roster_snapshot(
            platform="sleeper",
            league_id="L1",
            team_id="T1",
            player_id=player.id,
            week=1,
            season=2025,
            slot="FLEX",
        )
        assert ok2

        snaps = (
            db.query(RosterSnapshot)
            .filter(
                RosterSnapshot.platform == "sleeper",
                RosterSnapshot.league_id == "L1",
                RosterSnapshot.team_id == "T1",
                RosterSnapshot.week == 1,
                RosterSnapshot.player_id == player.id,
            )
            .all()
        )
        assert len(snaps) == 1
        assert snaps[0].slot == "FLEX"

    finally:
        db.query(RosterSnapshot).delete()
        db.query(Player).filter(Player.id == player.id).delete()
        db.commit()
        db.close()


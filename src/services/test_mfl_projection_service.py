from unittest.mock import patch

from src.database import SessionLocal, create_tables
from src.database import Player, PlayerProjections
from src.services.mfl_projection_service import MFLProjectionService


def setup_module(module):
    create_tables()


def test_mfl_projections_mapping_monkeypatched():
    db = SessionLocal()
    try:
        # Create a player with an MFL ID that matches the mocked projection
        p = Player(nfl_id="NFL_PROJ0001", name="Proj Guy", position="WR", team="BUF", mfl_id="9999")
        db.add(p)
        db.commit()
        db.refresh(p)

        svc = MFLProjectionService()

        fake_items = [
            {"id": "9999", "score": "10.5"},
            {"id": "0000", "score": "8.0"},  # Unknown
        ]

        with patch.object(MFLProjectionService, "fetch_weekly_projections", return_value=fake_items):
            ok = svc.sync_projections_to_database(week=1)
            assert ok

        proj = (
            db.query(PlayerProjections)
            .filter(
                PlayerProjections.player_id == p.id,
                PlayerProjections.week == 1,
                PlayerProjections.source == "mfl",
            )
            .first()
        )
        assert proj is not None
        assert abs(proj.projected_points - 10.5) < 1e-6

    finally:
        db.query(PlayerProjections).delete()
        db.query(Player).delete()
        db.commit()
        db.close()


import logging
from typing import List, Dict, Optional
from datetime import datetime

import requests

from src.config.config import get_config
from src.database import SessionLocal, Player, PlayerProjections
from src.utils.retry_handler import handle_api_request, APIError

logger = logging.getLogger(__name__)


class MFLProjectionService:
    """Fetch and sync weekly projections from MyFantasyLeague (MFL).

    Notes:
    - Endpoint shape varies by MFL configuration. We target the common export endpoint:
      https://api.myfantasyleague.com/{season}/export?TYPE=projectedScores&L={leagueId}&W={week}&JSON=1
    - Fallbacks and lenient parsing are included to avoid hard failures on shape drift.
    """

    def __init__(self) -> None:
        self.config = get_config()
        self.season = 2025
        self.league_id = self.config.MFL_LEAGUE_ID
        self.base_url = f"https://api.myfantasyleague.com/{self.season}/export"
        self.timeout = 15

    @handle_api_request
    def fetch_weekly_projections(self, week: Optional[int] = None) -> List[Dict]:
        if week is None:
            week = self._current_week()

        params = {
            "TYPE": "projectedScores",
            "L": self.league_id,
            "W": week,
            "JSON": "1",
        }

        try:
            resp = requests.get(self.base_url, params=params, timeout=self.timeout)
            resp.raise_for_status()
            data = resp.json() or {}

            # Typical structure: { "projectedScores": { "playerScore": [ {"id": "1234", "score": "12.3"}, ... ] } }
            proj_container = data.get("projectedScores") or data.get("playerScores") or {}
            items = proj_container.get("playerScore") or proj_container.get("playerScores") or []

            if not isinstance(items, list):
                items = [items] if items else []

            logger.info(f"Fetched {len(items)} MFL projected scores for week {week}")
            return items
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch MFL projections: {e}")
            raise APIError(
                f"MFL projections request failed: {e}",
                status_code=getattr(e.response, "status_code", None) if hasattr(e, "response") else None,
                platform="mfl",
            )

    def sync_projections_to_database(self, week: Optional[int] = None) -> bool:
        """Upsert PlayerProjections (source='mfl', scoring_format='ppr')."""
        try:
            items = self.fetch_weekly_projections(week)
            if not items:
                logger.warning("No MFL projections returned; skipping DB sync")
                return False

            if week is None:
                week = self._current_week()

            db = SessionLocal()
            synced = 0
            try:
                for item in items:
                    # MFL uses 'id' for player id and 'score'/'projectedScore' for projected points
                    pid = str(item.get("id") or item.get("playerId") or "").strip()
                    if not pid:
                        continue

                    # Look up player by MFL ID
                    player = db.query(Player).filter(Player.mfl_id == pid).first()
                    if not player:
                        continue  # skip unknowns; unification should map these before

                    raw_proj = item.get("score") or item.get("projectedScore") or item.get("value")
                    try:
                        projected_points = float(raw_proj) if raw_proj is not None else None
                    except (ValueError, TypeError):
                        projected_points = None

                    projection = (
                        db.query(PlayerProjections)
                        .filter(
                            PlayerProjections.player_id == player.id,
                            PlayerProjections.week == week,
                            PlayerProjections.season == self.season,
                            PlayerProjections.source == "mfl",
                        )
                        .first()
                    )

                    if projection:
                        projection.projected_points = projected_points
                        projection.mean = projected_points
                        projection.updated_at = datetime.utcnow()
                    else:
                        projection = PlayerProjections(
                            player_id=player.id,
                            week=week,
                            season=self.season,
                            projected_points=projected_points,
                            mean=projected_points,
                            source="mfl",
                            scoring_format="ppr",
                        )
                        db.add(projection)

                    synced += 1

                db.commit()
                logger.info(f"Synced {synced} MFL projections to database for week {week}")
                return True
            except Exception as e:
                db.rollback()
                logger.error(f"DB error during MFL projections sync: {e}")
                return False
            finally:
                db.close()
        except APIError:
            return False

    def _current_week(self) -> int:
        now = datetime.now()
        if now.month >= 9:
            return min(max(now.isocalendar()[1] - 35, 1), 18)
        return 1


def test_mfl_projection_sync() -> bool:
    print("Testing MFL Projection Service...")
    svc = MFLProjectionService()
    try:
        ok = svc.sync_projections_to_database()
        print(f"  Sync result: {ok}")
        return True
    except Exception as e:
        print(f"  ✗ MFL projection sync failed: {e}")
        return False


if __name__ == "__main__":
    test_mfl_projection_sync()


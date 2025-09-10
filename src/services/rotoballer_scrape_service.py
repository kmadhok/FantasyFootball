import logging
import re
import sys
from io import StringIO
from typing import List, Optional, Dict, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup

from src.database import SessionLocal, Player, PlayerRankings
from src.utils.player_id_mapper import PlayerIDMapper
from src.config.config import get_config

logger = logging.getLogger(__name__)


class RotoBallerRankingService:
    def __init__(self) -> None:
        self.session = requests.Session()
        cfg = get_config()
        self.season = cfg.get_current_season_year()
        # Reasonable defaults
        self.session.headers.update({
            "User-Agent": cfg.YAHOO_USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })
        self.mapper = PlayerIDMapper()

    def fetch_html(self, url: str) -> str:
        resp = self.session.get(url, timeout=25)
        resp.raise_for_status()
        return resp.text

    def parse_tables(self, html: str) -> List[pd.DataFrame]:
        # Try pandas first
        try:
            return pd.read_html(StringIO(html))
        except ValueError:
            # No tables found via pandas; try extracting a known container
            soup = BeautifulSoup(html, 'lxml')
            tables = soup.find_all('table')
            dfs = []
            for t in tables:
                try:
                    dfs.append(pd.read_html(str(t))[0])
                except Exception:
                    continue
            return dfs

    def select_ranking_table(self, dfs: List[pd.DataFrame]) -> Optional[pd.DataFrame]:
        # Heuristic: choose a table containing 'Player' and either 'POS'/'Position' and possibly 'Tier'/'Rank'
        best = None
        best_cols = 0
        for df in dfs:
            cols = [str(c).strip().lower() for c in df.columns]
            score = 0
            if any('player' in c for c in cols):
                score += 1
            if any(c in ('pos', 'position') for c in cols):
                score += 1
            if any('tier' in c for c in cols):
                score += 1
            if any('rank' in c or c == '#' for c in cols):
                score += 1
            if score > best_cols:
                best = df
                best_cols = score
        return best

    def _normalize_player_row(self, row: pd.Series) -> Tuple[Optional[str], str, str, Optional[int], Optional[str]]:
        # Extract name, team, position, optional rank and tier
        name_val = row.get('Player') or row.get('PLAYER') or row.get('Name') or row.get('Player Name')
        if pd.isna(name_val):
            return None, 'FA', 'UNK', None, None
        name_text = str(name_val)

        # Often appears as "Name Team - POS" or includes extra notes; normalize
        name_text = ' '.join(name_text.split())
        m = re.match(r"^(?P<name>.+?)\s+(?P<team>[A-Za-z]{2,3})\s*-\s*(?P<pos>[A-Za-z/]+)$", name_text)
        if m:
            name = m.group('name').strip()
            team = m.group('team').upper()
            pos = m.group('pos').upper()
        else:
            # Try separate columns
            pos = str(row.get('POS') or row.get('Position') or 'UNK').upper()
            team = str(row.get('Team') or row.get('TM') or 'FA').upper()
            name = name_text

        # Rank/tier
        tier = None
        if 'Tier' in row.index:
            tier = str(row.get('Tier')) if not pd.isna(row.get('Tier')) else None
        rank_val = row.get('Rank') or row.get('#')
        try:
            rank = int(rank_val) if not pd.isna(rank_val) else None
        except Exception:
            rank = None

        return name, team, pos, rank, tier

    def sync_rankings(self, url: str, top_n: int = 150) -> Dict[str, int]:
        html = self.fetch_html(url)
        dfs = self.parse_tables(html)
        if not dfs:
            logger.warning("No tables found on RotoBaller page")
            return {"rows_parsed": 0, "rankings_synced": 0}

        table = self.select_ranking_table(dfs)
        if table is None or table.empty:
            logger.warning("No suitable ranking table found on RotoBaller page")
            return {"rows_parsed": 0, "rankings_synced": 0}

        # Standardize column names for convenience
        table.columns = [str(c).strip().replace('\n', ' ') for c in table.columns]

        count = 0
        synced = 0
        db = SessionLocal()
        try:
            for _, row in table.iterrows():
                name, team, pos, rank, tier = self._normalize_player_row(row)
                if not name:
                    continue
                pos = self.mapper.normalize_position(pos)
                if pos not in ("QB", "RB", "WR", "TE"):
                    continue
                team = self.mapper.normalize_team(team)
                name = self.mapper.normalize_player_name(name)

                # Create/find player
                player = (
                    db.query(Player)
                    .filter(Player.name.ilike(name), Player.position == pos, Player.team == team)
                    .first()
                )
                if not player:
                    canonical = self.mapper.generate_canonical_id(name, pos, team)
                    player = Player(nfl_id=canonical, name=name, position=pos, team=team)
                    db.add(player)
                    db.flush()

                # Upsert ranking
                pr = (
                    db.query(PlayerRankings)
                    .filter(
                        PlayerRankings.player_id == player.id,
                        PlayerRankings.season == self.season,
                        PlayerRankings.source == 'rotoballer',
                    ).first()
                )
                if pr:
                    pr.rank = rank
                    pr.tier = tier
                    pr.position = pos
                else:
                    pr = PlayerRankings(
                        player_id=player.id,
                        season=self.season,
                        source='rotoballer',
                        rank=rank,
                        tier=tier,
                        position=pos,
                    )
                    db.add(pr)
                synced += 1
                count += 1
                if count >= top_n:
                    break
            db.commit()
            logger.info(f"RotoBaller rankings synced: rows={count}, upserts={synced}")
            return {"rows_parsed": count, "rankings_synced": synced}
        except Exception as e:
            db.rollback()
            logger.error(f"Failed to sync RotoBaller rankings: {e}")
            return {"rows_parsed": count, "rankings_synced": synced}
        finally:
            db.close()


def main(argv: List[str]) -> int:
    import argparse
    parser = argparse.ArgumentParser(description="Sync RotoBaller PPR rankings (Overall)")
    parser.add_argument('--url', required=False, default='https://www.rotoballer.com/nfl-fantasy-football-rankings-tiered-ppr/265860', help='RotoBaller rankings URL')
    parser.add_argument('--top', type=int, default=150, help='Limit number of rows')
    args = parser.parse_args(argv)

    svc = RotoBallerRankingService()
    out = svc.sync_rankings(args.url, top_n=args.top)
    print(out)
    return 0


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))


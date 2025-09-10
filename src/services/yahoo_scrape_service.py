import logging
import re
import sys
import urllib.parse as urlparse
from dataclasses import dataclass
from io import StringIO
from typing import List, Optional, Dict, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup

from src.config.config import get_config
from src.database import SessionLocal, Player, PlayerProjections
from src.utils.player_id_mapper import PlayerIDMapper

logger = logging.getLogger(__name__)


@dataclass
class YahooFA:
    name: str
    position: str
    team: str
    projected: Optional[float]


class YahooFreeAgentService:
    def __init__(self) -> None:
        self.config = get_config()
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": self.config.YAHOO_USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })
        # Optional Cookie header if user pasted it into .env
        if self.config.YAHOO_COOKIE:
            self.session.headers.update({"Cookie": self.config.YAHOO_COOKIE})

        self.mapper = PlayerIDMapper()
        self.season = self.config.get_current_season_year()

    def _infer_week_from_url(self, url: str) -> Optional[int]:
        try:
            qs = urlparse.parse_qs(urlparse.urlparse(url).query)
            stat1 = qs.get('stat1', [''])[0]
            # Expected like S_PW_1 (Projected Week 1)
            m = re.match(r"S_PW_(\d+)", stat1 or '')
            if m:
                return int(m.group(1))
            return None
        except Exception:
            return None

    def fetch_html(self, url: str) -> str:
        # Mimic XHR update Yahoo uses; still returns HTML fragment with rows
        hdrs = {
            "X-Requested-With": "XMLHttpRequest",
            "ajax-request": "true",
            "Referer": url,
        }
        resp = self.session.get(url, headers=hdrs, timeout=20, allow_redirects=True)
        resp.raise_for_status()
        return resp.text

    def parse_free_agents(self, html: str) -> List[YahooFA]:
        # Try pandas first (robust table extraction)
        try:
            tables = pd.read_html(StringIO(html))
            # Find a table that contains a Player column
            target = None
            for t in tables:
                cols = [str(c).lower() for c in t.columns]
                if any('player' in c for c in cols):
                    target = t
                    break
            if target is None:
                raise ValueError("No player table found via pandas.read_html")

            # Identify projected/points column
            proj_col = None
            for c in target.columns:
                cl = str(c).lower()
                if 'proj' in cl or cl == 'pts' or 'points' in cl:
                    proj_col = c
                    break

            fas: List[YahooFA] = []
            for _, row in target.iterrows():
                raw_player = str(row.get('Player') or row.get('PLAYER') or '')
                if not raw_player or raw_player.strip() == '' or raw_player.strip().lower() == 'player':
                    continue
                # Example pattern: "Chris Evans Cin - RB"
                name, team, pos = self._split_player_cell(raw_player)
                if not name:
                    continue
                projected = None
                if proj_col is not None:
                    try:
                        projected = float(row[proj_col])
                    except Exception:
                        projected = None
                fas.append(YahooFA(name=name, position=pos, team=team, projected=projected))
            return fas
        except Exception:
            # Fallback to BeautifulSoup parsing
            soup = BeautifulSoup(html, 'lxml')
            fas: List[YahooFA] = []
            for tr in soup.find_all('tr'):
                tds = tr.find_all('td')
                if not tds:
                    continue
                # Heuristic: first cell contains player info
                text = ' '.join(tds[0].get_text(strip=True).split())
                name, team, pos = self._split_player_cell(text)
                if not name:
                    continue
                projected = None
                # Try to parse numeric from any cell that looks like points
                for td in tds[1:5]:
                    val = td.get_text(strip=True)
                    try:
                        projected = float(val)
                        break
                    except Exception:
                        continue
                fas.append(YahooFA(name=name, position=pos, team=team, projected=projected))
            return fas

    def _split_player_cell(self, text: str) -> Tuple[Optional[str], str, str]:
        """Parse a cell like "Ja'Marr Chase Cin - WR" → (name, team, pos)."""
        # Normalize spaces
        text = ' '.join((text or '').split())
        # Primary pattern: Name Team - POS
        m = re.match(r"^(?P<name>.+?)\s+([A-Za-z]{2,3})\s*-\s*([A-Za-z/]+)$", text)
        if m:
            name = m.group('name').strip()
            parts = text[len(name):].strip()
            mm = re.match(r"^([A-Za-z]{2,3})\s*-\s*([A-Za-z/]+)$", parts)
            if mm:
                return name, mm.group(1).upper(), mm.group(2).upper()
        # Alternate: Name - Team - POS
        m = re.match(r"^(?P<name>.+?)\s*-\s*([A-Za-z]{2,3})\s*-\s*([A-Za-z/]+)$", text)
        if m:
            return m.group('name').strip(), m.group(2).upper(), m.group(3).upper()
        return None, 'FA', 'UNK'

    def _update_offset(self, url: str, offset: int) -> str:
        """Update Yahoo pagination offset. Yahoo uses 'count' as start offset per 25 items."""
        parsed = urlparse.urlparse(url)
        q = urlparse.parse_qs(parsed.query)
        q['count'] = [str(offset)]
        new_query = urlparse.urlencode({k: v[0] for k, v in q.items()})
        return urlparse.urlunparse(parsed._replace(query=new_query))

    def sync_to_database(self, url: str, week: Optional[int] = None) -> Dict[str, int]:
        if week is None:
            week = self._infer_week_from_url(url) or 1

        html = self.fetch_html(url)
        fas = self.parse_free_agents(html)
        if not fas:
            return {"players_parsed": 0, "projections_synced": 0}

        db = SessionLocal()
        synced = 0
        parsed = 0
        try:
            for fa in fas:
                parsed += 1
                if not fa.name:
                    continue
                name = self.mapper.normalize_player_name(fa.name)
                pos = self.mapper.normalize_position(fa.position)
                team = self.mapper.normalize_team(fa.team)

                # Find or create player
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

                # Upsert projection (source='yahoo')
                proj = (
                    db.query(PlayerProjections)
                    .filter(
                        PlayerProjections.player_id == player.id,
                        PlayerProjections.week == week,
                        PlayerProjections.season == self.season,
                        PlayerProjections.source == 'yahoo',
                    )
                    .first()
                )
                if proj:
                    proj.projected_points = fa.projected
                    proj.mean = fa.projected
                else:
                    proj = PlayerProjections(
                        player_id=player.id,
                        week=week,
                        season=self.season,
                        projected_points=fa.projected,
                        mean=fa.projected,
                        source='yahoo',
                        scoring_format='ppr',
                    )
                    db.add(proj)
                synced += 1

            db.commit()
            logger.info(f"Yahoo FA sync complete: parsed={parsed}, projections={synced}")
            return {"players_parsed": parsed, "projections_synced": synced}
        except Exception as e:
            db.rollback()
            logger.error(f"Failed to sync Yahoo FA data: {e}")
            return {"players_parsed": parsed, "projections_synced": synced}
        finally:
            db.close()

    def sync_top_free_agents(self, url: str, top_n: int = 150, week: Optional[int] = None) -> Dict[str, int]:
        """Fetch top N free agents by paging through Yahoo results (25 per page)."""
        if week is None:
            week = self._infer_week_from_url(url) or 1

        all_rows: List[YahooFA] = []
        seen: set = set()
        offset = 0
        while len(all_rows) < top_n and offset <= 500:
            page_url = self._update_offset(url, offset)
            try:
                html = self.fetch_html(page_url)
                rows = self.parse_free_agents(html)
                logger.info(f"Yahoo page offset={offset}: parsed_rows={len(rows)}")
                if not rows:
                    break
                for r in rows:
                    key = (r.name, r.team, r.position)
                    if not r.name or key in seen:
                        continue
                    all_rows.append(r)
                    seen.add(key)
                    if len(all_rows) >= top_n:
                        break
                if len(rows) < 10:  # likely no more pages
                    break
            except Exception as e:
                logger.warning(f"Failed page at offset={offset}: {e}")
                break
            offset += 25

        # Persist (offense-only)
        db = SessionLocal()
        synced = 0
        try:
            for fa in all_rows:
                name = self.mapper.normalize_player_name(fa.name)
                pos = self.mapper.normalize_position(fa.position)
                if pos not in ("QB", "RB", "WR", "TE"):
                    continue
                team = self.mapper.normalize_team(fa.team)
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

                proj = (
                    db.query(PlayerProjections)
                    .filter(
                        PlayerProjections.player_id == player.id,
                        PlayerProjections.week == week,
                        PlayerProjections.season == self.season,
                        PlayerProjections.source == 'yahoo',
                    )
                    .first()
                )
                if proj:
                    proj.projected_points = fa.projected
                    proj.mean = fa.projected
                else:
                    proj = PlayerProjections(
                        player_id=player.id,
                        week=week,
                        season=self.season,
                        projected_points=fa.projected,
                        mean=fa.projected,
                        source='yahoo',
                        scoring_format='ppr',
                    )
                    db.add(proj)
                synced += 1
            db.commit()
            logger.info(f"Yahoo FA top {top_n} synced: rows={len(all_rows)}, projections={synced}")
            return {"players_parsed": len(all_rows), "projections_synced": synced}
        except Exception as e:
            db.rollback()
            logger.error(f"Failed to sync Yahoo FA top list: {e}")
            return {"players_parsed": len(all_rows), "projections_synced": synced}
        finally:
            db.close()


def main(argv: List[str]) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Sync Yahoo Fantasy free agents projections to DB")
    parser.add_argument('--url', required=True, help='Yahoo players list URL for your league')
    parser.add_argument('--week', type=int, help='Week number (optional; inferred from stat1 if not provided)')
    parser.add_argument('--top', type=int, default=0, help='Fetch top N free agents by paging (e.g., 150). If 0, scrape only given URL page.')
    args = parser.parse_args(argv)

    svc = YahooFreeAgentService()
    if args.top and args.top > 0:
        out = svc.sync_top_free_agents(args.url, args.top, args.week)
    else:
        out = svc.sync_to_database(args.url, args.week)
    print(out)
    return 0


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))

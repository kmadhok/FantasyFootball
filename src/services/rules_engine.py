import logging
from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Optional, Tuple

from sqlalchemy import func

from src.database import (
    SessionLocal,
    WaiverCandidates,
    Player,
    PlayerUsage,
    PlayerInjuryReport,
    DepthChart,
    NFLSchedule,
    DefensiveStats,
    Alert,
    NewsItem,
)

logger = logging.getLogger(__name__)


@dataclass
class RuleResult:
    player_id: int
    rule_id: str
    title: str
    bullets: List[str]


def _league_team_count(db, league_id: str) -> int:
    from src.database import RosterEntry

    return (
        db.query(RosterEntry.user_id)
        .filter(RosterEntry.league_id == league_id, RosterEntry.is_active == True)
        .distinct()
        .count()
    )


def rule_role_spike(row, week: int, db) -> Optional[RuleResult]:
    # Thresholds (pp deltas using 0-1 scale)
    route_pp = (row.route_delta or 0.0) >= 0.20
    snap_pp = (row.snap_delta or 0.0) >= 0.25
    if not (route_pp or snap_pp):
        return None

    pos = (row.pos or '').upper()
    if pos in ('WR', 'TE'):
        if (row.tprr or 0.0) < 0.18:
            return None
    elif pos == 'RB':
        # Consider carry share for RBs if available from usage
        usage = (
            db.query(PlayerUsage)
            .filter(PlayerUsage.player_id == row.player_id, PlayerUsage.week == week)
            .first()
        )
        if usage and usage.carry_share is not None and usage.carry_share < 0.30:
            return None

    bullets = []
    if route_pp:
        bullets.append(f"Routes up {row.route_delta:.0%} WoW")
    if snap_pp:
        bullets.append(f"Snaps up {row.snap_delta:.0%} WoW")
    if row.tprr is not None:
        bullets.append(f"TPRR {row.tprr:.0%} (current week)")
    if row.proj_next is not None:
        bullets.append(f"Next week proj {row.proj_next:.1f} PPR")

    return RuleResult(player_id=row.player_id, rule_id='role_spike', title='Role Spike', bullets=bullets)


def rule_injury_vacuum(row, week: int, db) -> Optional[RuleResult]:
    # Find starter on same team (depth_rank 1) with injury status
    starter = (
        db.query(DepthChart)
        .join(Player, Player.id == DepthChart.player_id)
        .filter(
            DepthChart.team == row.player.team,
            DepthChart.position == row.pos,
            DepthChart.week == week,
            DepthChart.depth_rank == 1,
        )
        .first()
    )
    injured = False
    if starter:
        ir = (
            db.query(PlayerInjuryReport)
            .filter(
                PlayerInjuryReport.player_id == starter.player_id,
                PlayerInjuryReport.week == week,
            )
            .first()
        )
        if ir and (str(ir.report_status) in {"Out", "IR"} or (str(ir.report_status) == 'Doubtful' and (ir.practice_participation_pct or 0) < 25)):
            injured = True
    if not injured:
        return None

    # Candidate readiness: snap_pct >= 40% last game OR depth rank <= 2
    ready = False
    usage = (
        db.query(PlayerUsage)
        .filter(PlayerUsage.player_id == row.player_id, PlayerUsage.week == week)
        .first()
    )
    if usage and (usage.snap_pct or 0) >= 0.40:
        ready = True
    else:
        cand_depth = (
            db.query(DepthChart)
            .filter(DepthChart.player_id == row.player_id, DepthChart.week == week)
            .first()
        )
        if cand_depth and (cand_depth.depth_rank or 99) <= 2:
            ready = True

    if not ready:
        return None

    bullets = [
        "Starter on IR/Out/Doubtful",
        "Next-man-up profile fits",
    ]
    if usage and usage.snap_pct is not None:
        bullets.append(f"Last game snaps {usage.snap_pct:.0%}")
    if row.proj_next is not None:
        bullets.append(f"Next week proj {row.proj_next:.1f} PPR")

    return RuleResult(player_id=row.player_id, rule_id='injury_vacuum', title='Injury Vacuum', bullets=bullets)


def rule_redzone(row, week: int, db) -> Optional[RuleResult]:
    pos = (row.pos or '').upper()
    rz_ok = False
    if pos in ('RB', 'TE') and (row.rz_last2 or 0) >= 3:
        rz_ok = True
    if pos in ('WR', 'TE') and (row.ez_last2 or 0) >= 2:
        rz_ok = True
    if not rz_ok:
        return None

    bullets = []
    if row.rz_last2 is not None:
        bullets.append(f"RZ touches (L2) {row.rz_last2}")
    if row.ez_last2 is not None:
        bullets.append(f"EZ targets (L2) {row.ez_last2}")
    if row.proj_next is not None:
        bullets.append(f"Next week proj {row.proj_next:.1f} PPR")
    return RuleResult(player_id=row.player_id, rule_id='red_zone', title='Red Zone Specialist', bullets=bullets)


def rule_trend_breakout(rows_same_pos: List[WaiverCandidates], row, week: int, db) -> Optional[RuleResult]:
    # Positive slope and above positional median
    slopes = [r.trend_slope for r in rows_same_pos if r.trend_slope is not None]
    if not slopes:
        return None
    median_slope = sorted(slopes)[len(slopes) // 2]
    if (row.trend_slope or 0.0) <= 0 or (row.trend_slope or 0.0) <= median_slope:
        return None

    # Replacement level: use positional median next-week projection among free agents
    proj = [r.proj_next for r in rows_same_pos if r.proj_next is not None]
    repl = sorted(proj)[len(proj) // 2] if proj else 0
    if (row.proj_next or 0) < repl:
        return None

    bullets = [f"3-week trend + ({row.trend_slope:.2f})", f">= replacement ({repl:.1f} pts)"]
    return RuleResult(player_id=row.player_id, rule_id='trend_breakout', title='Trend Breakout', bullets=bullets)


def _persist_alert(db, player_id: int, rule: RuleResult) -> int:
    player = db.query(Player).filter(Player.id == player_id).first()
    if not player:
        return 0

    headline = f"{rule.title}: {player.name}"
    news = NewsItem(
        player_id=player_id,
        headline=headline,
        headline_hash=f"rule_{rule.rule_id}_{player_id}_{datetime.utcnow().date().isoformat()}",
        content="; ".join(rule.bullets),
        source="rule",
        source_weight=1.0,
        confidence_score=0.9,
        event_type=rule.rule_id,
    )
    db.add(news)
    db.flush()

    alert = Alert(
        player_id=player_id,
        news_item_id=news.id,
        alert_type=rule.rule_id,
        is_rostered=False,
        waiver_recommendation='high_priority' if rule.rule_id in ('role_spike', 'injury_vacuum') else 'medium_priority',
        faab_suggestion=None,
        waiver_urgency='immediate' if rule.rule_id in ('role_spike', 'injury_vacuum') else 'next_cycle',
        delivery_status='pending',
    )
    db.add(alert)
    db.flush()
    return alert.id


def evaluate_rules(league_id: str, week: Optional[int] = None) -> Dict[str, int]:
    """Evaluate B1–B4 rules and persist alerts. Returns counts per rule and total alerts created."""
    created = {"role_spike": 0, "injury_vacuum": 0, "red_zone": 0, "trend_breakout": 0, "total": 0}
    db = SessionLocal()
    try:
        # Determine target week (current if not provided)
        if week is None:
            from src.services.enhanced_waiver_candidates_builder import EnhancedWaiverCandidatesBuilder
            week = EnhancedWaiverCandidatesBuilder()._get_current_nfl_week()

        # Sanity: skip solo leagues
        if _league_team_count(db, league_id) <= 1:
            logger.info(f"Skipping rule eval for solo league {league_id}")
            return created

        # Pull free-agent candidates for this league/week
        rows = (
            db.query(WaiverCandidates, Player)
            .join(Player, Player.id == WaiverCandidates.player_id)
            .filter(WaiverCandidates.league_id == league_id, WaiverCandidates.week == week, WaiverCandidates.rostered == False)
            .all()
        )
        if not rows:
            return created

        # Positional cohorts for trend breakout
        by_pos: Dict[str, List[WaiverCandidates]] = {}
        for wc, _ in rows:
            by_pos.setdefault(wc.pos, []).append(wc)

        # Evaluate per candidate
        for wc, player in rows:
            # Attach player for team/position convenience
            wc.player = player  # type: ignore

            res = rule_role_spike(wc, week, db)
            if res:
                _persist_alert(db, wc.player_id, res)
                created['role_spike'] += 1
                created['total'] += 1

            res = rule_injury_vacuum(wc, week, db)
            if res:
                _persist_alert(db, wc.player_id, res)
                created['injury_vacuum'] += 1
                created['total'] += 1

            res = rule_redzone(wc, week, db)
            if res:
                _persist_alert(db, wc.player_id, res)
                created['red_zone'] += 1
                created['total'] += 1

            res = rule_trend_breakout(by_pos.get(wc.pos, []), wc, week, db)
            if res:
                _persist_alert(db, wc.player_id, res)
                created['trend_breakout'] += 1
                created['total'] += 1

        db.commit()
        return created
    except Exception as e:
        db.rollback()
        logger.error(f"Rule evaluation failed: {e}")
        return created
    finally:
        db.close()


def test_rules_engine_smoke():
    print("Rules engine smoke test (requires seeded WaiverCandidates)")
    try:
        out = evaluate_rules("demo_league_12345")
        print(out)
        return True
    except Exception as e:
        print(f"Failed: {e}")
        return False


if __name__ == "__main__":
    test_rules_engine_smoke()


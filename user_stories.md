EPIC A — DATA FOUNDATION

US-A1: Canonical player/league schema
As the app, I want a canonical schema (players, leagues, rosters, usage, projections) so I can score candidates consistently across Sleeper & MFL.

Dependencies: none

Acceptance Criteria

Player rows can be looked up by canonical_player_id regardless of platform.

Roster snapshots persist (league_id, team_id, player_id, week, slot).

Usage table contains: week, snap_pct, route_pct, target_share, carry_share, rz_touches, ez_targets.

Projections table contains: week, mean, stdev, floor, ceiling for your scoring.

All tables can be joined to materialize a waiver_candidates view.

Data Contracts

player_map: canonical_id, sleeper_id?, mfl_id?, name, pos, team_nfl

roster_snapshots: platform, league_id, team_id, week, player_id, slot, synced_at

usage: player_id, week, snap_pct, route_pct, target_share, carry_share, rz_touches, ez_targets

projections: player_id, week, mean, stdev, floor, ceiling, source

Tasks for Claude Code

Create SQL DDL or ORM models for the above.

Add idempotent upsert helpers.

US-A2: Waiver candidates materialized view
As a manager, I want a materialized view that computes all features per player/week so alerts run fast and predictably.

Dependencies: US-A1

Acceptance Criteria

View waiver_candidates(league_id, week, player_id, pos, rostered?, snap_delta, route_delta, tprr, rz_last2, ez_last2, opp_next, proj_next, trend_slope, roster_fit, market_heat, scarcity) is queryable.

Refresh job populates for current week in < 1 minute.

Non-rostered players only (relative to your team).

Data Contracts (inputs/joins)

joins usage, projections, roster_snapshots, league scoring, schedule matrix.

Tasks for Claude Code

Implement SQL view or Python builder that emits a DataFrame and writes to table.

Add APScheduler job build_waiver_candidates(league_id, week).

EPIC B — SMART WAIVER ALERTS (TRIGGERS)

US-B1: Role Spike alert
As a manager, I want alerts when a WR/TE/RB’s role jumps so I can add them before the market adjusts.

Dependencies: US-A2

Acceptance Criteria

Trigger when route_pct Δ ≥ 20pp OR snap_pct Δ ≥ 25pp (week-over-week).

For WR/TE require TPRR ≥ 18% in current week; for RB consider carry_share.

Player is not on your roster; ≤50% rostered in league (or unavailable if solo league → skip).

Alert includes why-now bullets and suggested FAAB (from US-C2).

Data Contracts

Uses waiver_candidates.snap_delta, route_delta, tprr, pos.

Tasks for Claude Code

Implement rule_role_spike(candidate) -> Optional[Alert].

US-B2: Injury Vacuum alert
As a manager, I want alerts when a starter goes Out/IR and a clear next-man-up exists.

Dependencies: US-A2

Acceptance Criteria

Starter on same NFL team has status in {Out, IR, Doubtful with <25% practice participation}.

Candidate has snap_pct ≥ 40% last game or depth chart rank ≤2.

Alert includes “block” flag if next opponent needs that position.

Data Contracts

news/status feed (stub CSV acceptable), depth chart CSV optional.

Tasks

rule_injury_vacuum(candidate, team_status, depth_chart).

US-B3: Red Zone / Goal-line specialist alert
Acceptance Criteria

RB/TE: inside-10 touches ≥ 3 (last 2 games) OR WR/TE: ez_targets ≥ 2 (last 2).

Include touchdown volatility note and startability rating for next week.

Tasks

rule_redzone(candidate) using rz_last2, ez_last2.

US-B4: Trend Breakout alert
Acceptance Criteria

3-week positive slope on targets (WR/TE) or carries (RB) above positional median.

Alert only if proj_next ≥ positional replacement level.

Tasks

rule_trend_breakout(candidate, pos_replacement).

US-B5: Streamer Spotlight (next week)
Acceptance Criteria

Unrostered QB/TE/DST with top-8 proj_next in your scoring AND opponent filter (e.g., high implied points allowed or sacks allowed).

Includes start/sit confidence (High/Med/Low).

Tasks

rule_streamer(candidate, matchup_data).

US-B6: Handcuff Heat alert
Acceptance Criteria

Lead back questionable/limited AND candidate ≥30% snaps OR coach quotes indicate committee.

Adds “insurance” tag if you roster the starter.

Tasks

rule_handcuff_heat(candidate, lead_back_status).

US-B7: Bye / Late-swap Guard pre-alert
Acceptance Criteria

You have a bye hole in ≤2 weeks at candidate’s position AND candidate projects startable that week.

Alert contains “pre-bid” suggestion.

Tasks

rule_bye_guard(candidate, your_roster, schedule).

EPIC C — SCORING & FAAB

US-C1: WaiverScore engine
As the app, I want to compute a single WaiverScore per candidate so I can rank and tier them.

Dependencies: US-A2

Acceptance Criteria

Score formula (weights via YAML):
0.35*RoleDeltaZ + 0.20*OppShareZ + 0.15*RedZoneZ + 0.10*NextWeekProjZ + 0.10*TrendSlopeZ + 0.10*RosterFit - 0.10*BustRisk.

Z-scores are positional (WR vs WR, etc.).

Returns {score, components, tier} where tiers are A (>=0.7), B (0.45–0.69), C (0.25–0.44).

Data Contracts

Input: candidate row; Context: positional population stats; Config: weights.yml.

Tasks

score_player(candidate, ctx, weights) -> ScoreResult.

Small unit tests for edge cases (missing usage → lower BustRisk?).

US-C2: FAAB bid band calculator
As a manager, I want suggested conservative/median/aggressive bids that reflect my needs and market heat.

Dependencies: US-C1

Acceptance Criteria

Computes RawBid using: ValueΔ * WeeksRemaining * (0.3 + 0.7*Scarcity) * Need.

Clamps to Cap = min(FAAB_remaining*0.45, FAAB_remaining - 1).

Applies BlockFactor (1.15–1.25) if opponent would start the player; MarketHeat (+10–30%) based on needy teams count.

Returns integer dollar bids {conservative=0.8x, median=1.0x, aggressive=1.2x}.

Tasks

compute_faab_bid(score_result, roster_ctx, market_ctx) -> BidBand.

EPIC D — NOTIFICATIONS & DIGESTS

US-D1: Tuesday Waiver Digest
As a manager, I want a single Tuesday morning digest grouped by tiers with FAAB bands and rationale.

Dependencies: US-B1..B7, US-C1..C2

Acceptance Criteria

Runs Tue 9:00 AM local; includes Tier A/B/C lists (max 5 per tier).

Each item shows: name/pos/team, WaiverScore, FAAB band, 3–4 bullet rationales, quick-action buttons (Conservative / Median / Aggressive / Ignore 7d).

Digest excludes players suppressed by cooldown (US-E1).

Posts to your chosen channel (Telegram/Discord/Slack).

Tasks

generate_tuesday_digest(league_id, week) → NotificationPayload.

notify(channel, payload) integration.

US-D2: Post-Waiver Recap
As a manager, I want a Wednesday recap of wins/losses and the best remaining pivots.

Dependencies: US-D1, US-F1

Acceptance Criteria

Lists your bids vs winners, delta to winning price, and “backup” adds.

Updates outcome tracking table.

Tasks

generate_waiver_recap(league_id, week).

US-D3: Live late-swap watchlist (lite)
As a manager, I want a Sunday-AM watchlist of questionable players + pivots with higher variance.

Dependencies: minimal (can be stubbed)

Acceptance Criteria

Shows only players on your roster flagged Q/GTD + 2 pivots each, with a “risk knob” note.

One message; no spam.

Tasks

generate_late_swap_watchlist(league_id, week).

EPIC E — QUALITY & EXPERIENCE

US-E1: Alert cooldown & dedupe
As a manager, I don’t want duplicate alerts unless something meaningfully changes.

Dependencies: US-D1

Acceptance Criteria

Hash of (player_id, rule_id, week, tier, faab_band) stored in alert_log.

Suppress repeats for 7 days unless score changes by ≥15% or new rule fires.

Tasks

should_alert(payload) -> bool.

record_alert(payload).

US-E2: Outcome tracking & learning loop
As the app, I want to record whether I won bids and realized points to tune weights.

Dependencies: US-D2

Acceptance Criteria

Store: bid placed, result, winning bid, start/not start, points over replacement next 1–3 weeks.

Export CSV for analysis.

Tasks

Functions to write entries after waivers and after games finalize.

EPIC F — BONUS (EASY WINS)

US-F1: Dropometer (who to cut)
As a manager, I want a ranked list of bench players by probability of cracking my lineup in the next 3 weeks.

Dependencies: US-A2, US-C1

Acceptance Criteria

Computes StartProb from projections vs your starters’ floor/ceiling and schedule.

Outputs top 5 drop candidates with reasons (blocked role, low ceiling).

Tasks

compute_dropometer(your_roster_ctx) -> List[DropCandidate].

US-F2: Stash Ladder (league-winner upside)
As a manager, I want a prioritized stash list based on takeover probability × team efficiency.

Acceptance Criteria

Score = TakeoverProb * TeamRushEfficiency (RB) or RouteVacancyProb * TeamPassEfficiency (WR/TE).

Top 5 with short rationale.

Tasks

build_stash_ladder() (rules can read coach quotes CSV for now).

US-F3: Bye-week Oracle
As a manager, I want a two-week look-ahead shopping list to cover byes without panic.

Acceptance Criteria

For each upcoming bye hole, suggest 2–3 players with best proj_next and availability likelihood.

Integrates with Tuesday digest if within 2 weeks.

Tasks

generate_bye_oracle_list(league_id, week).

IMPLEMENTATION NOTES FOR CLAUDE CODE

Shared Function Signatures

build_waiver_candidates(league_id: str, week: int) -> pd.DataFrame

score_player(candidate: dict, ctx: ScoringContext, weights: dict) -> ScoreResult

compute_faab_bid(score: ScoreResult, roster_ctx: RosterContext, market_ctx: MarketContext) -> BidBand

evaluate_rules(candidates: Iterable[dict]) -> List[Alert]

generate_tuesday_digest(league_id: str, week: int) -> NotificationPayload

notify(channel: str, payload: NotificationPayload) -> None

should_alert(alert: Alert) -> bool

record_alert(alert: Alert) -> None

record_outcome(outcome: Outcome) -> None

Schedules (APScheduler)

Tue 09:00: digest_job

Wed 08:00: recap_job

Sun 09:00: late_swap_job

Mon 07:00: usage_refresh_job

Nightly 01:00: projections_refresh_job

Config (YAML)

weights.yml: component weights + tier thresholds.

rules.yml: enable/disable rules per league.

channels.yml: notification endpoints.

cooldown.yml: suppression windows, significant-change threshold.

Testing Checklist (per story)

Unit: rule triggers with synthetic candidates (positive/negative cases).

Unit: scoring + FAAB math with deterministic fixtures.

Integration: Tuesday digest renders tiers correctly; cooldown suppresses repeats.

Integration: After mock waivers, recap compiles correct results.
Fantasy Football Real‑Time Alert & Waiver Monitor

Document type: Product Requirements Document (PRD)Audience: Junior developerFeature name / file: prd-fantasy-alert-system.md

1 · Overview

Managing two fantasy teams across Sleeper and MyFantasyLeague (MFL) is time‑consuming.  Critical roster news (injuries, trades, depth‑chart changes) can break at any moment, and waiver rules differ per platform (FAAB vs. priority).  The goal is to build a background service that:

Monitors free news sources (Reddit, X/Twitter beat reporters, public APIs) and league transaction feeds in near real‑time.

Pushes actionable alerts within < 5 minutes directly to the user.

Tracks waiver state (remaining FAAB on MFL, waiver order on Sleeper) so the user can decide claims quickly.

The system runs only during the NFL season; it shuts down in the off‑season.

2 · Goals (v1)

Deliver 100 % of qualifying alerts (< 5 min latency) for players on the user’s roster or newly promoted starters.

Auto‑sync rosters from both leagues daily—no manual player list maintenance.

Capture and display current waiver order (Sleeper) and FAAB balances (MFL) in alerts.

Maintain < 5 % false‑positive rate on alerts.

Operate entirely on free‑tier APIs / hosting (or ≤ $10 / month if unavoidable).

3 · User Stories

U1. As a fantasy manager, I want an alert within five minutes when one of my rostered players is ruled out so I can add a replacement.

U2. As a fantasy manager, I want an alert when a backup running back is promoted to starter so I can place a timely waiver claim.

4 · Functional Requirements

Roster Sync1.1 The system shall pull Sleeper rosters via GET /league/{league_id}/rosters once every 24 h.1.2 The system shall pull MFL rosters via export?TYPE=rosters once every 24 h.1.3 The system shall map platform‑specific player IDs to a canonical NFL ID.

Waiver State Capture2.1 The system shall fetch Sleeper waiver order (/league/{id}/waivers) every 6 h.2.2 The system shall fetch MFL FAAB balances via export?TYPE=blindBidSummary daily.2.3 The system shall not recommend bid amounts in v1 (informational only).

News Ingestion3.1 The system shall stream new submissions & comments from r/fantasyfootball and filter for roster‑movement keywords (trade, signed, injured, IR, released, etc.).3.2 The system shall connect to Twitter/X filtered stream for predefined beat‑reporter handles and scan tweets for the same keyword list.3.3 The system shall poll free structured feeds (ESPN player news JSON, Sleeper trending) every 2 min.3.4 The system shall tag each news item with a confidence score (source weight + keyword match).3.5 The system shall deduplicate items using (player_id, headline_hash) within a 24 h window.

Alert Delivery4.1 The system shall push alerts via a Slack Incoming Webhook (chosen for easiest free implementation).4.2 Each alert shall include: player name, team, event type, source link, timestamp, current waiver info (order or FAAB), and rostered‑by‑user flag.4.3 Alerts shall be sent only if the player is on the user’s roster or the event elevates the fantasy value of an unrostered starter‑level player (RB1, WR1–2, QB, TE1).

Service Lifecycle5.1 The service shall disable ingestion jobs automatically at the end of the NFL season (Super Bowl + 7 days).5.2 The service shall restart the week before NFL preseason unless manually paused.

5 · Non‑Goals (Out of Scope)

Draft strategy or board projections.

DFS / sports‑betting integrations.

Automated FAAB bid calculation or sit‑start lineup optimization.

6 · Design Considerations (Optional Guidance)

Architecture: Small Docker service with three async workers (Ingest → Dedup → Notify) using Redis for message passing.

Hosting: Fly.io 256 MB free machine or GitHub Actions (5‑min cron).

Data Store: SQLite + Redis cache, all free tier.

Rate Limits: Keep < 100 requests/min to Reddit; use one Twitter dev key (< 50 filtered rules).

Player Classification: Use a static JSON of depth charts or free API‑Sports depth endpoint.

7 · Success Metrics

Metric

Target

Alert latency

 ≥ 95 % of alerts delivered in < 5 min

False positives

≤ 5 % of total alerts per week

Uptime during regular season

 ≥ 95 %

Roster sync accuracy

 100 % of player IDs mapped correctly

8 · Open Questions

League IDs & credentials – Awaiting actual Sleeper league_id and MFL league_id (+ season + password if private).

Slack Webhook URL – Provide or confirm alternative channel (Discord, email).

Exact beat‑reporter handle list – Confirm baseline list or accept default 64‑account bundle.

Keyword list tuning – Any extra phrases to include/exclude?

Off‑season shut‑down date – Use Super Bowl + 7 days or different cutoff?

9 · Version History

Date

Author

Notes

2025‑07‑16

ChatGPT (o3)

v1 — initial PRD based on user clarifications


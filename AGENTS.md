# Repository Guidelines

## Project Structure & Module Organization
- Source in `src/`: `services/` (domain logic), `database/` (models, migrations), `utils/`, `config/`, and `main.py`.
- Tests live in both `src/**/test_*.py` (unit) and `tests/` (integration/manual validation helpers).
- Data and artifacts in `data/` (SQLite by default). Interactive scripts in `manual_test/`.
- Containerization via `Dockerfile` and `docker-compose.yml`.

## Build, Test, and Development Commands
- Setup: `python -m venv venv && source venv/bin/activate && pip install -r requirements.txt`.
- Env: create `.env` with `SLEEPER_LEAGUE_ID`, `MFL_LEAGUE_ID`, `MFL_LEAGUE_API_KEY`, `DATABASE_URL=sqlite:///data/fantasy_football.db`.
- DB init: `python src/database/migrations.py`.
- Run app: `python -m src.main` (run from repo root to resolve imports).
- Tests: `pytest -v`; examples: `pytest -m "unit and not slow"`, `pytest src/services/test_roster_sync.py -v`.
- Docker (optional): `docker compose up --build`.

## Coding Style & Naming Conventions
- Python 3.12, 4-space indentation, PEP 8. Prefer type hints for new/modified code.
- Format/lint/type-check: `black .`, `flake8 src tests`, `mypy src`.
- Naming: modules/files/functions `snake_case`, classes `PascalCase`, constants `UPPER_SNAKE_CASE`.
- Tests mirror targets (e.g., `src/utils/test_player_id_mapper.py`) and use clear, behavior-focused names.

## Testing Guidelines
- Framework: `pytest` with markers: `unit`, `integration`, `slow` (see `pytest.ini`).
- Test discovery: filenames `test_*.py` or `*_test.py`; functions `test_*`.
- Unit tests should mock I/O and network; integration tests may use the SQLite DB in `data/`.
- Run all tests from project root to ensure module paths resolve.

## Commit & Pull Request Guidelines
- Prefer Conventional Commits: `feat: ...`, `fix: ...`, `refactor: ...`, `test: ...`, `docs: ...`.
- PRs include: clear description, scope/impact, linked issue(s), reproduction/verification steps, and notes on config/DB changes.
- Before pushing: `black . && flake8 src tests && mypy src && pytest -v`.

## Security & Configuration Tips
- Do not commit real secrets. `.env` is loaded by `python-dotenv` and used in Docker; use placeholders locally.
- Be mindful of `data/` contents (SQLite DB, CSVs). Avoid committing sensitive or oversized artifacts.
- For import reliability, run commands from the repo root and prefer `python -m <module>`.


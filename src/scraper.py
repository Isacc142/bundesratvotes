import argparse
import re
import sqlite3
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup


# ---------------------------------------------------------------------------
# Bundesrat session model
# ---------------------------------------------------------------------------

_BUNDESRAT_BASE_URL = "https://www.bundesrat.de/"

# Matches e.g. "08.05.2026 | 1065. Sitzung des Bundesrates"
_SESSION_RE = re.compile(
    r"(\d{2})\.(\d{2})\.(\d{4})\s*\|\s*(\d+)\.\s*Sitzung",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class BundesratSession:
    """Composite identity for a Bundesrat plenary session."""

    ordinal: int          # e.g. 1065
    session_date: date    # e.g. date(2026, 5, 8)
    detail_url: Optional[str] = None   # absolute URL to session TO page
    is_sonder: bool = False            # True for Sondersitzungen

    @property
    def session_id(self) -> str:
        """Human-readable composite key: '1065/2026-05-08'."""
        return f"{self.ordinal}/{self.session_date.isoformat()}"


def parse_bundesrat_sessions(
    html: str,
    base_url: str = _BUNDESRAT_BASE_URL,
) -> list[BundesratSession]:
    """Parse all plenary sessions from the Bundesrat archive HTML page.

    Returns sessions sorted newest-first (highest ordinal first).
    """
    soup = BeautifulSoup(html, "html.parser")
    sessions: list[BundesratSession] = []

    for a in soup.select("ul.link-list li a[href]"):
        text = " ".join(a.get_text(" ", strip=True).split())
        m = _SESSION_RE.search(text)
        if not m:
            continue

        day, month, year, ordinal = (
            int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4))
        )
        try:
            session_date = date(year, month, day)
        except ValueError:
            continue

        href = a["href"].strip()
        detail_url = urljoin(base_url, href) if href else None
        is_sonder = "sonder" in text.lower()

        sessions.append(
            BundesratSession(
                ordinal=ordinal,
                session_date=session_date,
                detail_url=detail_url,
                is_sonder=is_sonder,
            )
        )

    # Deduplicate by ordinal (keep first occurrence)
    seen: set[int] = set()
    unique: list[BundesratSession] = []
    for s in sessions:
        if s.ordinal not in seen:
            seen.add(s.ordinal)
            unique.append(s)

    unique.sort(key=lambda s: s.ordinal, reverse=True)
    return unique


def load_bundesrat_sessions(
    html_path: Path,
    base_url: str = _BUNDESRAT_BASE_URL,
) -> list[BundesratSession]:
    """Load and parse sessions from a saved bundesrat.html file."""
    return parse_bundesrat_sessions(
        html_path.read_text(encoding="utf-8"), base_url=base_url
    )


# ---------------------------------------------------------------------------
# SQLite helpers
# ---------------------------------------------------------------------------


def init_db(db_path: Path) -> sqlite3.Connection:
    """Open (or create) the SQLite database and ensure all tables exist."""
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bundesrat_sessions (
            ordinal      INTEGER PRIMARY KEY,
            session_date TEXT NOT NULL,
            detail_url   TEXT,
            is_sonder    INTEGER NOT NULL DEFAULT 0,
            updated_at   TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()
    return conn


def store_bundesrat_sessions(
    conn: sqlite3.Connection,
    sessions: list[BundesratSession],
) -> None:
    """Upsert sessions into the bundesrat_sessions table."""
    conn.executemany(
        """
        INSERT INTO bundesrat_sessions(ordinal, session_date, detail_url, is_sonder)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(ordinal) DO UPDATE SET
            session_date = excluded.session_date,
            detail_url   = excluded.detail_url,
            is_sonder    = excluded.is_sonder,
            updated_at   = CURRENT_TIMESTAMP
        """,
        [
            (s.ordinal, s.session_date.isoformat(), s.detail_url, int(s.is_sonder))
            for s in sessions
        ],
    )
    conn.commit()


# ---------------------------------------------------------------------------


class HtmlPullAgent:
    def __init__(self, timeout: int = 20):
        self.timeout = timeout

    def fetch_and_save(self, url: str, output_file: str | None = None) -> str:
        response = requests.get(
            url,
            timeout=self.timeout,
            headers={"User-Agent": "Mozilla/5.0 (compatible; HtmlPullAgent/1.0)"},
        )
        response.raise_for_status()

        if not output_file:
            host = urlparse(url).netloc.replace(":", "_") or "page"
            output_file = f"{host}.html"

        with open(output_file, "w", encoding="utf-8") as f:
            f.write(response.text)

        return output_file


def _slugify(name: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9._-]+", "_", name.strip().lower()).strip("_")
    return slug or "page"
    #test


def _read_url_entries(urls_file: str) -> list[tuple[str, str]]:
    entries: list[tuple[str, str]] = []
    for i, raw_line in enumerate(Path(urls_file).read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        parts = [p.strip() for p in line.split(",")]
        if len(parts) >= 2:
            state, url = parts[0], parts[1]
        else:
            url = parts[0]
            state = urlparse(url).netloc or f"url_{i}"

        entries.append((_slugify(state), url))
    return entries


class PullPDFsAgent:
    def __init__(self, timeout: int = 20):
        self.html_agent = HtmlPullAgent(timeout=timeout)

    def fetch_all_html(self, urls_file: str, output_dir: str) -> list[str]:
        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        saved_files: list[str] = []
        failed: list[tuple[str, str, str]] = []

        for state, url in _read_url_entries(urls_file):
            output_file = out_dir / f"{state}.html"
            try:
                self.html_agent.fetch_and_save(url, str(output_file))
                saved_files.append(str(output_file))
            except Exception as exc:
                failed.append((state, url, str(exc)))
                print(f"[ERROR] Failed to fetch '{state}' ({url}): {exc}")

        if failed:
            print(f"Completed with errors: {len(saved_files)} saved, {len(failed)} failed.")
        return saved_files


def main():
    parser = argparse.ArgumentParser(
        description="Fetch HTML files and/or parse Bundesrat sessions."
    )
    subparsers = parser.add_subparsers(dest="command", help="Sub-commands")

    # --- fetch: download HTML files ---
    fetch_parser = subparsers.add_parser("fetch", help="Fetch all HTML files from urls.txt")
    fetch_parser.add_argument(
        "--urls-file",
        default=str(Path(__file__).with_name("urls.txt")),
        help="Path to URL list file (default: urls.txt next to this script)",
    )
    fetch_parser.add_argument(
        "--output-dir",
        default=str(Path(__file__).with_name("html_files")),
        help="Directory for saved HTML files (default: html_files next to this script)",
    )

    # --- parse-sessions: extract Bundesrat sessions from bundesrat.html ---
    sessions_parser = subparsers.add_parser(
        "parse-sessions",
        help="Parse Bundesrat plenary sessions from bundesrat.html and store them in SQLite",
    )
    sessions_parser.add_argument(
        "--bundesrat-html",
        default=str(Path(__file__).with_name("html_files") / "bundesrat.html"),
        help="Path to saved bundesrat.html (default: html_files/bundesrat.html)",
    )
    sessions_parser.add_argument(
        "--db-path",
        default=str(Path(__file__).with_name("bundesrat.sqlite3")),
        help="SQLite database path (default: bundesrat.sqlite3 next to this script)",
    )

    args = parser.parse_args()

    if args.command == "fetch":
        agent = PullPDFsAgent()
        paths = agent.fetch_all_html(args.urls_file, args.output_dir)
        print(f"Saved {len(paths)} HTML files to: {args.output_dir}")

    elif args.command == "parse-sessions":
        html_path = Path(args.bundesrat_html)
        if not html_path.exists():
            print(f"[ERROR] File not found: {html_path}")
            return
        sessions = load_bundesrat_sessions(html_path)
        print(f"Parsed {len(sessions)} sessions.")
        for s in sessions[:5]:
            print(f"  {s.session_id}  sonder={s.is_sonder}")
        if len(sessions) > 5:
            print(f"  ... ({len(sessions) - 5} more)")
        conn = init_db(Path(args.db_path))
        store_bundesrat_sessions(conn, sessions)
        conn.close()
        print(f"Stored to: {args.db_path}")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
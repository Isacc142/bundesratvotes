import argparse
import hashlib
import re
import sqlite3
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


@dataclass(frozen=True)
class StateSource:
	state: str
	url: str
	adapter_key: Optional[str] = None


@dataclass(frozen=True)
class PdfCandidate:
	url: str
	source_url: str
	title: str
	published_at: Optional[datetime]
	discovered_at: datetime


@dataclass(frozen=True)
class DownloadResult:
	status: str
	local_path: str
	sha256: Optional[str]
	size_bytes: Optional[int]
	error: Optional[str]


def create_session(user_agent: str) -> requests.Session:
	session = requests.Session()
	retry = Retry(
		total=5,
		connect=5,
		read=5,
		backoff_factor=1.2,
		status_forcelist=(429, 500, 502, 503, 504),
		allowed_methods=frozenset({"GET", "HEAD"}),
		raise_on_status=False,
	)
	adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=20)
	session.mount("http://", adapter)
	session.mount("https://", adapter)
	session.headers.update({"User-Agent": user_agent, "Accept": "*/*"})
	return session


def normalize_state_name(raw: str) -> str:
	return re.sub(r"\s+", "-", raw.strip().lower())


def init_db(db_path: Path) -> sqlite3.Connection:
	conn = sqlite3.connect(db_path)
	conn.execute(
		"""
		CREATE TABLE IF NOT EXISTS ingestion_runs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			started_at TEXT DEFAULT CURRENT_TIMESTAMP,
			finished_at TEXT,
			source_file TEXT,
			total_states INTEGER,
			ok_states INTEGER,
			failed_states INTEGER
		)
		"""
	)
	conn.execute(
		"""
		CREATE TABLE IF NOT EXISTS latest_pdfs (
			state TEXT PRIMARY KEY,
			adapter_key TEXT,
			source_url TEXT,
			pdf_url TEXT,
			title TEXT,
			published_at TEXT,
			local_path TEXT,
			status TEXT NOT NULL,
			sha256 TEXT,
			bytes INTEGER,
			error TEXT,
			updated_at TEXT DEFAULT CURRENT_TIMESTAMP
		)
		"""
	)
	conn.commit()
	return conn


def load_state_sources(path: Path) -> list[StateSource]:
	sources: list[StateSource] = []
	with path.open("r", encoding="utf-8") as fh:
		for line in fh:
			line = line.strip()
			if not line or line.startswith("#"):
				continue

			parts = [p.strip() for p in line.split(",")]
			if len(parts) >= 2 and parts[0].startswith(("http://", "https://")) is False:
				state = parts[0]
				url = parts[1]
				adapter_key = parts[2] if len(parts) >= 3 and parts[2] else None
				sources.append(StateSource(state=state, url=url, adapter_key=adapter_key))
			else:
				url = line
				host = (urlparse(url).hostname or "").lower() or "unknown"
				state = host.split(".")[0]
				sources.append(StateSource(state=state, url=url, adapter_key=None))

	if not sources:
		raise ValueError(f"No state sources found in: {path}")
	return sources


def parse_date(text: str) -> Optional[datetime]:
	if not text:
		return None

	patterns = (
		r"\b(\d{1,2})\.(\d{1,2})\.(\d{4})\b",
		r"\b(\d{4})-(\d{1,2})-(\d{1,2})\b",
		r"\b(\d{4})(\d{2})(\d{2})\b",
	)
	for i, pattern in enumerate(patterns):
		m = re.search(pattern, text)
		if not m:
			continue
		try:
			if i == 0:
				d, mo, y = map(int, m.groups())
				return datetime(y, mo, d)
			y, mo, d = map(int, m.groups())
			return datetime(y, mo, d)
		except ValueError:
			return None
	return None


def parse_html_links(base_url: str, html: str) -> list[tuple[str, str]]:
	soup = BeautifulSoup(html, "html.parser")
	links: list[tuple[str, str]] = []
	for a in soup.find_all("a", href=True):
		href = a["href"].strip()
		if href.startswith(("mailto:", "javascript:", "#")):
			continue
		abs_url = urljoin(base_url, href)
		title = " ".join(a.get_text(" ", strip=True).split())
		links.append((abs_url, title))
	return links


def pick_newest(candidates: list[PdfCandidate]) -> Optional[PdfCandidate]:
	if not candidates:
		return None

	def score(item: PdfCandidate) -> tuple[float, float, str]:
		published_ts = item.published_at.timestamp() if item.published_at else float("-inf")
		discovered_ts = item.discovered_at.timestamp()
		return (published_ts, discovered_ts, item.url)

	return sorted(candidates, key=score, reverse=True)[0]


def sha256_file(path: Path) -> str:
	h = hashlib.sha256()
	with path.open("rb") as f:
		for chunk in iter(lambda: f.read(1024 * 1024), b""):
			h.update(chunk)
	return h.hexdigest()


def safe_filename_from_url(url: str) -> str:
	from urllib.parse import urlparse as _urlparse

	name = Path(_urlparse(url).path).name or "file.pdf"
	name = re.sub(r"[^A-Za-z0-9._-]+", "_", name)
	if not name.lower().endswith(".pdf"):
		name += ".pdf"
	return name


def download_pdf(
	session: requests.Session,
	out_dir: Path,
	timeout: int,
	delay: float,
	user_agent: str,
	url: str,
) -> DownloadResult:
	time.sleep(delay)
	out_dir.mkdir(parents=True, exist_ok=True)

	file_name = safe_filename_from_url(url)
	final_path = out_dir / file_name
	if final_path.exists():
		stem, suf = final_path.stem, final_path.suffix
		suffix = hashlib.md5(url.encode()).hexdigest()[:8]
		final_path = out_dir / f"{stem}_{suffix}{suf}"

	tmp_path = final_path.with_suffix(final_path.suffix + ".part")

	try:
		with session.get(url, stream=True, timeout=timeout, headers={"User-Agent": user_agent}) as r:
			if not r.ok:
				return DownloadResult("failed", "", None, None, f"http_{r.status_code}")

			content_type = (r.headers.get("Content-Type") or "").lower()
			first_chunk = b""
			size = 0

			with tmp_path.open("wb") as f:
				for i, chunk in enumerate(r.iter_content(chunk_size=1024 * 64)):
					if not chunk:
						continue
					if i == 0:
						first_chunk = chunk
					f.write(chunk)
					size += len(chunk)

			is_pdf = first_chunk.startswith(b"%PDF-") or "application/pdf" in content_type
			if not is_pdf:
				tmp_path.unlink(missing_ok=True)
				return DownloadResult("failed", "", None, None, "not_pdf")

		tmp_path.replace(final_path)
		return DownloadResult("ok", str(final_path), sha256_file(final_path), size, None)

	except requests.RequestException as e:
		tmp_path.unlink(missing_ok=True)
		return DownloadResult("failed", "", None, None, f"request_error:{e.__class__.__name__}")
	except OSError as e:
		tmp_path.unlink(missing_ok=True)
		return DownloadResult("failed", "", None, None, f"os_error:{e.__class__.__name__}")


class BaseStateAdapter:
	def discover_pdf_candidates(
		self,
		session: requests.Session,
		source: StateSource,
		timeout: int,
	) -> list[PdfCandidate]:
		try:
			r = session.get(source.url, timeout=timeout)
		except requests.RequestException:
			return []

		if not r.ok or "html" not in (r.headers.get("Content-Type", "").lower()):
			return []

		candidates: list[PdfCandidate] = []
		for link_url, title in parse_html_links(source.url, r.text):
			if link_url.lower().endswith(".pdf"):
				date = parse_date(f"{title} {link_url}")
				candidates.append(
					PdfCandidate(
						url=link_url,
						source_url=source.url,
						title=title or link_url,
						published_at=date,
						discovered_at=datetime.now(UTC),
					)
				)
		return candidates

	def find_latest_pdf(
		self,
		session: requests.Session,
		source: StateSource,
		timeout: int,
	) -> Optional[PdfCandidate]:
		return pick_newest(self.discover_pdf_candidates(session, source, timeout))


class BadenWuerttembergAdapter(BaseStateAdapter):
	pass


class BayernAdapter(BaseStateAdapter):
	pass


class BerlinAdapter(BaseStateAdapter):
	pass


class BrandenburgAdapter(BaseStateAdapter):
	pass


class BremenAdapter(BaseStateAdapter):
	pass


class HamburgAdapter(BaseStateAdapter):
	pass


class HessenAdapter(BaseStateAdapter):
	pass


class MecklenburgVorpommernAdapter(BaseStateAdapter):
	pass


class NiedersachsenAdapter(BaseStateAdapter):
	pass


class NordrheinWestfalenAdapter(BaseStateAdapter):
	pass


class RheinlandPfalzAdapter(BaseStateAdapter):
	pass


class SaarlandAdapter(BaseStateAdapter):
	pass


class SachsenAdapter(BaseStateAdapter):
	pass


class SachsenAnhaltAdapter(BaseStateAdapter):
	pass


class SchleswigHolsteinAdapter(BaseStateAdapter):
	pass


class ThueringenAdapter(BaseStateAdapter):
	pass


def build_state_adapters() -> dict[str, BaseStateAdapter]:
	base = BaseStateAdapter()
	return {
		"default": base,
		"baden-wuerttemberg": BadenWuerttembergAdapter(),
		"bw": BadenWuerttembergAdapter(),
		"bayern": BayernAdapter(),
		"berlin": BerlinAdapter(),
		"brandenburg": BrandenburgAdapter(),
		"bremen": BremenAdapter(),
		"hamburg": HamburgAdapter(),
		"hessen": HessenAdapter(),
		"mecklenburg-vorpommern": MecklenburgVorpommernAdapter(),
		"niedersachsen": NiedersachsenAdapter(),
		"nordrhein-westfalen": NordrheinWestfalenAdapter(),
		"rheinland-pfalz": RheinlandPfalzAdapter(),
		"saarland": SaarlandAdapter(),
		"sachsen": SachsenAdapter(),
		"sachsen-anhalt": SachsenAnhaltAdapter(),
		"schleswig-holstein": SchleswigHolsteinAdapter(),
		"thueringen": ThueringenAdapter(),
	}


def resolve_adapter(source: StateSource, registry: dict[str, BaseStateAdapter]) -> BaseStateAdapter:
	key = normalize_state_name(source.adapter_key or source.state)
	return registry.get(key, registry["default"])


def store_latest_result(
	conn: sqlite3.Connection,
	source: StateSource,
	candidate: Optional[PdfCandidate],
	result: DownloadResult,
) -> None:
	conn.execute(
		"""
		INSERT INTO latest_pdfs(
			state, adapter_key, source_url, pdf_url, title, published_at,
			local_path, status, sha256, bytes, error
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(state) DO UPDATE SET
			adapter_key=excluded.adapter_key,
			source_url=excluded.source_url,
			pdf_url=excluded.pdf_url,
			title=excluded.title,
			published_at=excluded.published_at,
			local_path=excluded.local_path,
			status=excluded.status,
			sha256=excluded.sha256,
			bytes=excluded.bytes,
			error=excluded.error,
			updated_at=CURRENT_TIMESTAMP
		""",
		(
			source.state,
			source.adapter_key,
			source.url,
			candidate.url if candidate else None,
			candidate.title if candidate else None,
			candidate.published_at.isoformat() if candidate and candidate.published_at else None,
			result.local_path,
			result.status,
			result.sha256,
			result.size_bytes,
			result.error,
		),
	)
	conn.commit()


def run_latest_per_state(
	source_file: Path,
	out_dir: Path,
	db_path: Path,
	timeout: int,
	delay: float,
	user_agent: str,
) -> None:
	sources = load_state_sources(source_file)
	conn = init_db(db_path)
	session = create_session(user_agent)
	adapters = build_state_adapters()

	run_id = conn.execute(
		"INSERT INTO ingestion_runs(source_file, total_states, ok_states, failed_states) VALUES (?, ?, 0, 0)",
		(str(source_file), len(sources)),
	).lastrowid
	conn.commit()

	ok_states = 0
	failed_states = 0

	for source in sources:
		adapter = resolve_adapter(source, adapters)
		candidate = adapter.find_latest_pdf(session, source, timeout)
		if not candidate:
			result = DownloadResult("failed", "", None, None, "no_pdf_candidate")
			store_latest_result(conn, source, None, result)
			failed_states += 1
			print(f"FAILED {source.state}: no PDF candidate found")
			continue

		state_dir = out_dir / normalize_state_name(source.state)
		result = download_pdf(session, state_dir, timeout, delay, user_agent, candidate.url)
		store_latest_result(conn, source, candidate, result)

		if result.status == "ok":
			ok_states += 1
			print(f"OK     {source.state}: {candidate.url} -> {result.local_path}")
		else:
			failed_states += 1
			print(f"FAILED {source.state}: {candidate.url} ({result.error})")

	conn.execute(
		"""
		UPDATE ingestion_runs
		SET finished_at=CURRENT_TIMESTAMP, ok_states=?, failed_states=?
		WHERE id=?
		""",
		(ok_states, failed_states, run_id),
	)
	conn.commit()


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(
		description="Download newest Bundesrat vote PDF per state using per-state adapters."
	)
	parser.add_argument("--source-file", default="urls.txt", help="Input file with state sources.")
	parser.add_argument("--out-dir", default="./latest_pdfs", help="Directory for downloaded PDFs.")
	parser.add_argument("--db-path", default="./latest_votes.sqlite3", help="SQLite database path.")
	parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout in seconds.")
	parser.add_argument("--delay", type=float, default=0.6, help="Delay between requests in seconds.")
	parser.add_argument("--user-agent", default="bundesratvotes/0.1 (+contact@example.com)")
	return parser.parse_args()


def main() -> None:
	args = parse_args()
	run_latest_per_state(
		source_file=Path(args.source_file),
		out_dir=Path(args.out_dir),
		db_path=Path(args.db_path),
		timeout=max(1, args.timeout),
		delay=max(0.0, args.delay),
		user_agent=args.user_agent,
	)


if __name__ == "__main__":
	main()

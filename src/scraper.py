import argparse
import re
from pathlib import Path
from urllib.parse import urlparse

import requests


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
    parser = argparse.ArgumentParser(description="Fetch all URLs from urls.txt and save HTML files.")
    parser.add_argument(
        "--urls-file",
        default=str(Path(__file__).with_name("urls.txt")),
        help="Path to URL list file (default: urls.txt next to this script)",
    )
    parser.add_argument(
        "--output-dir",
        default=str(Path(__file__).with_name("html_files")),
        help="Directory for saved HTML files (default: html_files next to this script)",
    )
    args = parser.parse_args()

    agent = PullPDFsAgent()
    paths = agent.fetch_all_html(args.urls_file, args.output_dir)
    print(f"Saved {len(paths)} HTML files to: {args.output_dir}")


if __name__ == "__main__":
    main()
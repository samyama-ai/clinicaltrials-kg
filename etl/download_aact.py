"""Download AACT pipe-delimited flat files from ctti-clinicaltrials.org.

Usage:
    python -m etl.download_aact                         # Auto-detect URL
    python -m etl.download_aact --url https://...       # Direct URL
    python -m etl.download_aact --output-dir data/aact  # Custom output dir
"""

import os
import re
import zipfile
from pathlib import Path

import requests

AACT_DOWNLOADS_URL = "https://aact.ctti-clinicaltrials.org/downloads"
DEFAULT_DATA_DIR = Path("data/aact")
CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB


def find_flat_files_url() -> str:
    """Scrape the AACT downloads page for the current flat files URL."""
    resp = requests.get(AACT_DOWNLOADS_URL, timeout=30)
    resp.raise_for_status()

    # DigitalOcean Spaces URLs for AACT downloads
    urls = re.findall(
        r"https://ctti-aact\.nyc3\.digitaloceanspaces\.com/[a-z0-9]+",
        resp.text,
    )
    # Find the one in a "pipe" / "flat" / "text" context
    for url in urls:
        idx = resp.text.index(url)
        context = resp.text[max(0, idx - 300) : idx + 100].lower()
        if "pipe" in context or "flat" in context or "text" in context:
            return url

    # Fallback: second URL is typically flat files (first is PostgreSQL dump)
    if len(urls) >= 2:
        return urls[1]
    if urls:
        return urls[0]
    raise RuntimeError(
        "Could not find flat files download URL. "
        "Visit https://aact.ctti-clinicaltrials.org/downloads and pass --url manually."
    )


def download_aact(url: str | None = None, output_dir: str | None = None) -> Path:
    """Download and extract AACT pipe-delimited flat files.

    Args:
        url: Direct download URL (auto-detected if None).
        output_dir: Directory to extract files into.

    Returns:
        Path to directory containing extracted .txt files.
    """
    out = Path(output_dir or DEFAULT_DATA_DIR)
    out.mkdir(parents=True, exist_ok=True)

    if url is None:
        print("Finding latest flat files URL from AACT downloads page...")
        url = find_flat_files_url()

    zip_path = out / "aact_flat_files.zip"

    print(f"Downloading AACT flat files (~2.2 GB):\n  {url}")
    resp = requests.get(url, stream=True, timeout=600)
    resp.raise_for_status()

    total = int(resp.headers.get("content-length", 0))
    downloaded = 0

    with open(zip_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
            f.write(chunk)
            downloaded += len(chunk)
            if total:
                pct = downloaded * 100 / total
                mb = downloaded / 1e6
                total_mb = total / 1e6
                print(
                    f"\r  {mb:.0f} / {total_mb:.0f} MB ({pct:.1f}%)",
                    end="",
                    flush=True,
                )
    print("\n  Download complete.")

    print(f"Extracting to {out} ...")
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(out)

    zip_path.unlink()

    txt_files = sorted(out.rglob("*.txt"))
    print(f"Extracted {len(txt_files)} files")
    return out


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Download AACT flat files")
    ap.add_argument("--url", help="Direct download URL (auto-detected if omitted)")
    ap.add_argument("--output-dir", default="data/aact", help="Output directory")
    args = ap.parse_args()
    download_aact(url=args.url, output_dir=args.output_dir)

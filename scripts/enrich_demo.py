#!/usr/bin/env python3
# scripts/enrich_demo.py
from __future__ import annotations

import argparse
import csv
import re
import time
from pathlib import Path
from typing import Dict, List, Tuple

import httpx

AIC_MUSEUM_ID = "art-institute-of-chicago-us"
MET_MUSEUM_ID = "the-metropolitan-museum-of-art-new-york-city-us"


def _read_header(csv_path: Path) -> List[str]:
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        return next(reader)


def _load_existing_keys(csv_path: Path, key_field: str) -> set:
    keys = set()
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            k = (row.get(key_field) or "").strip()
            if k:
                keys.add(k)
    return keys


def _append_rows(csv_path: Path, fieldnames: List[str], rows: List[Dict[str, str]]) -> int:
    if not rows:
        return 0
    with csv_path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        # 不写 header（因为 copies 里已经有）
        count = 0
        for r in rows:
            out = {k: (r.get(k, "") if r.get(k, "") is not None else "") for k in fieldnames}
            w.writerow(out)
            count += 1
    return count


def _slugify(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s


def _extract_year(s: str) -> str:
    if not s:
        return ""
    m = re.search(r"(1[0-9]{3}|20[0-9]{2})", s)
    return m.group(1) if m else ""


def fetch_aic_artworks(target: int) -> Tuple[List[Dict[str, str]], Dict[str, str]]:
    """
    AIC: https://api.artic.edu/api/v1/artworks
    """
    fields = ",".join([
        "id", "title", "artist_id", "artist_title",
        "classification_title", "date_start", "date_display",
        "image_id", "thumbnail", "medium_display", "dimensions",
        "is_on_view",
    ])

    rows: List[Dict[str, str]] = []
    artists: Dict[str, str] = {}

    with httpx.Client(timeout=30) as client:
        page = 1
        iiif_url = "https://www.artic.edu/iiif/2"

        while len(rows) < target:
            r = client.get(
                "https://api.artic.edu/api/v1/artworks",
                params={"fields": fields, "limit": 100, "page": page},
            )
            r.raise_for_status()
            data = r.json()

            cfg = data.get("config") or {}
            if cfg.get("iiif_url"):
                iiif_url = cfg["iiif_url"]

            items = data.get("data") or []
            if not items:
                break

            for a in items:
                image_id = a.get("image_id") or ""
                if not image_id:
                    continue

                aic_id = str(a.get("id", "")).strip()
                if not aic_id:
                    continue

                artist_src_id = a.get("artist_id")
                artist_id = f"aic-artist-{artist_src_id}" if artist_src_id else ""
                artist_name = (a.get("artist_title") or "").strip()
                if artist_id and artist_name:
                    artists[artist_id] = artist_name

                thumb = a.get("thumbnail") or {}
                alt = (thumb.get("alt_text") or "").strip()

                rows.append({
                    "artwork_id": f"aic-{aic_id}",
                    "title": (a.get("title") or "").strip(),
                    "artist_id": artist_id,
                    "art_type": (a.get("classification_title") or "").strip(),
                    "year": str(a.get("date_start") or "").strip(),
                    "image_url": f"{iiif_url}/{image_id}/full/843,/0/default.jpg",
                    "description": alt,
                    "museum_id": AIC_MUSEUM_ID,
                    "medium": (a.get("medium_display") or "").strip(),
                    "dimensions": (a.get("dimensions") or "").strip(),
                    "museum_page_url": f"https://www.artic.edu/artworks/{aic_id}",
                    "on_view": "TRUE" if a.get("is_on_view") else "FALSE",
                    "highlight": "FALSE",
                })

                if len(rows) >= target:
                    break

            page += 1
            time.sleep(0.2)

    return rows[:target], artists


def fetch_met_artworks(target: int) -> Tuple[List[Dict[str, str]], Dict[str, str]]:
    """
    MET: https://collectionapi.metmuseum.org/public/collection/v1
    这里用 search(hasImages=true) 拿一批有图的 objectIDs，再逐个 objects/{id} 拉详情。
    """
    rows: List[Dict[str, str]] = []
    artists: Dict[str, str] = {}

    with httpx.Client(timeout=30) as client:
        s = client.get(
            "https://collectionapi.metmuseum.org/public/collection/v1/search",
            params={"hasImages": "true", "q": "painting"},
        )
        s.raise_for_status()
        object_ids = (s.json().get("objectIDs") or [])[: max(target * 5, 500)]

        for oid in object_ids:
            if len(rows) >= target:
                break

            d = client.get(f"https://collectionapi.metmuseum.org/public/collection/v1/objects/{oid}")
            if d.status_code != 200:
                continue
            obj = d.json()

            img = (obj.get("primaryImageSmall") or obj.get("primaryImage") or "").strip()
            if not img:
                continue

            title = (obj.get("title") or "").strip()
            object_url = (obj.get("objectURL") or "").strip()
            if not object_url:
                continue

            artist_name = (obj.get("artistDisplayName") or "").strip()
            artist_id = f"met-artist-{_slugify(artist_name)}" if artist_name else ""
            if artist_id and artist_name:
                artists[artist_id] = artist_name

            rows.append({
                "artwork_id": f"met-{oid}",
                "title": title,
                "artist_id": artist_id,
                "art_type": (obj.get("objectName") or "").strip(),
                "year": _extract_year((obj.get("objectDate") or "").strip()),
                "image_url": img,
                "description": (obj.get("creditLine") or "").strip(),
                "museum_id": MET_MUSEUM_ID,
                "medium": (obj.get("medium") or "").strip(),
                "dimensions": (obj.get("dimensions") or "").strip(),
                "museum_page_url": object_url,
                "on_view": "TRUE" if obj.get("isOnView") else "FALSE",
                "highlight": "FALSE",
            })

            time.sleep(0.15)

    return rows[:target], artists


def upsert_artists(artists_csv: Path, new_artists: Dict[str, str]) -> int:
    """
    尽量“按你现有 artists.csv 的表头”去追加最小字段：
    - 只保证 artist_id / name 两列（如果存在）
    - 其他列留空
    """
    if not artists_csv.exists() or not new_artists:
        return 0

    header = _read_header(artists_csv)
    if "artist_id" not in header:
        return 0

    name_col = "name" if "name" in header else None
    existing = _load_existing_keys(artists_csv, "artist_id")

    rows = []
    for aid, nm in new_artists.items():
        if aid in existing:
            continue
        r = {h: "" for h in header}
        r["artist_id"] = aid
        if name_col:
            r[name_col] = nm
        rows.append(r)

    return _append_rows(artists_csv, header, rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run_dir", required=True, help="e.g. runs/run_20260215_161030")
    ap.add_argument("--target_artworks", type=int, default=100)
    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    copies = run_dir / "copies"
    artworks_csv = copies / "artworks.csv"
    artists_csv = copies / "artists.csv"

    if not artworks_csv.exists():
        raise SystemExit(f"Missing {artworks_csv}. Did you run gap_scanner first?")

    header = _read_header(artworks_csv)
    required = ["artwork_id", "title", "artist_id", "art_type", "year", "image_url",
                "description", "museum_id", "medium", "dimensions", "museum_page_url", "on_view", "highlight"]
    missing = [c for c in required if c not in header]
    if missing:
        raise SystemExit(f"artworks.csv missing columns: {missing}")

    existing_artwork_ids = _load_existing_keys(artworks_csv, "artwork_id")

    # 1) AIC
    aic_rows, aic_artists = fetch_aic_artworks(args.target_artworks)
    aic_rows = [r for r in aic_rows if r["artwork_id"] not in existing_artwork_ids]
    added_aic = _append_rows(artworks_csv, header, aic_rows)
    print(f"[AIC] added artworks: {added_aic}")

    # 2) MET
    met_rows, met_artists = fetch_met_artworks(args.target_artworks)
    met_rows = [r for r in met_rows if r["artwork_id"] not in existing_artwork_ids]
    added_met = _append_rows(artworks_csv, header, met_rows)
    print(f"[MET] added artworks: {added_met}")

    # artists（可选但强烈建议）
    added_artists = upsert_artists(artists_csv, {**aic_artists, **met_artists})
    print(f"[artists] added: {added_artists}")

    print("Done.")


if __name__ == "__main__":
    main()

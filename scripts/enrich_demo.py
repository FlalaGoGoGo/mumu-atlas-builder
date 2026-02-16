cat > scripts/enrich_demo.py <<'PY'
#!/usr/bin/env python3
# Enrich demo: read runs/<run_dir>/backlog.json -> pick museums -> call AIC/Met APIs -> write enriched CSVs
from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Set

import httpx

# local router
from mcp_server import route_source


def slugify(text: str) -> str:
    text = (text or "").strip().lower()
    text = re.sub(r"['’]", "", text)
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")


def first_year(text: str) -> str:
    """Extract the first 4-digit year from a string; return '' if not found."""
    if not text:
        return ""
    m = re.search(r"(1[0-9]{3}|20[0-9]{2})", text)
    return m.group(1) if m else ""


def read_csv_rows(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        rows = [dict(r) for r in reader]
    return fieldnames, rows


def write_csv_rows(path: Path, fieldnames: List[str], rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in fieldnames})


def ensure_schema_row(fieldnames: List[str], data: Dict[str, str]) -> Dict[str, str]:
    return {k: data.get(k, "") for k in fieldnames}


def fetch_aic_artworks(client: httpx.Client, limit: int) -> List[Dict[str, str]]:
    """
    Use AIC API list endpoint to fetch artworks with image_id.
    Returns list of normalized dicts (not yet mapped to CSV schema).
    """
    base = "https://api.artic.edu/api/v1/artworks"
    fields = ",".join([
        "id", "title", "artist_title", "date_display",
        "classification_title", "medium_display", "dimensions",
        "image_id", "is_on_view",
    ])

    collected: List[Dict[str, str]] = []
    page = 1
    per_page = 100 if limit > 100 else max(1, limit)

    while len(collected) < limit:
        resp = client.get(base, params={"page": page, "limit": per_page, "fields": fields})
        resp.raise_for_status()
        payload = resp.json()
        data = payload.get("data", []) or []
        if not data:
            break

        for it in data:
            image_id = it.get("image_id") or ""
            if not image_id:
                continue

            artwork_id = f"aic-{it.get('id')}"
            title = it.get("title") or ""
            artist_name = it.get("artist_title") or ""
            date_display = it.get("date_display") or ""
            year = first_year(date_display)
            art_type = it.get("classification_title") or ""
            medium = it.get("medium_display") or ""
            dimensions = it.get("dimensions") or ""
            on_view = "true" if it.get("is_on_view") is True else "false"

            image_url = f"https://www.artic.edu/iiif/2/{image_id}/full/843,/0/default.jpg"
            museum_page_url = f"https://www.artic.edu/artworks/{it.get('id')}"

            collected.append({
                "artwork_id": artwork_id,
                "title": title,
                "artist_name": artist_name,
                "year": year,
                "art_type": art_type,
                "image_url": image_url,
                "description": "",
                "medium": medium,
                "dimensions": dimensions,
                "museum_page_url": museum_page_url,
                "on_view": on_view,
            })

            if len(collected) >= limit:
                break

        page += 1

    return collected


def fetch_met_artworks(client: httpx.Client, limit: int) -> List[Dict[str, str]]:
    """
    Use Met Collection API:
      - GET /objects -> objectIDs
      - GET /objects/{id} -> detail
    Collect items that have primaryImageSmall.
    """
    base = "https://collectionapi.metmuseum.org/public/collection/v1"
    ids_resp = client.get(f"{base}/objects")
    ids_resp.raise_for_status()
    object_ids = ids_resp.json().get("objectIDs") or []

    collected: List[Dict[str, str]] = []
    # Iterate until we collect enough with images
    for oid in object_ids:
        if len(collected) >= limit:
            break

        detail = client.get(f"{base}/objects/{oid}")
        if detail.status_code != 200:
            continue
        it = detail.json()

        img = it.get("primaryImageSmall") or ""
        title = it.get("title") or ""
        if not img or not title:
            continue

        artwork_id = f"met-{it.get('objectID')}"
        artist_name = it.get("artistDisplayName") or ""
        date_display = it.get("objectDate") or ""
        year = first_year(date_display)
        art_type = it.get("objectName") or ""
        medium = it.get("medium") or ""
        dimensions = it.get("dimensions") or ""
        museum_page_url = it.get("objectURL") or ""

        collected.append({
            "artwork_id": artwork_id,
            "title": title,
            "artist_name": artist_name,
            "year": year,
            "art_type": art_type,
            "image_url": img,
            "description": "",
            "medium": medium,
            "dimensions": dimensions,
            "museum_page_url": museum_page_url,
            "on_view": "",  # Met API doesn't provide on_view in this endpoint
        })

    return collected


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run_dir", required=True, help="e.g. runs/run_20260215_161030")
    ap.add_argument("--target_artworks", type=int, default=100)
    ap.add_argument("--max_museums", type=int, default=2, help="how many museums to enrich from backlog")
    ap.add_argument("--museum_ids", default="", help="comma-separated museum_ids to override backlog selection")
    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    backlog_path = run_dir / "backlog.json"
    copies_dir = run_dir / "copies"

    if not backlog_path.exists():
        raise SystemExit(f"Missing backlog.json at: {backlog_path}")

    if not copies_dir.exists():
        raise SystemExit(f"Missing copies/ at: {copies_dir} (expected gap_scanner output to include copies/)")

    artworks_csv = copies_dir / "artworks.csv"
    artists_csv = copies_dir / "artists.csv"

    if not artworks_csv.exists():
        raise SystemExit(f"Missing artworks.csv at: {artworks_csv}")

    # Load backlog
    backlog = json.loads(backlog_path.read_text(encoding="utf-8"))
    backlog_list = backlog.get("top_museums_to_enrich") or []

    # Choose museums
    if args.museum_ids.strip():
        museum_ids = [m.strip() for m in args.museum_ids.split(",") if m.strip()]
    else:
        museum_ids = [x.get("museum_id") for x in backlog_list if x.get("museum_id")]
        museum_ids = museum_ids[: max(1, args.max_museums)]

    if not museum_ids:
        raise SystemExit("No museum_ids found to enrich.")

    # Load existing CSVs
    artworks_fields, artworks_rows = read_csv_rows(artworks_csv)
    existing_artwork_ids: Set[str] = {r.get("artwork_id", "") for r in artworks_rows}

    artists_fields: List[str] = []
    artists_rows: List[Dict[str, str]] = []
    existing_artist_ids: Set[str] = set()
    if artists_csv.exists():
        artists_fields, artists_rows = read_csv_rows(artists_csv)
        existing_artist_ids = {r.get("artist_id", "") for r in artists_rows}

    # Prepare HTTP client
    with httpx.Client(timeout=30.0, headers={"User-Agent": "mumu-atlas-builder-enrich-demo/1.0"}) as client:
        added_artworks: List[Dict[str, str]] = []
        added_artists: List[Dict[str, str]] = []

        for mid in museum_ids:
            plan = route_source(mid)
            source = plan.get("source", "fallback_manual")

            print(f"\n== Enriching: {mid} (source={source}) ==")

            if source == "aic_api":
                fetched = fetch_aic_artworks(client, args.target_artworks)
                museum_name_for_ids = "aic"

            elif source == "met_api":
                fetched = fetch_met_artworks(client, args.target_artworks)
                museum_name_for_ids = "met"

            else:
                print("Skip (fallback_manual): no API wiring in demo.")
                continue

            # Map to artworks.csv schema
            for it in fetched:
                aw_id = it["artwork_id"]
                if aw_id in existing_artwork_ids:
                    continue

                artist_name = it.get("artist_name", "").strip()
                artist_id = f"{museum_name_for_ids}-artist-{slugify(artist_name)}" if artist_name else ""

                row = {
                    "artwork_id": aw_id,
                    "title": it.get("title", ""),
                    "artist_id": artist_id,
                    "art_type": it.get("art_type", ""),
                    "year": it.get("year", ""),
                    "image_url": it.get("image_url", ""),
                    "description": it.get("description", ""),
                    "museum_id": mid,
                    "medium": it.get("medium", ""),
                    "dimensions": it.get("dimensions", ""),
                    "museum_page_url": it.get("museum_page_url", ""),
                    "on_view": it.get("on_view", ""),
                    "highlight": "false",
                }

                added_artworks.append(ensure_schema_row(artworks_fields, row))
                existing_artwork_ids.add(aw_id)

                # Map to artists.csv schema (only if file exists and has basic columns)
                if artists_fields and artist_id and artist_id not in existing_artist_ids:
                    base_artist = {k: "" for k in artists_fields}
                    if "artist_id" in base_artist:
                        base_artist["artist_id"] = artist_id
                    if "name" in base_artist:
                        base_artist["name"] = artist_name
                    elif "artist_name" in base_artist:
                        base_artist["artist_name"] = artist_name
                    added_artists.append(base_artist)
                    existing_artist_ids.add(artist_id)

            print(f"Fetched: {len(fetched)} | Added artworks: {len([r for r in added_artworks if r.get('museum_id') == mid])}")

    # Write outputs (do NOT overwrite copies/)
    out_dir = run_dir / "enriched"
    out_dir.mkdir(parents=True, exist_ok=True)

    new_artworks_all = artworks_rows + added_artworks
    write_csv_rows(out_dir / "artworks.csv", artworks_fields, new_artworks_all)

    if artists_fields:
        new_artists_all = artists_rows + added_artists
        write_csv_rows(out_dir / "artists.csv", artists_fields, new_artists_all)

    summary = {
        "run_dir": str(run_dir),
        "museums_enriched": museum_ids,
        "target_artworks_per_museum": args.target_artworks,
        "added_artworks": len(added_artworks),
        "added_artists": len(added_artists),
        "outputs": {
            "artworks_csv": str(out_dir / "artworks.csv"),
            "artists_csv": str(out_dir / "artists.csv") if artists_fields else None,
        },
        "note": "This demo only enriches artworks via AIC/Met APIs; exhibitions are skipped."
    }
    (out_dir / "enrich_summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    print("\n✅ Done.")
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
PY
chmod +x scripts/enrich_demo.py

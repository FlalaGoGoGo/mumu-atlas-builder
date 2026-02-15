import os, json, shutil, datetime, argparse
import pandas as pd

def pct_missing(s: pd.Series) -> float:
    if s.dtype == "object":
        empty = s.fillna("").astype(str).str.strip().eq("")
        return float(empty.mean() * 100)
    return float(s.isna().mean() * 100)

def run(seed_dir="data/seed", run_id=None,
        target_artworks_per_museum=100, target_exhibitions_per_museum=30,
        focus_museum_ids=None, focus_countries=None):

    focus_museum_ids = focus_museum_ids or []
    focus_countries = focus_countries or []

    if not run_id:
        run_id = "run_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    run_dir = os.path.join("runs", run_id)
    os.makedirs(run_dir, exist_ok=True)
    os.makedirs(os.path.join(run_dir, "copies"), exist_ok=True)

    paths = {
        "museums": os.path.join(seed_dir, "museums.csv"),
        "artworks": os.path.join(seed_dir, "artworks.csv"),
        "artists": os.path.join(seed_dir, "artists.csv"),
        "exhibitions": os.path.join(seed_dir, "exhibitions.csv"),
    }

    for k, p in paths.items():
        if not os.path.exists(p):
            raise FileNotFoundError(f"Missing required file: {p}")

    dfs = {k: pd.read_csv(p) for k, p in paths.items()}

    # snapshot
    for _, p in paths.items():
        shutil.copy2(p, os.path.join(run_dir, "copies", os.path.basename(p)))

    stats = {}
    for name, df in dfs.items():
        miss = {col: pct_missing(df[col]) for col in df.columns}
        stats[name] = {
            "rows": int(len(df)),
            "cols": int(df.shape[1]),
            "missingness_pct": dict(sorted(miss.items(), key=lambda x: x[1], reverse=True))
        }

    museums = dfs["museums"].copy()
    artworks = dfs["artworks"].copy()
    exhibitions = dfs["exhibitions"].copy()
    artists = dfs["artists"].copy()

    # normalize ids
    for df, col in [(museums, "museum_id"), (artworks, "museum_id"), (exhibitions, "museum_id")]:
        if col in df.columns:
            df[col] = df[col].astype(str)

    if "artist_id" in artworks.columns:
        artworks["artist_id"] = artworks["artist_id"].astype(str)
    if "artist_id" in artists.columns:
        artists["artist_id"] = artists["artist_id"].astype(str)

    # counts by museum
    if "museum_id" in artworks.columns:
        art_counts = artworks.groupby("museum_id").size().rename("artworks_count").reset_index()
    else:
        art_counts = pd.DataFrame(columns=["museum_id", "artworks_count"])

    if "museum_id" in exhibitions.columns:
        exh_counts = exhibitions.groupby("museum_id").size().rename("exhibitions_count").reset_index()
    else:
        exh_counts = pd.DataFrame(columns=["museum_id", "exhibitions_count"])

    if "museum_id" in museums.columns:
        m = museums.merge(art_counts, on="museum_id", how="left").merge(exh_counts, on="museum_id", how="left")
        m["artworks_count"] = m["artworks_count"].fillna(0).astype(int)
        m["exhibitions_count"] = m["exhibitions_count"].fillna(0).astype(int)
    else:
        m = pd.DataFrame()

    # artist coverage
    artist_cov = None
    if "artist_id" in artworks.columns and "artist_id" in artists.columns and len(artworks) > 0:
        known = set(artists["artist_id"].dropna().astype(str).tolist())
        total = len(artworks)
        ok = artworks["artist_id"].astype(str).isin(known).sum()
        artist_cov = {
            "artworks_total": int(total),
            "artist_match_count": int(ok),
            "artist_match_rate": float(ok / max(total, 1))
        }
    stats["artist_coverage"] = artist_cov

    # backlog scoring
    def boost(row):
        b = 0
        if focus_museum_ids and str(row.get("museum_id", "")) in set(focus_museum_ids):
            b += 100
        if focus_countries and str(row.get("country", "")) in set(focus_countries):
            b += 30
        return b

    museum_tasks = []
    if not m.empty:
        for _, row in m.iterrows():
            art_c = int(row.get("artworks_count", 0))
            exh_c = int(row.get("exhibitions_count", 0))
            score = 0
            score += max(0, target_artworks_per_museum - art_c)
            score += max(0, target_exhibitions_per_museum - exh_c) * 2
            score += boost(row)
            museum_tasks.append({
                "type": "museum_enrichment",
                "museum_id": str(row.get("museum_id", "")),
                "museum_name": str(row.get("museum_name", "")) if "museum_name" in m.columns else "",
                "country": str(row.get("country", "")) if "country" in m.columns else "",
                "artworks_count": art_c,
                "exhibitions_count": exh_c,
                "score": float(score),
                "targets": {
                    "target_artworks_per_museum": target_artworks_per_museum,
                    "target_exhibitions_per_museum": target_exhibitions_per_museum
                }
            })

    museum_tasks = sorted(museum_tasks, key=lambda x: x["score"], reverse=True)

    # column-level tasks
    column_tasks = []
    for table, meta in stats.items():
        if not isinstance(meta, dict) or "missingness_pct" not in meta:
            continue
        for col, misspct in meta["missingness_pct"].items():
            if float(misspct) >= 20:
                column_tasks.append({
                    "type": "column_fill_strategy",
                    "table": table,
                    "column": col,
                    "missingness_pct": float(misspct)
                })
    column_tasks = sorted(column_tasks, key=lambda x: x["missingness_pct"], reverse=True)

    backlog = {
        "run_id": run_id,
        "targets": {
            "target_artworks_per_museum": target_artworks_per_museum,
            "target_exhibitions_per_museum": target_exhibitions_per_museum
        },
        "top_museums_to_enrich": museum_tasks[:25],
        "high_missing_columns": column_tasks[:25]
    }

    with open(os.path.join(run_dir, "gap_report.json"), "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

    with open(os.path.join(run_dir, "backlog.json"), "w", encoding="utf-8") as f:
        json.dump(backlog, f, ensure_ascii=False, indent=2)

    # markdown report
    lines = []
    lines.append(f"# Gap Report — {run_id}\n")
    lines.append("## Table Sizes\n")
    for t in ["museums", "artworks", "artists", "exhibitions"]:
        if t in stats:
            lines.append(f"- **{t}**: {stats[t]['rows']} rows, {stats[t]['cols']} cols")

    lines.append("\n## Artist Coverage\n")
    if artist_cov:
        lines.append(f"- artworks_total: {artist_cov['artworks_total']}")
        lines.append(f"- artist_match_rate: {artist_cov['artist_match_rate']:.2%}")
    else:
        lines.append("- artist coverage could not be computed (missing artist_id columns or empty table).")

    lines.append("\n## Top Museums to Enrich (by score)\n")
    for item in backlog["top_museums_to_enrich"][:10]:
        lines.append(
            f"- {item['museum_id']} | {item['museum_name']} | artworks={item['artworks_count']} | exhibitions={item['exhibitions_count']} | score={item['score']}"
        )

    lines.append("\n## Highest Missing Columns (>=20%)\n")
    for item in backlog["high_missing_columns"][:10]:
        lines.append(f"- {item['table']}.{item['column']}: {item['missingness_pct']:.1f}%")

    with open(os.path.join(run_dir, "gap_report.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    return run_dir

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed_dir", default="data/seed")
    ap.add_argument("--run_id", default="")
    ap.add_argument("--target_artworks_per_museum", type=int, default=100)
    ap.add_argument("--target_exhibitions_per_museum", type=int, default=30)
    args = ap.parse_args()

    run_dir = run(
        seed_dir=args.seed_dir,
        run_id=args.run_id or None,
        target_artworks_per_museum=args.target_artworks_per_museum,
        target_exhibitions_per_museum=args.target_exhibitions_per_museum,
    )
    print("Wrote:", run_dir)

if __name__ == "__main__":
    main()

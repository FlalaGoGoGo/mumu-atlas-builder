# Source Router (MuMu Atlas Builder)

## Purpose
Given a `museum_id`, decide the best data source for enrichment (API-first), returning a structured plan (source, endpoints, and query templates).
This reduces manual decision-making and makes the enrichment workflow reproducible.

## Inputs
- museum_id (string)
- enrichment_goal (string; e.g., "100 artworks + 30 exhibitions")
- optional: priority_fields (list; e.g., ["description", "image_url", "start_date", "end_date", "related_artworks"])

## Output (JSON)
{
  "museum_id": "...",
  "source": "aic_api | met_api | fallback_manual",
  "artworks_plan": {
    "method": "...",
    "endpoints": ["..."],
    "notes": "..."
  },
  "exhibitions_plan": {
    "method": "...",
    "endpoints": ["..."],
    "notes": "..."
  },
  "mapping_notes": [
    "How to map fields into MuMu CSV schema",
    "How to store provenance / source URLs"
  ]
}

## Routing Rules
### 1) Art Institute of Chicago (AIC)
If `museum_id == "art-institute-of-chicago-us"`:
- source: `aic_api`
- artworks:
  - Search endpoint: `https://api.artic.edu/api/v1/artworks/search`
  - Fetch fields via `fields=` parameter (e.g., title, artist_title, date_display, medium_display, image_id, thumbnail, provenance_text, gallery_title, place_of_origin)
  - Image URL pattern: `https://www.artic.edu/iiif/2/{image_id}/full/843,/0/default.jpg`
- exhibitions:
  - AIC API coverage varies; if exhibitions are not reliably available, treat as `fallback_manual` for exhibitions only (museum site / press pages).

### 2) The Metropolitan Museum of Art (The Met)
If `museum_id == "the-metropolitan-museum-of-art-new-york-city-us"`:
- source: `met_api`
- artworks:
  - Object IDs endpoint: `https://collectionapi.metmuseum.org/public/collection/v1/objects`
  - Object detail endpoint: `https://collectionapi.metmuseum.org/public/collection/v1/objects/{objectID}`
  - Use `primaryImageSmall`, `artistDisplayName`, `title`, `objectDate`, `medium`, `culture`, `department`, `objectURL`
- exhibitions:
  - The Met API is collection-focused; exhibitions usually require a fallback (museum exhibition pages) unless another dataset is used.

### 3) Fallback
For other museums:
- source: `fallback_manual`
- recommend: Wikidata + museum official collection search pages + one trusted aggregator (only if allowed by the site).
- always store provenance URLs in output.

## Guardrails
- Prefer official museum APIs first.
- Do not fabricate missing information.
- Always output provenance / source URLs for any enriched record.

from typing import Any, Dict
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("mumu-atlas-builder")

@mcp.tool()
def route_source(museum_id: str, enrichment_goal: str = "100 artworks + 30 exhibitions") -> Dict[str, Any]:
    """
    Decide the best enrichment data source for a given museum_id.
    Returns a structured plan with endpoints and mapping notes.
    """
    museum_id = (museum_id or "").strip()

    if museum_id == "art-institute-of-chicago-us":
        return {
            "museum_id": museum_id,
            "enrichment_goal": enrichment_goal,
            "source": "aic_api",
            "artworks_plan": {
                "method": "AIC public API search + IIIF image fetch",
                "endpoints": [
                    "https://api.artic.edu/api/v1/artworks/search",
                    "https://api.artic.edu/api/v1/artworks/{id}"
                ],
                "notes": "Use fields= to request needed columns. For images use https://www.artic.edu/iiif/2/{image_id}/full/843,/0/default.jpg"
            },
            "exhibitions_plan": {
                "method": "fallback_manual",
                "endpoints": [],
                "notes": "AIC API coverage for exhibitions may be incomplete; use museum exhibition pages if needed."
            },
            "mapping_notes": [
                "Store provenance_url for each artwork (AIC artwork page or API record).",
                "Map image_id -> image_url using IIIF pattern.",
                "Normalize dates into your artworks.csv / exhibitions.csv schema."
            ]
        }

    if museum_id == "the-metropolitan-museum-of-art-new-york-city-us":
        return {
            "museum_id": museum_id,
            "enrichment_goal": enrichment_goal,
            "source": "met_api",
            "artworks_plan": {
                "method": "MET Collection API objects list + object detail",
                "endpoints": [
                    "https://collectionapi.metmuseum.org/public/collection/v1/objects",
                    "https://collectionapi.metmuseum.org/public/collection/v1/objects/{objectID}"
                ],
                "notes": "Use primaryImageSmall, artistDisplayName, title, objectDate, medium, culture, department, objectURL."
            },
            "exhibitions_plan": {
                "method": "fallback_manual",
                "endpoints": [],
                "notes": "MET API is collection-focused; exhibitions typically require museum exhibition pages or another dataset."
            },
            "mapping_notes": [
                "Store objectURL as provenance_url.",
                "Use primaryImageSmall for image_url when present.",
                "Normalize dates and artists into your schema."
            ]
        }

    return {
        "museum_id": museum_id,
        "enrichment_goal": enrichment_goal,
        "source": "fallback_manual",
        "artworks_plan": {
            "method": "wikidata + official museum collection pages",
            "endpoints": [],
            "notes": "Prefer official sources; only scrape if allowed by robots/terms."
        },
        "exhibitions_plan": {
            "method": "official museum exhibition pages",
            "endpoints": [],
            "notes": "Collect title, date range, link, and relate to artworks when possible."
        },
        "mapping_notes": [
            "Always store provenance URLs for any enriched record.",
            "Do not fabricate missing fields; leave blank if unknown."
        ]
    }

if __name__ == "__main__":
    mcp.run()

#!/usr/bin/env python3

import csv
import html
import json
import math
import re
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


INPUT_PATH = Path("/Users/ronanliedmeier/Downloads/wc_order-export-2026-04-13.csv")
OUTPUT_DIR = Path("/Users/ronanliedmeier/Documents/Klaviyo")
LIVE_CATALOG_PATH = OUTPUT_DIR / "homefans_live_product_catalog_2026-04-14.json"
LOCATION_TAXONOMY_PATH = OUTPUT_DIR / "homefans_location_taxonomy_2026-04-14.json"
VALID_STATUSES = {"Completed", "Booking confirmed"}
INTERNAL_DOMAINS = {"homefans.net", "homefans.com"}
EXCLUDED_PRODUCTS = {"Ticket Sale"}
UNMATCHABLE_GENERIC_PRODUCTS = {
    "no title 600",
    "merchandising",
    "b2b trip",
    "c",
    "footb",
    "non sporting experiences",
    "private tour add on",
}
FORCE_UNMATCHED_PRODUCTS = {
    "Ajax Matchday Experience with…",
    "Argentina National Team Experi…",
    "C.A. Tigre Matchday Experience",
    "Colombia National Team Matchda…",
    "Local Food Experience",
    "Rio de Janeiro Women’s Footbal…",
    "Eternal Derby: Dinamo Zagreb v…",
    "Lanús vs Atlético Mineiro – Co…",
    "O'Classico: FC Porto vs Benfic…",
}
ADDON_KEYWORDS = (
    "transport",
    "transportation",
    "transfer",
    "upgrade",
    "shirt",
    "jersey",
    "room",
    "hotel",
    "supplement",
    "extra night",
    "night stay",
    "add on",
    "add-on",
    "merch",
)
LOW_SIGNAL_MATCH_TOKENS = {
    "a",
    "an",
    "and",
    "at",
    "best",
    "box",
    "by",
    "cf",
    "city",
    "club",
    "day",
    "de",
    "del",
    "della",
    "derby",
    "el",
    "en",
    "eternal",
    "experience",
    "fc",
    "food",
    "football",
    "for",
    "foreigners",
    "from",
    "game",
    "games",
    "guide",
    "host",
    "inside",
    "included",
    "la",
    "las",
    "like",
    "live",
    "local",
    "long",
    "los",
    "lower",
    "match",
    "matchday",
    "middle",
    "of",
    "official",
    "on",
    "only",
    "our",
    "package",
    "person",
    "plus",
    "premium",
    "private",
    "rated",
    "road",
    "seat",
    "seats",
    "short",
    "side",
    "soccer",
    "special",
    "stadium",
    "the",
    "tickets",
    "ticket",
    "top",
    "tour",
    "true",
    "upper",
    "v",
    "vip",
    "vs",
    "watch",
    "with",
    "your",
}
RECENCY_WINDOW_DAYS = 365
UTM_PARAMS = {
    "utm_source": "klaviyo",
    "utm_medium": "email",
    "utm_campaign": "post_purchase_upsell",
}
MANUAL_LIVE_MATCHES = {
    "Fan of Platense for a day! Mat…": {
        "live_title": "Platense – Match Day Experience at the home of the Argentine champions.",
        "live_url": "https://homefans.com/product/platense-match-day-experience/",
        "live_match_confidence": "manual",
    },
    "Hotel Extra Nights (Twin)": {
        "live_title": "Extra Night (Twin Room)",
        "live_url": "https://homefans.com/product/extra-night-twin-room/",
        "live_match_confidence": "manual",
    },
    "Pumas UNAM Football Matchday E…": {
        "live_title": "Pumas UNAM Match Day Experience, Mexico City",
        "live_url": "https://homefans.com/product/pumas-unam-match-day-experience-mexico-city/",
        "live_match_confidence": "manual",
    },
    "The Eternal derby: Dinamo Buch…": {
        "live_title": "Dinamo Bucharest Matchday Experience – Step Into One of Romania’s Most Passionate Football Atmospheres!",
        "live_url": "https://homefans.com/product/dinamo-bucharest-matchday-experience-step-into-one-of-romanias-most-passionate-football-atmospheres/",
        "live_match_confidence": "manual",
    },
    "Single Room Surcharge": {
        "live_title": "Single Room Supplement",
        "live_url": "https://homefans.com/product/single-room-supplement/",
        "live_match_confidence": "manual",
    },
}


def parse_order_number(order_field: str) -> int:
    match = re.match(r"#(\d+)", order_field.strip())
    return int(match.group(1)) if match else 0


def normalize_name(order_field: str) -> str:
    name = re.sub(r"^#\d+\s*", "", order_field).strip().lower()
    return re.sub(r"\s+", " ", name)


def customer_key(row: dict[str, str]) -> str:
    email = row["Email"].strip().lower()
    if "@" in email and email.split("@")[-1] not in INTERNAL_DOMAINS:
        return f"email:{email}"

    name = normalize_name(row["Order"])
    if name:
        return f"name:{name}"

    billing = re.sub(r"\s+", " ", row["Billing"].strip().lower())
    if billing:
        return f"billing:{billing}"

    return f"order:{parse_order_number(row['Order'])}"


def parse_products(products_field: str) -> list[str]:
    # The WooCommerce export stores multiple line items in a single cell.
    # Splitting on commas is imperfect, but it is the most consistent signal
    # available in this export and surfaces add-ons/upgrades cleanly enough
    # for recommendation work.
    items = [item.strip() for item in products_field.split(",") if item.strip()]
    return list(dict.fromkeys(items))


def phi_coefficient(total_customers: int, a_customers: set[str], b_customers: set[str]) -> float:
    n11 = len(a_customers & b_customers)
    n10 = len(a_customers - b_customers)
    n01 = len(b_customers - a_customers)
    n00 = total_customers - n11 - n10 - n01
    denominator = (n11 + n10) * (n01 + n00) * (n11 + n01) * (n10 + n00)
    if denominator <= 0:
        return 0.0
    return (n11 * n00 - n10 * n01) / math.sqrt(denominator)


def lift(total_customers: int, overlap: int, a_count: int, b_count: int) -> float:
    if not total_customers or not a_count or not b_count:
        return 0.0
    return (overlap / total_customers) / ((a_count / total_customers) * (b_count / total_customers))


def normalize_catalog_text(text: str) -> str:
    text = html.unescape(text or "")
    text = text.replace("…", " ")
    for symbol in ["⭐", "🏆", "🎟", "🚐", "⚽", "🔥", "📘", "|"]:
        text = text.replace(symbol, " ")
    text = unicodedata.normalize("NFKD", text)
    text = "".join(character for character in text if not unicodedata.combining(character))
    text = text.lower().replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def distinctive_tokens(text: str) -> set[str]:
    return {
        token
        for token in normalize_catalog_text(text).split()
        if len(token) > 2 and token not in LOW_SIGNAL_MATCH_TOKENS and not token.isdigit()
    }


def is_ancillary_product(name: str) -> bool:
    normalized = normalize_catalog_text(name)
    return any(keyword in normalized for keyword in ADDON_KEYWORDS)


def is_trigger_eligible_product(name: str) -> bool:
    normalized = normalize_catalog_text(name)
    if name in EXCLUDED_PRODUCTS or name in FORCE_UNMATCHED_PRODUCTS:
        return False
    if normalized in UNMATCHABLE_GENERIC_PRODUCTS:
        return False
    if is_ancillary_product(name):
        return False
    return True


def commercial_review(
    location_proximity_label: str,
    same_order_count: int,
    later_customer_count: int,
    candidate_name: str,
) -> tuple[bool, str]:
    if location_proximity_label == "same_city":
        return True, "approved_same_city"

    if location_proximity_label == "same_country":
        return True, "approved_same_country"

    if location_proximity_label == "ancillary_bundle":
        if same_order_count > 0 or later_customer_count > 0:
            return True, "approved_ancillary_bundle"
        return False, "rejected_ancillary_without_behavior"

    if location_proximity_label == "same_continent":
        if same_order_count >= 2 or later_customer_count >= 2:
            return True, "approved_strong_regional_lifecycle"
        return False, "rejected_weak_same_continent"

    if location_proximity_label == "no_location_info":
        if same_order_count >= 2:
            return True, "approved_strong_bundle_without_location"
        if is_ancillary_product(candidate_name) and (same_order_count > 0 or later_customer_count > 0):
            return True, "approved_ancillary_without_location"
        return False, "rejected_unknown_location"

    return False, "rejected_different_region"


def append_utm(url: str, rank: int | None = None) -> str:
    if not url:
        return ""

    parsed = urlsplit(url)
    query_params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query_params.update(UTM_PARAMS)
    if rank is not None:
        query_params["utm_content"] = f"upsell_suggestion_{rank}"

    return urlunsplit(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            urlencode(query_params),
            parsed.fragment,
        )
    )


def safe_int(value: object) -> int:
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return 0


def safe_float(value: object) -> float:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return 0.0


def select_dominant_location(
    score_map: dict[str, float],
    *,
    min_score: float = 6.0,
    min_ratio: float = 1.25,
) -> str:
    if not score_map:
        return ""

    ordered = sorted(score_map.items(), key=lambda item: (item[1], item[0]), reverse=True)
    top_label, top_score = ordered[0]
    if top_score < min_score:
        return ""

    second_score = ordered[1][1] if len(ordered) > 1 else 0.0
    if second_score and top_score < second_score * min_ratio:
        return ""

    return top_label


def base_row_title(row: dict[str, object]) -> str:
    return str(row.get("base_product_live_title") or row.get("base_product") or "").strip()


def suggestion_row_title(row: dict[str, object], rank: int) -> str:
    return str(row.get(f"suggestion_{rank}_live_title") or row.get(f"suggestion_{rank}") or "").strip()


def build_geo_fill_pool(wide_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    pool_by_key: dict[str, dict[str, object]] = {}
    for row in wide_rows:
        if row.get("base_product_trigger_eligible") != "yes":
            continue

        title = base_row_title(row)
        url = str(row.get("base_product_url") or "").strip()
        if not title or not url or title in EXCLUDED_PRODUCTS:
            continue

        normalized_title = normalize_catalog_text(title)
        if not normalized_title:
            continue

        candidate = {
            "title": title,
            "normalized_title": normalized_title,
            "url": url,
            "url_confidence": str(row.get("base_product_url_confidence") or "").strip(),
            "locality": str(row.get("base_product_locality") or "").strip(),
            "country": str(row.get("base_product_country") or "").strip(),
            "continent": str(row.get("base_product_continent") or "").strip(),
            "orders_last_12m": safe_int(row.get("base_product_orders_last_12m")),
            "recent_activity_weight": safe_float(row.get("base_product_recent_activity_weight")),
            "last_seen_date": str(row.get("base_product_last_seen_date") or "").strip(),
        }
        key = url or normalized_title
        existing = pool_by_key.get(key)
        if existing is None or (
            candidate["orders_last_12m"],
            candidate["recent_activity_weight"],
            candidate["title"],
        ) > (
            existing["orders_last_12m"],
            existing["recent_activity_weight"],
            existing["title"],
        ):
            pool_by_key[key] = candidate

    return sorted(
        pool_by_key.values(),
        key=lambda candidate: (
            -candidate["orders_last_12m"],
            -candidate["recent_activity_weight"],
            candidate["title"],
        ),
    )


def apply_base_location_metadata(
    row: dict[str, object],
    *,
    live_title: str = "",
    url: str = "",
    url_confidence: str = "",
    locality: str = "",
    country: str = "",
    continent: str = "",
) -> bool:
    changed = False

    if live_title and not row.get("base_product_live_title"):
        row["base_product_live_title"] = live_title
        changed = True

    if url and not row.get("base_product_url"):
        row["base_product_url"] = url
        changed = True

    if url_confidence and not row.get("base_product_url_confidence"):
        row["base_product_url_confidence"] = url_confidence
        changed = True

    if locality and not row.get("base_product_locality"):
        row["base_product_locality"] = locality
        changed = True

    if country and not row.get("base_product_country"):
        row["base_product_country"] = country
        changed = True

    if continent and not row.get("base_product_continent"):
        row["base_product_continent"] = continent
        changed = True

    return changed


def infer_base_metadata_from_product_lookup(
    row: dict[str, object],
    product_geo_lookup: dict[str, dict[str, object]],
) -> bool:
    product_geo = product_geo_lookup.get(str(row.get("base_product") or "").strip())
    if not product_geo:
        return False

    return apply_base_location_metadata(
        row,
        live_title=str(product_geo.get("live_title") or "").strip(),
        url=str(product_geo.get("url") or "").strip(),
        url_confidence=str(product_geo.get("url_confidence") or "").strip(),
        locality=str(product_geo.get("locality") or "").strip(),
        country=str(product_geo.get("country") or "").strip(),
        continent=str(product_geo.get("continent") or "").strip(),
    )


def infer_base_metadata_from_geo_pool(row: dict[str, object], geo_pool: list[dict[str, object]]) -> None:
    if row.get("base_product_locality") and row.get("base_product_country") and row.get("base_product_url"):
        return

    base_title = base_row_title(row)
    normalized_base = normalize_catalog_text(base_title)
    if not normalized_base:
        return

    candidates = [
        candidate
        for candidate in geo_pool
        if candidate["normalized_title"] == normalized_base
        or candidate["normalized_title"].startswith(normalized_base)
        or normalized_base.startswith(candidate["normalized_title"])
    ]
    if not candidates:
        return

    best = candidates[0]
    if best["normalized_title"] != normalized_base and min(len(best["normalized_title"]), len(normalized_base)) < 14:
        return

    apply_base_location_metadata(
        row,
        live_title=best["title"],
        url=best["url"],
        url_confidence="geo_inferred",
        locality=best["locality"],
        country=best["country"],
        continent=best["continent"],
    )


def infer_base_metadata_from_behavior(
    row: dict[str, object],
    *,
    active_suggestion_products: set[str],
    product_geo_lookup: dict[str, dict[str, object]],
    same_order_pairs: Counter,
    same_order_pairs_weighted: dict[tuple[str, str], float],
    later_pairs: Counter,
    later_pairs_weighted: dict[tuple[str, str], float],
) -> bool:
    if row.get("base_product_continent"):
        return False

    base_product = str(row.get("base_product") or "").strip()
    if not base_product:
        return False

    continent_scores: dict[str, float] = defaultdict(float)
    country_scores: dict[tuple[str, str], float] = defaultdict(float)
    locality_scores: dict[tuple[str, str, str], float] = defaultdict(float)

    for candidate_product, candidate_geo in product_geo_lookup.items():
        if candidate_product == base_product or candidate_product not in active_suggestion_products:
            continue

        candidate_url = str(candidate_geo.get("url") or "").strip()
        candidate_continent = str(candidate_geo.get("continent") or "").strip()
        candidate_country = str(candidate_geo.get("country") or "").strip()
        candidate_locality = str(candidate_geo.get("locality") or "").strip()
        if not candidate_url or not candidate_continent:
            continue

        pair = tuple(sorted((base_product, candidate_product)))
        same_order_count = same_order_pairs.get(pair, 0)
        same_order_weighted = same_order_pairs_weighted.get(pair, 0.0)
        later_customer_count = later_pairs.get((base_product, candidate_product), 0)
        later_customer_weighted = later_pairs_weighted.get((base_product, candidate_product), 0.0)
        behavior_score = round(
            (same_order_weighted * 5)
            + (same_order_count * 4)
            + (later_customer_weighted * 3)
            + (later_customer_count * 2),
            6,
        )
        if behavior_score <= 0:
            continue

        continent_scores[candidate_continent] += behavior_score
        if candidate_country:
            country_scores[(candidate_continent, candidate_country)] += behavior_score
        if candidate_country and candidate_locality:
            locality_scores[(candidate_continent, candidate_country, candidate_locality)] += behavior_score

    dominant_continent = select_dominant_location(continent_scores)
    if not dominant_continent:
        return False

    dominant_country = select_dominant_location(
        {
            country: score
            for (continent, country), score in country_scores.items()
            if continent == dominant_continent
        },
        min_score=6.0,
        min_ratio=1.2,
    )
    dominant_locality = select_dominant_location(
        {
            locality: score
            for (continent, country, locality), score in locality_scores.items()
            if continent == dominant_continent and country == dominant_country
        },
        min_score=8.0,
        min_ratio=1.2,
    )

    return apply_base_location_metadata(
        row,
        locality=dominant_locality,
        country=dominant_country,
        continent=dominant_continent,
    )


def select_geo_fill_candidates(row: dict[str, object], geo_pool: list[dict[str, object]]) -> list[dict[str, object]]:
    base_title = base_row_title(row)
    base_url = str(row.get("base_product_url") or "").strip()
    base_locality = normalize_catalog_text(str(row.get("base_product_locality") or ""))
    base_country = normalize_catalog_text(str(row.get("base_product_country") or ""))
    base_continent = normalize_catalog_text(str(row.get("base_product_continent") or ""))

    existing_titles = {normalize_catalog_text(base_title)}
    existing_urls = {base_url} if base_url else set()
    for rank in range(1, 4):
        suggestion_title = suggestion_row_title(row, rank)
        suggestion_url = str(row.get(f"suggestion_{rank}_url_clean") or row.get(f"suggestion_{rank}_url") or "").strip()
        if suggestion_title:
            existing_titles.add(normalize_catalog_text(suggestion_title))
        if suggestion_url:
            existing_urls.add(suggestion_url)

    tiers: list[tuple[str, str, str, int, object]] = []
    if base_locality:
        tiers.append(
            (
                "same_city",
                "geo_destination_fill_same_city",
                "approved_geo_fill_same_city",
                0,
                lambda candidate: normalize_catalog_text(candidate["locality"]) == base_locality,
            )
        )
    if base_country:
        tiers.append(
            (
                "same_country",
                "geo_destination_fill_same_country",
                "approved_geo_fill_same_country",
                1,
                lambda candidate: normalize_catalog_text(candidate["country"]) == base_country,
            )
        )
    if base_continent:
        tiers.append(
            (
                "same_continent",
                "geo_destination_fill_same_continent",
                "approved_geo_fill_same_continent",
                3,
                lambda candidate: normalize_catalog_text(candidate["continent"]) == base_continent,
            )
        )

    needed = max(0, 3 - safe_int(row.get("available_recommendations")))
    selected: list[dict[str, object]] = []
    for location_label, basis, commercial_review, priority_bucket, predicate in tiers:
        for candidate in geo_pool:
            if len(selected) >= needed:
                return selected
            if candidate["normalized_title"] in existing_titles:
                continue
            if candidate["url"] in existing_urls:
                continue
            if not predicate(candidate):
                continue

            selected.append(
                {
                    **candidate,
                    "recommendation_basis": basis,
                    "commercial_review": commercial_review,
                    "location_proximity": location_label,
                    "location_priority_bucket": priority_bucket,
                    "confidence_flag": "low",
                }
            )
            existing_titles.add(candidate["normalized_title"])
            existing_urls.add(candidate["url"])

    return selected


def apply_geo_destination_backfills(results: dict[str, object]) -> None:
    wide_rows = results["wide_rows"]
    detailed_rows = results["detailed_rows"]
    geo_pool = build_geo_fill_pool(wide_rows)
    detailed_template = dict.fromkeys(detailed_rows[0].keys(), "") if detailed_rows else {}
    product_geo_lookup = results["product_geo_lookup"]
    same_order_pairs = results["same_order_pairs"]
    same_order_pairs_weighted = results["same_order_pairs_weighted"]
    later_pairs = results["later_pairs"]
    later_pairs_weighted = results["later_pairs_weighted"]
    active_suggestion_products = results["active_suggestion_products"]

    behavior_backed_full_three = sum(1 for row in wide_rows if safe_int(row.get("available_recommendations")) >= 3)
    geo_filled_rows = 0
    geo_filled_slots = 0
    geo_filled_to_full_three = 0
    title_or_lookup_inferred_rows = 0
    behavior_location_inferred_rows = 0

    for row in wide_rows:
        had_base_continent = bool(row.get("base_product_continent"))
        infer_base_metadata_from_product_lookup(row, product_geo_lookup)
        infer_base_metadata_from_geo_pool(row, geo_pool)
        if not had_base_continent and row.get("base_product_continent"):
            title_or_lookup_inferred_rows += 1
        if not row.get("base_product_continent") and infer_base_metadata_from_behavior(
            row,
            active_suggestion_products=active_suggestion_products,
            product_geo_lookup=product_geo_lookup,
            same_order_pairs=same_order_pairs,
            same_order_pairs_weighted=same_order_pairs_weighted,
            later_pairs=later_pairs,
            later_pairs_weighted=later_pairs_weighted,
        ):
            behavior_location_inferred_rows += 1
        available_before = safe_int(row.get("available_recommendations"))
        if available_before >= 3:
            row["needs_manual_fill"] = "no"
            row["available_recommendations"] = available_before
            continue

        geo_fill_candidates = select_geo_fill_candidates(row, geo_pool)
        if not geo_fill_candidates:
            row["needs_manual_fill"] = "yes"
            row["available_recommendations"] = available_before
            continue

        geo_filled_rows += 1
        geo_filled_slots += len(geo_fill_candidates)

        for rank_offset, candidate in enumerate(geo_fill_candidates, start=available_before + 1):
            row.update(
                {
                    f"suggestion_{rank_offset}": candidate["title"],
                    f"suggestion_{rank_offset}_basis": candidate["recommendation_basis"],
                    f"suggestion_{rank_offset}_commercial_review": candidate["commercial_review"],
                    f"suggestion_{rank_offset}_phi": "",
                    f"suggestion_{rank_offset}_lift": "",
                    f"suggestion_{rank_offset}_recency_weighted_score": round(candidate["recent_activity_weight"], 6),
                    f"suggestion_{rank_offset}_location_proximity": candidate["location_proximity"],
                    f"suggestion_{rank_offset}_locality": candidate["locality"],
                    f"suggestion_{rank_offset}_country": candidate["country"],
                    f"suggestion_{rank_offset}_continent": candidate["continent"],
                    f"suggestion_{rank_offset}_live_title": candidate["title"],
                    f"suggestion_{rank_offset}_url_clean": candidate["url"],
                    f"suggestion_{rank_offset}_url": append_utm(candidate["url"], rank_offset),
                    f"suggestion_{rank_offset}_url_match_score": "",
                    f"suggestion_{rank_offset}_url_match_gap": "",
                    f"suggestion_{rank_offset}_url_confidence": candidate["url_confidence"] or "high",
                    f"suggestion_{rank_offset}_last_seen_date": candidate["last_seen_date"],
                    f"suggestion_{rank_offset}_orders_last_12m": candidate["orders_last_12m"],
                    f"suggestion_{rank_offset}_recent_activity_weight": round(candidate["recent_activity_weight"], 6),
                    f"suggestion_{rank_offset}_later_customers": "",
                    f"suggestion_{rank_offset}_later_weighted_12m": "",
                    f"suggestion_{rank_offset}_post_purchase_rate": "",
                    f"suggestion_{rank_offset}_same_order_count": "",
                    f"suggestion_{rank_offset}_same_order_weighted_12m": "",
                    f"suggestion_{rank_offset}_confidence": candidate["confidence_flag"],
                }
            )

            if detailed_template:
                detailed_row = {**detailed_template}
                detailed_row.update(
                    {
                        "rank": rank_offset,
                        "base_product": row["base_product"],
                        "suggested_product": candidate["title"],
                        "recommendation_basis": candidate["recommendation_basis"],
                        "commercial_review": candidate["commercial_review"],
                        "location_priority_bucket": candidate["location_priority_bucket"],
                        "location_proximity": candidate["location_proximity"],
                        "priority_bucket": 98 + candidate["location_priority_bucket"],
                        "score": round(candidate["recent_activity_weight"], 6),
                        "base_customer_count": row["base_customer_count"],
                        "base_order_count": row["base_order_count"],
                        "base_product_last_seen_date": row["base_product_last_seen_date"],
                        "base_product_orders_last_12m": row["base_product_orders_last_12m"],
                        "base_product_live_title": row["base_product_live_title"],
                        "base_product_url": row["base_product_url"],
                        "base_product_url_confidence": row["base_product_url_confidence"],
                        "base_product_locality": row["base_product_locality"],
                        "base_product_country": row["base_product_country"],
                        "base_product_continent": row["base_product_continent"],
                        "suggested_product_last_seen_date": candidate["last_seen_date"],
                        "suggested_product_orders_last_12m": candidate["orders_last_12m"],
                        "suggested_product_recent_activity_weight": round(candidate["recent_activity_weight"], 6),
                        "suggested_product_live_title": candidate["title"],
                        "suggested_product_url": append_utm(candidate["url"], rank_offset),
                        "suggested_product_url_match_score": "",
                        "suggested_product_url_match_gap": "",
                        "suggested_product_url_confidence": candidate["url_confidence"] or "high",
                        "suggested_product_locality": candidate["locality"],
                        "suggested_product_country": candidate["country"],
                        "suggested_product_continent": candidate["continent"],
                        "recency_weighted_score": round(candidate["recent_activity_weight"], 6),
                        "confidence_flag": candidate["confidence_flag"],
                        "suggested_product_url_clean": candidate["url"],
                    }
                )
                detailed_rows.append(detailed_row)

        available_after = available_before + len(geo_fill_candidates)
        if available_before < 3 and available_after >= 3:
            geo_filled_to_full_three += 1
        row["available_recommendations"] = available_after
        row["needs_manual_fill"] = "yes" if available_after < 3 else "no"

    results["behavior_backed_full_three"] = behavior_backed_full_three
    results["geo_filled_rows"] = geo_filled_rows
    results["geo_filled_slots"] = geo_filled_slots
    results["geo_filled_to_full_three"] = geo_filled_to_full_three
    results["title_or_lookup_inferred_rows"] = title_or_lookup_inferred_rows
    results["behavior_location_inferred_rows"] = behavior_location_inferred_rows
    results["rows_still_needing_manual_fill"] = sum(1 for row in wide_rows if row["needs_manual_fill"] == "yes")


def load_location_taxonomy() -> dict[int, dict[str, object]]:
    if not LOCATION_TAXONOMY_PATH.exists():
        return {}

    with LOCATION_TAXONOMY_PATH.open(encoding="utf-8") as handle:
        rows = json.load(handle)

    return {row["id"]: row for row in rows}


def location_depth(location_id: int, taxonomy: dict[int, dict[str, object]]) -> int:
    depth = 0
    seen = set()
    current_id = location_id
    while current_id and current_id in taxonomy and current_id not in seen:
        seen.add(current_id)
        depth += 1
        current_id = taxonomy[current_id]["parent"]
    return depth


def build_location_profile(location_ids: list[int], taxonomy: dict[int, dict[str, object]]) -> dict[str, object]:
    continents = []
    countries = []
    locals_ = []

    for location_id in location_ids:
        node = taxonomy.get(location_id)
        if not node:
            continue

        record = {"id": node["id"], "name": node["name"], "slug": node["slug"]}
        depth = location_depth(location_id, taxonomy)
        if depth <= 1:
            continents.append(record)
        elif depth == 2:
            countries.append(record)
        else:
            locals_.append(record)

    def dedupe(records: list[dict[str, object]]) -> list[dict[str, object]]:
        seen = set()
        output = []
        for record in records:
            if record["slug"] in seen:
                continue
            seen.add(record["slug"])
            output.append(record)
        return output

    continents = dedupe(continents)
    countries = dedupe(countries)
    locals_ = dedupe(locals_)

    return {
        "continent_names": [record["name"] for record in continents],
        "continent_slugs": {record["slug"] for record in continents},
        "country_names": [record["name"] for record in countries],
        "country_slugs": {record["slug"] for record in countries},
        "local_names": [record["name"] for record in locals_],
        "local_slugs": {record["slug"] for record in locals_},
        "primary_continent": continents[0]["name"] if continents else "",
        "primary_country": countries[0]["name"] if countries else "",
        "primary_local": locals_[0]["name"] if locals_ else "",
        "has_location": bool(continents or countries or locals_),
    }


def expand_location_ids(location_ids: list[int], taxonomy: dict[int, dict[str, object]]) -> list[int]:
    expanded = []
    seen = set()
    for location_id in location_ids:
        current_id = location_id
        while current_id and current_id in taxonomy and current_id not in seen:
            seen.add(current_id)
            expanded.append(current_id)
            current_id = taxonomy[current_id]["parent"]
    return expanded


def build_location_name_index(taxonomy: dict[int, dict[str, object]]) -> list[dict[str, object]]:
    index = []
    seen = set()
    for location_id, row in taxonomy.items():
        for raw_value in {row["name"], row["slug"].replace("-", " ")}:
            normalized = normalize_catalog_text(raw_value)
            if len(normalized) < 4 or normalized in LOW_SIGNAL_MATCH_TOKENS or normalized in seen:
                continue
            seen.add(normalized)
            index.append(
                {
                    "location_id": location_id,
                    "normalized": normalized,
                    "token_count": len(normalized.split()),
                    "depth": location_depth(location_id, taxonomy),
                }
            )

    index.sort(key=lambda row: (row["token_count"], row["depth"], len(row["normalized"])), reverse=True)
    return index


def infer_location_profile_from_name(
    name: str,
    taxonomy: dict[int, dict[str, object]],
    location_name_index: list[dict[str, object]],
) -> dict[str, object]:
    normalized_name = normalize_catalog_text(name)
    if not normalized_name or not taxonomy:
        return build_location_profile([], taxonomy)

    padded_name = f" {normalized_name} "
    matched_ids = []
    for row in location_name_index:
        pattern = f" {row['normalized']} "
        if pattern in padded_name:
            matched_ids.append(row["location_id"])

    return build_location_profile(expand_location_ids(matched_ids, taxonomy), taxonomy)


def location_compatibility_level(
    source_profile: dict[str, object],
    candidate_profile: dict[str, object],
) -> int:
    if not source_profile["has_location"] or not candidate_profile["has_location"]:
        return 0

    if source_profile["local_slugs"] & candidate_profile["local_slugs"]:
        return 3
    if source_profile["country_slugs"] & candidate_profile["country_slugs"]:
        return 2
    if source_profile["continent_slugs"] & candidate_profile["continent_slugs"]:
        return 1
    return -1


def recency_weight(order_date: datetime, analysis_end_date: datetime, window_start: datetime) -> float:
    if order_date < window_start:
        return 0.35

    window_days = max((analysis_end_date - window_start).days, 1)
    age_days = max((analysis_end_date - order_date).days, 0)
    freshness = max(0.0, 1 - (age_days / window_days))
    return round(1.0 + freshness, 6)


def location_proximity(
    base_profile: dict[str, object],
    candidate_profile: dict[str, object],
    candidate_name: str,
    same_order_count: int,
    later_customer_count: int,
) -> tuple[int, str]:
    if not base_profile["has_location"]:
        if is_ancillary_product(candidate_name) and (same_order_count > 0 or later_customer_count > 0):
            return 2, "ancillary_bundle"
        return 4, "no_location_info"

    if (
        base_profile["local_slugs"]
        and candidate_profile["local_slugs"]
        and base_profile["local_slugs"] & candidate_profile["local_slugs"]
    ):
        return 0, "same_city"

    if (
        base_profile["country_slugs"]
        and candidate_profile["country_slugs"]
        and base_profile["country_slugs"] & candidate_profile["country_slugs"]
    ):
        return 1, "same_country"

    if is_ancillary_product(candidate_name) and (same_order_count > 0 or later_customer_count > 0):
        return 2, "ancillary_bundle"

    if (
        base_profile["continent_slugs"]
        and candidate_profile["continent_slugs"]
        and base_profile["continent_slugs"] & candidate_profile["continent_slugs"]
    ):
        return 3, "same_continent"

    if not candidate_profile["has_location"]:
        return 4, "no_location_info"

    return 5, "different_region"


def load_live_catalog(location_taxonomy: dict[int, dict[str, object]]) -> list[dict[str, object]]:
    if not LIVE_CATALOG_PATH.exists():
        return []

    with LIVE_CATALOG_PATH.open(encoding="utf-8") as handle:
        raw_catalog = json.load(handle)

    catalog = []
    for item in raw_catalog:
        title = html.unescape(item.get("title", {}).get("rendered", ""))
        url = item.get("link", "")
        if not title or not url:
            continue
        location_ids = item.get("location", []) or []
        location_profile = build_location_profile(expand_location_ids(location_ids, location_taxonomy), location_taxonomy)
        catalog.append(
            {
                "title": title,
                "norm": normalize_catalog_text(title),
                "slug": item.get("slug", ""),
                "url": url,
                "location_ids": location_ids,
                "location_profile": location_profile,
            }
        )
    return catalog


def match_live_product(
    name: str,
    live_catalog: list[dict[str, object]],
    source_location_profile: dict[str, object],
) -> dict[str, object] | None:
    def build_match_payload(
        item: dict[str, object],
        confidence: str,
        score: float | str,
        gap: float | str,
    ) -> dict[str, object]:
        return {
            "live_title": item["title"],
            "live_url": item["url"],
            "live_match_score": score,
            "live_match_gap": gap,
            "live_match_confidence": confidence,
            "live_location_local": item["location_profile"]["primary_local"],
            "live_location_country": item["location_profile"]["primary_country"],
            "live_location_continent": item["location_profile"]["primary_continent"],
            "live_location_has_data": item["location_profile"]["has_location"],
            "live_location_profile": item["location_profile"],
        }

    if name in MANUAL_LIVE_MATCHES:
        for item in live_catalog:
            if item["url"] == MANUAL_LIVE_MATCHES[name]["live_url"]:
                return build_match_payload(
                    item=item,
                    confidence=MANUAL_LIVE_MATCHES[name]["live_match_confidence"],
                    score="",
                    gap="",
                )
        return None

    if name in FORCE_UNMATCHED_PRODUCTS:
        return None

    if not live_catalog:
        return None

    normalized_name = normalize_catalog_text(name)
    if not normalized_name:
        return None
    if normalized_name in {normalize_catalog_text(product) for product in EXCLUDED_PRODUCTS}:
        return None
    if normalized_name in UNMATCHABLE_GENERIC_PRODUCTS:
        return None

    candidate_tokens = {token for token in normalized_name.split() if len(token) > 1}
    source_signal_tokens = distinctive_tokens(name)
    source_has_fixture_marker = bool(re.search(r"\b(v|vs)\b", normalized_name))
    scored_matches = []

    for item in live_catalog:
        item_tokens = set(item["norm"].split())
        item_signal_tokens = distinctive_tokens(item["title"])
        item_has_fixture_marker = bool(re.search(r"\b(v|vs)\b", item["norm"]))
        similarity_ratio = SequenceMatcher(None, normalized_name, item["norm"]).ratio()
        slug_ratio = SequenceMatcher(None, normalized_name.replace(" ", "-"), item["slug"]).ratio()
        overlap_count = len(candidate_tokens & item_tokens)
        overlap_ratio = overlap_count / max(1, len(candidate_tokens))
        signal_overlap = len(source_signal_tokens & item_signal_tokens)
        signal_coverage = signal_overlap / max(1, len(source_signal_tokens)) if source_signal_tokens else 0.0
        signal_missing_ratio = (
            len(source_signal_tokens - item_signal_tokens) / max(1, len(source_signal_tokens))
            if source_signal_tokens
            else 0.0
        )
        compatibility_level = location_compatibility_level(source_location_profile, item["location_profile"])

        if (
            source_location_profile["has_location"]
            and item["location_profile"]["has_location"]
            and compatibility_level < 0
        ):
            continue

        if source_signal_tokens and signal_overlap == 0 and not is_ancillary_product(name):
            continue
        if source_has_fixture_marker and not item_has_fixture_marker:
            continue

        prefix_length = 0
        for left, right in zip(normalized_name, item["norm"]):
            if left == right:
                prefix_length += 1
            else:
                break
        prefix_bonus = min(prefix_length, 24) / 24
        contains_bonus = int(normalized_name in item["norm"] or item["norm"] in normalized_name)

        score = (
            similarity_ratio
            + (overlap_count * 0.08)
            + (overlap_ratio * 0.25)
            + (contains_bonus * 0.2)
            + (prefix_bonus * 0.25)
            + (slug_ratio * 0.15)
            + (signal_overlap * 0.18)
            + (signal_coverage * 0.4)
            + (max(compatibility_level, 0) * 0.18)
            - (signal_missing_ratio * 0.12)
        )
        scored_matches.append(
            (
                score,
                similarity_ratio,
                slug_ratio,
                overlap_count,
                overlap_ratio,
                signal_overlap,
                signal_coverage,
                compatibility_level,
                item,
            )
        )

    if not scored_matches:
        return None

    scored_matches.sort(key=lambda row: (row[0], row[5], row[7], row[2], row[3], row[1]), reverse=True)
    top_match = scored_matches[0]
    second_match = scored_matches[1] if len(scored_matches) > 1 else None
    second_score = second_match[0] if second_match else 0.0
    gap = top_match[0] - second_score
    same_top_title = (
        second_match is not None
        and normalize_catalog_text(top_match[8]["title"]) == normalize_catalog_text(second_match[8]["title"])
    )
    exact_match = normalized_name == top_match[8]["norm"]

    is_good_match = False
    confidence = ""
    if exact_match:
        is_good_match = True
        confidence = "high"
    elif top_match[0] >= 1.6 and top_match[5] >= 1 and (gap >= 0.03 or same_top_title):
        is_good_match = True
        confidence = "high"
    elif top_match[0] >= 1.25 and top_match[5] >= 1 and (gap >= 0.08 or top_match[3] >= 3 or top_match[4] >= 0.75):
        is_good_match = True
        confidence = "medium"
    elif top_match[0] >= 1.05 and top_match[5] >= 1 and top_match[4] >= 0.75 and gap >= 0.1:
        is_good_match = True
        confidence = "medium"

    if not is_good_match:
        return None

    return build_match_payload(
        item=top_match[8],
        confidence=confidence,
        score=round(top_match[0], 3),
        gap=round(gap, 3),
    )


def load_orders() -> tuple[list[dict], list[str], list[str]]:
    orders = []
    all_dates = []
    statuses = []

    with INPUT_PATH.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            statuses.append(row["Status"].strip())
            products = parse_products(row["Products"])
            if row["Status"].strip() not in VALID_STATUSES or not products:
                continue

            order_date = datetime.strptime(row["Date"].strip(), "%b %d, %Y")
            all_dates.append(order_date.strftime("%Y-%m-%d"))
            orders.append(
                {
                    "customer": customer_key(row),
                    "date": order_date,
                    "order_number": parse_order_number(row["Order"]),
                    "products": products,
                }
            )

    return orders, all_dates, statuses


def build_recommendations() -> dict[str, object]:
    orders, usable_dates, statuses = load_orders()
    analysis_end_date = max(order["date"] for order in orders)
    suggestion_window_start = analysis_end_date - timedelta(days=RECENCY_WINDOW_DAYS)
    location_taxonomy = load_location_taxonomy()
    location_name_index = build_location_name_index(location_taxonomy)
    live_catalog = load_live_catalog(location_taxonomy)
    live_match_cache: dict[str, dict[str, object] | None] = {}
    inferred_location_cache: dict[str, dict[str, object]] = {}

    def get_inferred_location_profile(product_name: str) -> dict[str, object]:
        if product_name not in inferred_location_cache:
            inferred_location_cache[product_name] = infer_location_profile_from_name(
                product_name,
                location_taxonomy,
                location_name_index,
            )
        return inferred_location_cache[product_name]

    def get_live_match(product_name: str) -> dict[str, object] | None:
        if product_name not in live_match_cache:
            live_match_cache[product_name] = match_live_product(
                product_name,
                live_catalog,
                get_inferred_location_profile(product_name),
            )
        return live_match_cache[product_name]

    customers = defaultdict(list)
    orders_with_product = Counter()
    same_order_pairs = Counter()
    same_order_pairs_weighted = defaultdict(float)
    product_customers = defaultdict(set)
    product_first_seen = {}
    product_last_seen = {}
    product_orders_last_12m = Counter()
    product_recent_activity_weight = defaultdict(float)
    product_customers_last_12m = defaultdict(set)
    first_purchase_position = defaultdict(dict)

    for order in orders:
        customers[order["customer"]].append(order)

    for customer_orders in customers.values():
        customer_orders.sort(key=lambda row: (row["date"], row["order_number"]))
        for order_index, order in enumerate(customer_orders):
            products = order["products"]
            order_weight = recency_weight(order["date"], analysis_end_date, suggestion_window_start)
            for product in products:
                product_customers[product].add(order["customer"])
                first_purchase_position[order["customer"]].setdefault(
                    product,
                    {"index": order_index, "date": order["date"]},
                )
                orders_with_product[product] += 1
                product_first_seen.setdefault(product, order["date"])
                product_last_seen[product] = max(product_last_seen.get(product, order["date"]), order["date"])
                if order["date"] >= suggestion_window_start:
                    product_orders_last_12m[product] += 1
                    product_recent_activity_weight[product] += order_weight
                    product_customers_last_12m[product].add(order["customer"])

            for i, left in enumerate(products):
                for right in products[i + 1 :]:
                    if left == right:
                        continue
                    pair = tuple(sorted((left, right)))
                    same_order_pairs[pair] += 1
                    same_order_pairs_weighted[pair] += order_weight

    later_pairs = Counter()
    later_pairs_weighted = defaultdict(float)
    for purchases in first_purchase_position.values():
        items = list(purchases.items())
        for base_product, base_info in items:
            for candidate_product, candidate_info in items:
                if base_product == candidate_product:
                    continue
                if base_info["index"] < candidate_info["index"]:
                    later_pairs[(base_product, candidate_product)] += 1
                    later_pairs_weighted[(base_product, candidate_product)] += recency_weight(
                        candidate_info["date"],
                        analysis_end_date,
                        suggestion_window_start,
                    )

    total_customers = len(customers)
    products = sorted(product_customers, key=lambda product: (-len(product_customers[product]), product))
    active_suggestion_products = {
        product for product, last_seen in product_last_seen.items() if last_seen >= suggestion_window_start
    }
    product_geo_lookup = {}
    for product in products:
        live_match = get_live_match(product)
        inferred_profile = get_inferred_location_profile(product)
        resolved_profile = (
            live_match["live_location_profile"]
            if live_match and live_match.get("live_location_profile")
            else inferred_profile
        )
        product_geo_lookup[product] = {
            "live_title": live_match["live_title"] if live_match else "",
            "url": live_match["live_url"] if live_match else "",
            "url_confidence": live_match["live_match_confidence"] if live_match else "",
            "locality": resolved_profile["primary_local"],
            "country": resolved_profile["primary_country"],
            "continent": resolved_profile["primary_continent"],
        }

    detailed_rows = []
    wide_rows = []

    for base_product in products:
        if base_product in EXCLUDED_PRODUCTS:
            continue
        base_customers = product_customers[base_product]
        base_customer_count = len(base_customers)
        base_order_count = orders_with_product[base_product]
        base_last_seen = product_last_seen[base_product]
        base_live_match = get_live_match(base_product)
        base_geo = product_geo_lookup[base_product]
        base_trigger_eligible = is_trigger_eligible_product(base_product)
        base_inferred_location_profile = get_inferred_location_profile(base_product)
        base_location_profile = (
            base_live_match["live_location_profile"]
            if base_live_match and base_live_match.get("live_location_profile")
            else base_inferred_location_profile
        )
        candidates = []

        for candidate_product in (products if base_trigger_eligible else []):
            if candidate_product == base_product:
                continue
            if candidate_product in EXCLUDED_PRODUCTS:
                continue
            if candidate_product in FORCE_UNMATCHED_PRODUCTS:
                continue
            if candidate_product not in active_suggestion_products:
                continue

            candidate_live_match = get_live_match(candidate_product)
            if live_catalog and candidate_live_match is None:
                continue
            candidate_inferred_location_profile = get_inferred_location_profile(candidate_product)
            candidate_location_profile = (
                candidate_live_match["live_location_profile"]
                if candidate_live_match and candidate_live_match.get("live_location_profile")
                else candidate_inferred_location_profile
            )

            candidate_customers = product_customers[candidate_product]
            overlap_customers = len(base_customers & candidate_customers)
            later_customer_count = later_pairs[(base_product, candidate_product)]
            same_order_count = same_order_pairs.get(tuple(sorted((base_product, candidate_product))), 0)
            same_order_weighted_12m = round(
                same_order_pairs_weighted.get(tuple(sorted((base_product, candidate_product))), 0.0),
                6,
            )
            later_customer_weighted_12m = round(later_pairs_weighted[(base_product, candidate_product)], 6)
            phi = phi_coefficient(total_customers, base_customers, candidate_customers)

            if not (later_customer_count > 0 or same_order_count > 0 or (overlap_customers > 0 and phi > 0)):
                continue

            location_priority_bucket, location_proximity_label = location_proximity(
                base_profile=base_location_profile,
                candidate_profile=candidate_location_profile,
                candidate_name=candidate_product,
                same_order_count=same_order_count,
                later_customer_count=later_customer_count,
            )
            commercial_approved, commercial_reason = commercial_review(
                location_proximity_label=location_proximity_label,
                same_order_count=same_order_count,
                later_customer_count=later_customer_count,
                candidate_name=candidate_product,
            )
            if not commercial_approved:
                continue

            has_behavioral_signal = later_customer_count > 0 or same_order_count > 0
            if base_location_profile["has_location"] and not has_behavioral_signal and location_priority_bucket >= 3:
                continue
            if (
                base_location_profile["has_location"]
                and location_priority_bucket == 5
                and same_order_count < 2
                and later_customer_count < 2
            ):
                continue

            relation_lift = lift(
                total_customers=total_customers,
                overlap=overlap_customers,
                a_count=base_customer_count,
                b_count=len(candidate_customers),
            )
            post_purchase_rate = later_customer_count / base_customer_count if base_customer_count else 0.0
            same_order_rate = same_order_count / base_order_count if base_order_count else 0.0
            overlap_rate = overlap_customers / base_customer_count if base_customer_count else 0.0
            candidate_recent_customer_count = len(product_customers_last_12m[candidate_product])
            candidate_last_seen_days = max((analysis_end_date - product_last_seen[candidate_product]).days, 0)
            candidate_freshness = round(max(0.0, 1 - (candidate_last_seen_days / 120)), 6)

            if (same_order_count >= 2 or same_order_weighted_12m >= 2.5) and phi > 0:
                priority_bucket = 0
            elif (later_customer_count >= 2 or later_customer_weighted_12m >= 2.5) and phi > 0:
                priority_bucket = 1
            elif same_order_count > 0:
                priority_bucket = 2
            elif later_customer_count > 0 and phi > 0:
                priority_bucket = 3
            elif overlap_customers > 0 and phi > 0 and location_priority_bucket <= 1:
                priority_bucket = 4
            else:
                priority_bucket = 5

            score = round(
                (same_order_weighted_12m * 5.5)
                + (same_order_count * 4.0)
                + (later_customer_weighted_12m * 5.0)
                + (later_customer_count * 4.0)
                + (max(phi, 0) * 12)
                + (overlap_customers * 1.0)
                + (math.log1p(max(relation_lift, 0)) * 2.5)
                + (product_recent_activity_weight[candidate_product] * 0.4)
                + (candidate_recent_customer_count * 0.35)
                + (candidate_freshness * 2.0),
                6,
            )

            if priority_bucket in {0, 2}:
                basis = "same_order_bundle"
            elif priority_bucket in {1, 3, 5} and later_customer_count > 0:
                basis = "later_lifecycle"
            else:
                basis = "lifetime_overlap"

            row = {
                "base_product": base_product,
                "suggested_product": candidate_product,
                "recommendation_basis": basis,
                "commercial_review": commercial_reason,
                "location_priority_bucket": location_priority_bucket,
                "location_proximity": location_proximity_label,
                "priority_bucket": priority_bucket,
                "score": score,
                "base_customer_count": base_customer_count,
                "base_order_count": base_order_count,
                "base_product_last_seen_date": base_last_seen.strftime("%Y-%m-%d"),
                "base_product_orders_last_12m": product_orders_last_12m[base_product],
                "base_product_live_title": base_geo["live_title"],
                "base_product_url": base_geo["url"],
                "base_product_url_confidence": base_geo["url_confidence"],
                "base_product_locality": base_geo["locality"],
                "base_product_country": base_geo["country"],
                "base_product_continent": base_geo["continent"],
                "suggested_customer_count": len(candidate_customers),
                "suggested_product_last_seen_date": product_last_seen[candidate_product].strftime("%Y-%m-%d"),
                "suggested_product_orders_last_12m": product_orders_last_12m[candidate_product],
                "suggested_product_recent_activity_weight": round(
                    product_recent_activity_weight[candidate_product],
                    6,
                ),
                "suggested_product_live_title": candidate_live_match["live_title"] if candidate_live_match else "",
                "suggested_product_url": candidate_live_match["live_url"] if candidate_live_match else "",
                "suggested_product_url_match_score": (
                    candidate_live_match["live_match_score"] if candidate_live_match else ""
                ),
                "suggested_product_url_match_gap": (
                    candidate_live_match["live_match_gap"] if candidate_live_match else ""
                ),
                "suggested_product_url_confidence": (
                    candidate_live_match["live_match_confidence"] if candidate_live_match else ""
                ),
                "suggested_product_locality": (
                    candidate_live_match["live_location_local"] if candidate_live_match else ""
                ),
                "suggested_product_country": (
                    candidate_live_match["live_location_country"] if candidate_live_match else ""
                ),
                "suggested_product_continent": (
                    candidate_live_match["live_location_continent"] if candidate_live_match else ""
                ),
                "overlap_customer_count": overlap_customers,
                "later_customer_count": later_customer_count,
                "later_customer_weighted_12m": later_customer_weighted_12m,
                "post_purchase_rate": round(post_purchase_rate, 6),
                "same_order_count": same_order_count,
                "same_order_weighted_12m": same_order_weighted_12m,
                "same_order_rate": round(same_order_rate, 6),
                "phi_coefficient": round(phi, 6),
                "lift": round(relation_lift, 6),
                "recency_weighted_score": score,
                "confidence_flag": (
                    "high"
                    if ((same_order_count >= 2 or same_order_weighted_12m >= 2.5) and phi > 0)
                    or ((later_customer_count >= 2 or later_customer_weighted_12m >= 2.5) and phi > 0)
                    else "medium"
                    if (same_order_count > 0 and phi > 0)
                    or (later_customer_count > 0 and phi > 0)
                    or (overlap_customers >= 2 and location_priority_bucket <= 1)
                    else "low"
                ),
            }
            candidates.append(row)

        candidates.sort(
            key=lambda row: (
                row["location_priority_bucket"],
                row["priority_bucket"],
                -row["recency_weighted_score"],
                -row["same_order_weighted_12m"],
                -row["later_customer_weighted_12m"],
                -row["suggested_product_recent_activity_weight"],
                -row["same_order_count"],
                -row["later_customer_count"],
                -row["phi_coefficient"],
                -row["overlap_customer_count"],
                -row["lift"],
                row["suggested_product"],
            )
        )

        top_three = candidates[:3]
        for rank, candidate in enumerate(top_three, start=1):
            detailed_candidate = {"rank": rank, **candidate}
            detailed_candidate["suggested_product_url_clean"] = candidate["suggested_product_url"]
            detailed_candidate["suggested_product_url"] = append_utm(candidate["suggested_product_url"], rank)
            detailed_rows.append(detailed_candidate)

        wide_row = {
            "base_product": base_product,
            "base_product_trigger_eligible": "yes" if base_trigger_eligible else "no",
            "base_customer_count": base_customer_count,
            "base_order_count": base_order_count,
            "base_product_last_seen_date": base_last_seen.strftime("%Y-%m-%d"),
            "base_product_orders_last_12m": product_orders_last_12m[base_product],
            "base_product_recent_activity_weight": round(product_recent_activity_weight[base_product], 6),
            "base_product_live_title": base_geo["live_title"],
            "base_product_url": base_geo["url"],
            "base_product_url_confidence": base_geo["url_confidence"],
            "base_product_locality": base_geo["locality"],
            "base_product_country": base_geo["country"],
            "base_product_continent": base_geo["continent"],
            "available_recommendations": len(top_three),
            "needs_manual_fill": "yes" if len(top_three) < 3 else "no",
        }

        for rank in range(1, 4):
            if rank <= len(top_three):
                candidate = top_three[rank - 1]
                wide_row.update(
                    {
                        f"suggestion_{rank}": candidate["suggested_product"],
                        f"suggestion_{rank}_basis": candidate["recommendation_basis"],
                        f"suggestion_{rank}_commercial_review": candidate["commercial_review"],
                        f"suggestion_{rank}_phi": candidate["phi_coefficient"],
                        f"suggestion_{rank}_lift": candidate["lift"],
                        f"suggestion_{rank}_recency_weighted_score": candidate["recency_weighted_score"],
                        f"suggestion_{rank}_location_proximity": candidate["location_proximity"],
                        f"suggestion_{rank}_locality": candidate["suggested_product_locality"],
                        f"suggestion_{rank}_country": candidate["suggested_product_country"],
                        f"suggestion_{rank}_continent": candidate["suggested_product_continent"],
                        f"suggestion_{rank}_live_title": candidate["suggested_product_live_title"],
                        f"suggestion_{rank}_url_clean": candidate["suggested_product_url"],
                        f"suggestion_{rank}_url": append_utm(candidate["suggested_product_url"], rank),
                        f"suggestion_{rank}_url_match_score": candidate["suggested_product_url_match_score"],
                        f"suggestion_{rank}_url_match_gap": candidate["suggested_product_url_match_gap"],
                        f"suggestion_{rank}_url_confidence": candidate["suggested_product_url_confidence"],
                        f"suggestion_{rank}_last_seen_date": candidate["suggested_product_last_seen_date"],
                        f"suggestion_{rank}_orders_last_12m": candidate["suggested_product_orders_last_12m"],
                        f"suggestion_{rank}_recent_activity_weight": candidate["suggested_product_recent_activity_weight"],
                        f"suggestion_{rank}_later_customers": candidate["later_customer_count"],
                        f"suggestion_{rank}_later_weighted_12m": candidate["later_customer_weighted_12m"],
                        f"suggestion_{rank}_post_purchase_rate": candidate["post_purchase_rate"],
                        f"suggestion_{rank}_same_order_count": candidate["same_order_count"],
                        f"suggestion_{rank}_same_order_weighted_12m": candidate["same_order_weighted_12m"],
                        f"suggestion_{rank}_confidence": candidate["confidence_flag"],
                    }
                )
            else:
                wide_row.update(
                    {
                        f"suggestion_{rank}": "",
                        f"suggestion_{rank}_basis": "",
                        f"suggestion_{rank}_commercial_review": "",
                        f"suggestion_{rank}_phi": "",
                        f"suggestion_{rank}_lift": "",
                        f"suggestion_{rank}_recency_weighted_score": "",
                        f"suggestion_{rank}_location_proximity": "",
                        f"suggestion_{rank}_locality": "",
                        f"suggestion_{rank}_country": "",
                        f"suggestion_{rank}_continent": "",
                        f"suggestion_{rank}_live_title": "",
                        f"suggestion_{rank}_url_clean": "",
                        f"suggestion_{rank}_url": "",
                        f"suggestion_{rank}_url_match_score": "",
                        f"suggestion_{rank}_url_match_gap": "",
                        f"suggestion_{rank}_url_confidence": "",
                        f"suggestion_{rank}_last_seen_date": "",
                        f"suggestion_{rank}_orders_last_12m": "",
                        f"suggestion_{rank}_recent_activity_weight": "",
                        f"suggestion_{rank}_later_customers": "",
                        f"suggestion_{rank}_later_weighted_12m": "",
                        f"suggestion_{rank}_post_purchase_rate": "",
                        f"suggestion_{rank}_same_order_count": "",
                        f"suggestion_{rank}_same_order_weighted_12m": "",
                        f"suggestion_{rank}_confidence": "",
                    }
                )

        wide_rows.append(wide_row)

    return {
        "orders": orders,
        "customers": customers,
        "products": products,
        "detailed_rows": detailed_rows,
        "wide_rows": wide_rows,
        "statuses": statuses,
        "usable_dates": usable_dates,
        "analysis_end_date": analysis_end_date,
        "suggestion_window_start": suggestion_window_start,
        "active_suggestion_products": active_suggestion_products,
        "product_geo_lookup": product_geo_lookup,
        "same_order_pairs": same_order_pairs,
        "same_order_pairs_weighted": same_order_pairs_weighted,
        "later_pairs": later_pairs,
        "later_pairs_weighted": later_pairs_weighted,
        "live_catalog_count": len(live_catalog),
        "location_taxonomy_count": len(location_taxonomy),
    }


def write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_summary(results: dict[str, object]) -> None:
    orders = results["orders"]
    customers = results["customers"]
    products = results["products"]
    wide_rows = results["wide_rows"]
    usable_dates = sorted(results["usable_dates"])
    analysis_end_date = results["analysis_end_date"]
    suggestion_window_start = results["suggestion_window_start"]
    active_suggestion_products = results["active_suggestion_products"]
    live_catalog_count = results["live_catalog_count"]
    location_taxonomy_count = results["location_taxonomy_count"]
    behavior_backed_full_three = results.get("behavior_backed_full_three", 0)
    geo_filled_rows = results.get("geo_filled_rows", 0)
    geo_filled_slots = results.get("geo_filled_slots", 0)
    geo_filled_to_full_three = results.get("geo_filled_to_full_three", 0)
    behavior_location_inferred_rows = results.get("behavior_location_inferred_rows", 0)
    rows_still_needing_manual_fill = results.get("rows_still_needing_manual_fill", 0)
    inferred_location_without_url_rows = sum(
        1 for row in wide_rows if row.get("base_product_continent") and not row.get("base_product_url")
    )

    rows_needing_manual_fill = sum(1 for row in wide_rows if row["needs_manual_fill"] == "yes")
    rows_with_full_three = len(wide_rows) - rows_needing_manual_fill
    live_snapshot_date = LIVE_CATALOG_PATH.stem.rsplit("_", 1)[-1]

    summary = f"""# Homefans Klaviyo Upsell Analysis

Generated using live Homefans snapshot {live_snapshot_date} from:
- `{INPUT_PATH}`

## Usable data
- Orders used: {len(orders)}
- Customer identities used: {len(customers)}
- Distinct parsed products/items: {len(products)}
- Product coverage window in this export: {usable_dates[0]} to {usable_dates[-1]}
- Suggestion activity window enforced: {suggestion_window_start.strftime("%Y-%m-%d")} to {analysis_end_date.strftime("%Y-%m-%d")}
- Products eligible to appear as suggestions: {len(active_suggestion_products)}
- Live Homefans catalog snapshot used for URL matching: {live_catalog_count} published products
- Homefans location taxonomy snapshot used for proximity scoring: {location_taxonomy_count} location terms
- Generic product explicitly excluded from all recommendations: Ticket Sale

## Method
1. Kept only `Completed` and `Booking confirmed` orders with a populated `Products` field.
2. Used email as the customer key for normal customers, but mapped `@homefans.net` and `@homefans.com` orders to the guest name so manual bookings did not collapse into operational inboxes.
3. Parsed the `Products` cell into individual items by comma to capture matchday products plus common upgrades/add-ons.
4. Calculated three signals for every product pair:
   - `later_customer_count`: customers who bought the suggested product on a later order after first buying the base product.
   - `same_order_count`: orders where both items appeared together.
   - `phi_coefficient`: customer-level binary correlation between the two products across the usable dataset.
5. Added a recency multiplier inside the last-12-month window so the most recent orders contribute more than older orders when ranking candidate upsells.
6. Allowed a product to appear as a suggested upsell only if it had at least one sale inside the last-12-month activity window above.
7. Removed `Ticket Sale` from both base-product and suggested-product consideration because it is a generic placeholder rather than a meaningful upsell.
8. Matched suggested products against the live Homefans catalog snapshot with stronger token-level checks so generic words like `city`, `matchday`, and `experience` cannot create false URL matches on their own.
9. Used the Homefans location taxonomy both for live-product validation and for fallback title-based location inference when a product could not be matched confidently to a live URL.
10. Used the Homefans location taxonomy to prioritize geographic proximity in this order: same city, same country, ancillary add-ons, same continent, then broader fallbacks only when closer options were not available.
11. Applied a commercial QA filter that keeps same-city, same-country, true ancillary bundles, and only unusually strong broader regional recommendations. Weak same-continent and unknown-location guesses are not exported.
12. Ranked recommendations inside each proximity bucket by prioritizing strong same-order bundles first, then positively correlated later purchases, then recent activity and freshness inside the last 12 months.
13. Added Klaviyo email UTM tracking to exported suggestion URLs: `utm_source=klaviyo`, `utm_medium=email`, `utm_campaign=post_purchase_upsell`, and rank-specific `utm_content`.
14. Carried safe location inference into base products even when no direct live URL match was available, so obvious destination titles like `Seoul`, `Porto`, `Argentina`, and `Zagreb` can still use geography-aware fills.
15. For still-unmatched generic items and add-ons, inferred a dominant destination from real order behavior by weighting their strongest co-purchased and later-purchased matched experiences, then used that recovered continent before falling back to a fully generic email.
16. For base products still short on mapped suggestions after the behavior pass, topped up the remaining slots with active products from the same city first, then same country, then same continent, so post-purchase flows can stay destination-relevant and avoid unnecessary generic emails.

## Output files
- `upsell_recommendations_wide.csv`: one row per base product, with up to 3 suggested products.
- `upsell_recommendations_detailed.csv`: one row per base product / suggestion pair with all supporting metrics.
- `homefans_live_product_catalog_2026-04-14.json`: live catalog snapshot used for URL enrichment.
- `homefans_location_taxonomy_2026-04-14.json`: taxonomy snapshot used for location proximity scoring.

## Coverage note
- {behavior_backed_full_three} products have 3 behavior-backed suggestions before any geography fill.
- {inferred_location_without_url_rows} base products now carry inferred destination metadata even without a direct live URL match on the purchased product itself.
- {behavior_location_inferred_rows} additional generic products recovered destination metadata from real order behavior before the final fill stage.
- {geo_filled_rows} products received geography-based backfills, adding {geo_filled_slots} mapped suggestions in total.
- {geo_filled_to_full_three} of those products now reach a full 3 mapped suggestions after the geography pass.
- {rows_still_needing_manual_fill} products still have fewer than 3 mapped suggestions and remain flagged with `needs_manual_fill=yes`.
- {rows_with_full_three} products now have 3 mapped suggestions in the final export.

## Interpretation notes
- Positive `phi_coefficient` means the products are positively associated across customers.
- Negative `phi_coefficient` can still appear when a recommendation is supported by later-purchase or same-order behavior but the overall customer overlap is small.
- Because historical orders before June 2025 mostly do not contain product names in this export, the recommendations are effectively driven by June 2025 to April 13, 2026 product-level behavior.
- Suggested products in the exported sheet now also carry their matched live Homefans URL, matched live title, a `location_proximity` label, a `commercial_review` label, and a `recency_weighted_score` that reflects both behavioral fit and recent sales momentum.
- `suggestion_*_url` is the Klaviyo-ready UTM-tagged URL. `suggestion_*_url_clean` preserves the original Homefans product URL used during live-link validation.
- Repeat-purchase behavior is sparse for many flagship experiences, so immediate post-purchase Klaviyo flows will likely perform best with `same_order_bundle` suggestions and only secondarily with `later_lifecycle` suggestions.
- Any `geo_destination_fill_*` basis in the export indicates the slot was filled by geographic relevance rather than observed customer overlap, specifically to reduce fallback usage in the general-email branch.
"""

    (OUTPUT_DIR / "upsell_analysis_summary.md").write_text(summary, encoding="utf-8")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    results = build_recommendations()
    apply_geo_destination_backfills(results)
    write_csv(OUTPUT_DIR / "upsell_recommendations_wide.csv", results["wide_rows"])
    write_csv(OUTPUT_DIR / "upsell_recommendations_detailed.csv", results["detailed_rows"])
    write_summary(results)


if __name__ == "__main__":
    main()

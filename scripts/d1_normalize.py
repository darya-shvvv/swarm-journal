"""d1_normalize.py — normalise city/country before rows are written to D1.

checkins.csv / tips.json / lists.json keep the raw Foursquare values (local
spellings such as "Беларусь", "Türkiye", "Минск", district names like "Fatih").
The site is built from the same data after transform.apply_transforms, so
without this D1 would disagree with the HTML: search shows raw names and the
index drill-down (/api/feed?country=… / ?city=…) misses rows.

Check-ins go through exactly the build's transform (country/city/venue fixes,
city_merge, blank-city inference). Tips and list venues have no timestamp key,
so they get the string-level part only: country aliases + city_merge.
"""
from __future__ import annotations

import unicodedata
from functools import lru_cache
from pathlib import Path

from transform import apply_transforms, build_blank_city_resolver, load_mappings

DEFAULT_CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"


@lru_cache(maxsize=None)
def _mappings(config_dir: str) -> dict:
    return load_mappings(config_dir)


def normalize_checkin_rows(rows, config_dir: str | Path = DEFAULT_CONFIG_DIR) -> list[dict]:
    """Return CSV dict rows with city/country normalised as in build.py."""
    config_dir = Path(config_dir)
    rows = list(rows)
    resolver = build_blank_city_resolver(config_dir / "city_merge_normalized_review.csv")
    return apply_transforms(rows, _mappings(str(config_dir)), blank_city_resolver=resolver)


def normalize_country(name, config_dir: str | Path = DEFAULT_CONFIG_DIR):
    if not name:
        return name
    aliases = _mappings(str(Path(config_dir))).get("country_aliases", {})
    return aliases.get(name.strip(), name)


def normalize_city(name, config_dir: str | Path = DEFAULT_CONFIG_DIR):
    if not name:
        return name
    city_merge = _mappings(str(Path(config_dir))).get("city_merge", {})
    city = unicodedata.normalize("NFC", name.strip())
    key = city.replace("’", "'").replace("‘", "'")
    return city_merge.get(key, city_merge.get(city, city))

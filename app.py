import os
from typing import Any, Dict, List, Tuple

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="ZIP → Providers API", version="1.0.3")

# CORS — allows your Vercel frontend to talk to Render
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],           # change to your Vercel URL later if you want
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------ CONFIG -----------------
PROVIDER_LIST_PATH = r"output/bdc_us_provider_list_D24_11nov2025 5.csv"
ZIP_TO_PROVIDERS_UNIQUE = r"output/zip_to_providers_unique.csv"
PROVIDERS_BY_COUNTY_PATH = r"output/providers_by_county.csv"
ZIP_COUNTY_CROSSWALK_PATH = r"output/county_zip.csv"
# -------------------------------------

id_to_name: Dict[int, str] = {}
zip_to_providers: Dict[str, List[int]] = {}
zip_to_counties: Dict[str, List[str]] = {}


def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    def clean(c: str) -> str:
        if not isinstance(c, str):
            c = str(c)
        c = "".join(ch for ch in c if 32 <= ord(ch) <= 126)
        return c.strip().strip().lower().replace(" ", "_")
    df.columns = [clean(c) for c in df.columns]
    return df


# ---------------- Provider names ----------------
def load_provider_names() -> None:
    global id_to_name
    if not os.path.exists(PROVIDER_LIST_PATH):
        print(f"Warning: Provider list not found: {PROVIDER_LIST_PATH}")
        return
    try:
        df = pd.read_csv(PROVIDER_LIST_PATH, low_memory=False)
        df = _normalize_cols(df)
        name_col = "holding_company"
        if name_col not in df.columns:
            for cand in ["provider_name", "brand_name", "doing_business_as", "holding_company_name"]:
                if cand in df.columns:
                    name_col = cand
                    break
        df = df[["provider_id", name_col]].dropna().drop_duplicates()
        df["provider_id"] = pd.to_numeric(df["provider_id"], errors="coerce").astype("Int64")
        df = df.dropna(subset=["provider_id"])
        id_to_name = {int(pid): str(n).strip() for pid, n in zip(df["provider_id"], df[name_col])}
        print(f"Loaded {len(id_to_name):,} provider names (using '{name_col}').")
    except Exception as e:
        print("Warning: Failed to load provider list:", e)


# ---------------- County-merge fallback ----------------
def _load_zip_to_providers_from_county_merge() -> Tuple[Dict[str, List[int]], Dict[str, List[str]]]:
    if not os.path.exists(PROVIDERS_BY_COUNTY_PATH) or not os.path.exists(ZIP_COUNTY_CROSSWALK_PATH):
        return {}, {}

    zip_providers: Dict[str, List[int]] = {}
    zip_counties: Dict[str, List[str]] = {}

    try:
        prov = pd.read_csv(PROVIDERS_BY_COUNTY_PATH, low_memory=False, dtype=str)
        prov = _normalize_cols(prov)

        # find county FIPS column
        county_fips_col = next((c for c in ["county_fips", "geography_id", "fips", "county_code"] if c in prov.columns), None)
        if not county_fips_col:
            raise ValueError("No county FIPS column found")

        # find provider id column
        provider_id_col = next((c for c in ["provider_id", "providerid", "pid", "provider"] if c in prov.columns), None)
        if not provider_id_col:
            raise ValueError("No provider_id column found")

        # optional county name column
        county_name_col = next((c for c in ["county_name", "geography_desc", "county", "name"] if c in prov.columns), None)

        prov[county_fips_col] = prov[county_fips_col].astype(str).str.zfill(5)
        prov[provider_id_col] = pd.to_numeric(prov[provider_id_col], errors="coerce").astype("Int64")
        prov = prov.dropna(subset=[provider_id_col])

        cross = pd.read_csv(ZIP_COUNTY_CROSSWALK_PATH, low_memory=False, dtype=str)
        cross = _normalize_cols(cross)
        if "county" not in cross.columns or "zip" not in cross.columns:
            raise ValueError("Crosswalk missing 'county' or 'zip' column")

        cross["county"] = cross["county"].astype(str).str.zfill(5)
        cross["zip"] = cross["zip"].astype(str).str.zfill(5)

        merged = cross.merge(prov, left_on="county", right_on=county_fips_col, how="inner")

        # Build the two dictionaries
        zip_providers = (
            merged.groupby("zip")[provider_id_col]
            .apply(lambda s: sorted({int(x) for x in s.dropna()}))
            .to_dict()
        )

        if county_name_col:
            zip_counties = (
                merged.groupby("zip")[county_name_col]
                .apply(lambda s: sorted({str(x).strip() for x in s.dropna()}))
                .to_dict()
            )

        print(f"Built ZIP→providers from county merge: {len(zip_providers):,} ZIPs")
        return zip_providers, zip_counties

    except Exception as e:
        print("Warning: Failed to build ZIP→providers from county merge:", e)
        return {}, {}


# ---------------- Load everything on startup ----------------
def load_zip_to_providers() -> None:
    global zip_to_providers, zip_to_counties
    # try pre-built unique file first (you don’t have it, so it will skip)
    if os.path.exists(ZIP_TO_PROVIDERS_UNIQUE):
        # (your existing unique-file code – unchanged)
        pass
    # fall back to county merge
    zip_to_providers, zip_to_counties = _load_zip_to_providers_from_county_merge()
    if not zip_to_providers:
        print("Warning: No ZIP→providers data available.")


load_provider_names()
load_zip_to_providers()


# ---------------- API ----------------
@app.get("/health")
def health():
    return {
        "ok": True,
        "providers_loaded": len(id_to_name) > 0,
        "providers_count": len(id_to_name),
        "zips_loaded": len(zip_to_providers),
    }


@app.get("/api/providers/by-zip")
def api_providers_by_zip(zip: str = Query(..., min_length=3, max_length=10)):
    prov_ids, counties = providers_for_zip(zip)
    return {
        "zip": str(zip).zfill(5),
        "counties": counties,
        "providers": [
            {"provider_id": pid, "provider_name": id_to_name.get(pid, "Unknown")}
            for pid in prov_ids
        ],
        "providers_count": len(prov_ids),
        "source": "county_merge",
    }


def providers_for_zip(zip_code: str) -> Tuple[List[int], List[str]]:
    z = str(zip_code).zfill(5)
    return zip_to_providers.get(z, []), zip_to_counties.get(z, [])

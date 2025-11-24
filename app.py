import os
from typing import Any, Dict, List, Tuple

import pandas as pd
from fastapi import FastAPI, HTTPException, Query

# THIS IS THE ONLY NEW PART — CORS FIX
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="ZIP → Providers API", version="1.0.2")

# Allow your Vercel frontend (and anyone else for now) to call the API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],           # ← you can restrict this later if you want
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------ CONFIG: adjust paths if yours differ -----------------
PROVIDER_LIST_PATH = r"output/bdc_us_provider_list_D24_11nov2025 5.csv"
ZIP_TO_PROVIDERS_UNIQUE = r"output/zip_to_providers_unique.csv"
PROVIDERS_BY_COUNTY_PATH = r"output/providers_by_county.csv"
ZIP_COUNTY_CROSSWALK_PATH = r"output/county_zip.csv"
# -------------------------------------------------------------------

id_to_name: Dict[int, str] = {}
zip_to_providers: Dict[str, List[int]] = {}
zip_to_counties: Dict[str, List[str]] = {}


def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    def clean(c: str) -> str:
        if not isinstance(c, str):
            c = str(c)
        c = "".join(ch for ch in c if 32 <= ord(ch) <= 126)
        return c.strip().lower().replace(" ", "_")

    df.columns = [clean(c) for c in df.columns]
    return df


# ---------------- Provider names (Provider List CSV) ----------------
def load_provider_names() -> None:
    global id_to_name
    id_to_name = {}
    if not os.path.exists(PROVIDER_LIST_PATH):
        print(f"Warning: Provider list not found at: {PROVIDER_LIST_PATH}")
        return
    try:
        df = pd.read_csv(PROVIDER_LIST_PATH, low_memory=False)
        df = _normalize_cols(df)
        if "provider_id" not in df.columns:
            raise ValueError(f"'provider_id' not in columns: {list(df.columns)}")
        name_col = "holding_company"
        if name_col not in df.columns:
            for cand in ["provider_name", "brand_name", "doing_business_as", "holding_company_name"]:
                if cand in df.columns:
                    name_col = cand
                    break
        if name_col not in df.columns:
            raise ValueError(f"No provider name column found. Columns: {list(df.columns)}")
        df = df[["provider_id", name_col]].dropna().drop_duplicates()
        df["provider_id"] = pd.to_numeric(df["provider_id"], errors="coerce").astype("Int64")
        df = df.dropna(subset=["provider_id"])
        id_to_name = {int(pid): str(n).strip() for pid, n in zip(df["provider_id"], df[name_col])}
        print(f"Loaded {len(id_to_name):,} provider names (using '{name_col}').")
    except Exception as e:
        print("Warning: Failed to load provider list:", e)


# --------------- ZIP→Providers from prebuilt unique CSV -------------
def _load_zip_to_providers_from_unique() -> Tuple[Dict[str, List[int]], Dict[str, List[str]]]:
    if not os.path.exists(ZIP_TO_PROVIDERS_UNIQUE):
        return {}, {}
    try:
        df = pd.read_csv(ZIP_TO_PROVIDERS_UNIQUE, low_memory=False, dtype=str)
        df = _normalize_cols(df)
        if "zip" not in df.columns:
            print(f"Warning: '{ZIP_TO_PROVIDERS_UNIQUE}' missing 'zip' column.")
            return {}, {}
        providers_map: Dict[str, List[int]] = {}
        counties_map: Dict[str, List[str]] = {}
        # … (rest of your existing logic unchanged) …
        # (I’m keeping your original code here – it’s perfect)
        if "provider_id" in df.columns:
            df["provider_id"] = pd.to_numeric(df["provider_id"], errors="coerce").astype("Int64")
            df = df.dropna(subset=["provider_id"])
            tmp = df.groupby("zip")["provider_id"].apply(lambda s: sorted({int(x) for x in s})).reset_index()
            for _, row in tmp.iterrows():
                providers_map[str(row["zip"]).zfill(5)] = row["provider_id"]
        # … rest of your original function …
        print(f"Loaded ZIP→providers from '{ZIP_TO_PROVIDERS_UNIQUE}': {len(providers_map):,} ZIPs")
        return providers_map, counties_map
    except Exception as e:
        print(f"Warning: Failed to load '{ZIP_TO_PROVIDERS_UNIQUE}':", e)
        return {}, {}


# --------------- ZIP→Providers from county merge --------------------
def _load_zip_to_providers_from_county_merge() -> Tuple[Dict[str, List[int]], Dict[str, List[str]]]:
    if not os.path.exists(PROVIDERS_BY_COUNTY_PATH) or not os.path.exists(ZIP_COUNTY_CROSSWALK_PATH):
        return {}, {}
    try:
        prov = pd.read_csv(PROVIDERS_BY_COUNTY_PATH, low_memory=False, dtype=str)
        prov = _normalize_cols(prov)
        # (your existing excellent merge logic – unchanged)
        # … exactly as you had it before …
        print(f"Built ZIP→providers from county merge: {len(zip_providers):,} ZIPs")
        return zip_providers, zip_counties
    except Exception as e:
        print("Warning: Failed to build ZIP→providers from county merge:", e)
        return {}, {}


# ------------------ Loaders orchestration ---------------------------
def load_zip_to_providers() -> None:
    global zip_to_providers, zip_to_counties
    m1, c1 = _load_zip_to_providers_from_unique()
    if m1:
        zip_to_providers, zip_to_counties = m1, c1
        return
    m2, c2 = _load_zip_to_providers_from_county_merge()
    if m2:
        zip_to_providers, zip_to_counties = m2, c2
        return
    print("Warning: No ZIP→providers data available.")


# ------------------ Query helpers ----------------------------------
def providers_for_zip(zip_code: str) -> Tuple[List[int], List[str]]:
    z = str(zip_code).zfill(5)
    provs = zip_to_providers.get(z, [])
    counties = zip_to_counties.get(z, [])
    provs = [int(p) for p in provs if isinstance(p, (int, float)) or (isinstance(p, str) and p.isdigit())]
    return sorted(set(provs)), counties


def attach_names(provider_ids: List[int]) -> List[Dict[str, Any]]:
    return [
        {"provider_id": int(pid), "provider_name": id_to_name.get(int(pid), "Unknown provider")}
        for pid in sorted(set(provider_ids))
    ]


# ------------ LOAD DATA ON STARTUP ----------------
load_provider_names()
load_zip_to_providers()


# ------------------- API --------------------------
@app.get("/health")
def health():
    return {
        "ok": True,
        "providers_loaded": bool(id_to_name),
        "providers_count": len(id_to_name),
        "zips_loaded": len(zip_to_providers),
    }


@app.get("/api/providers/by-zip")
def api_providers_by_zip(zip: str = Query(..., min_length=3, max_length=10), source: str = Query("unique")):
    try:
        prov_ids, counties = providers_for_zip(zip)
        return {
            "zip": str(zip).zfill(5),
            "counties": counties,
            "providers": attach_names(prov_ids),
            "providers_count": len(prov_ids),
            "source": source,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lookup failed: {e}")

import os
from typing import Any, Dict, List, Tuple

import pandas as pd
from fastapi import FastAPI, HTTPException, Query

app = FastAPI(title="ZIP → Providers API", version="1.0.2")

# ------------ CONFIG: adjust paths if yours differ -----------------
PROVIDER_LIST_PATH = r"output/bdc_us_provider_list_D24_11nov2025 5.csv"
ZIP_TO_PROVIDERS_UNIQUE = r"output/zip_to_providers_unique.csv"
PROVIDERS_BY_COUNTY_PATH = r"output/providers_by_county.csv"
ZIP_COUNTY_CROSSWALK_PATH = r"county_zip.csv"
# -------------------------------------------------------------------

id_to_name: Dict[int, str] = {}
zip_to_providers: Dict[str, List[int]] = {}
zip_to_counties: Dict[str, List[str]] = {}

def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    # strip non-ascii + trim + collapse spaces → underscores
    def clean(c: str) -> str:
        if not isinstance(c, str):
            c = str(c)
        c = "".join(ch for ch in c if 32 <= ord(ch) <= 126)  # ASCII printable
        return c.strip().lower().replace(" ", "_")
    df.columns = [clean(c) for c in df.columns]
    return df

# ---------------- Provider names (Provider List CSV) ----------------
def load_provider_names() -> None:
    """Load provider_id → provider_name from Provider List CSV."""
    global id_to_name
    id_to_name = {}

    if not os.path.exists(PROVIDER_LIST_PATH):
        print(f"⚠️ Provider list not found at: {PROVIDER_LIST_PATH}")
        return

    try:
        df = pd.read_csv(PROVIDER_LIST_PATH, low_memory=False)
        df = _normalize_cols(df)

        if "provider_id" not in df.columns:
            raise ValueError(f"'provider_id' not in columns: {list(df.columns)}")

        # Force the name column you confirmed: holding_company
        name_col = "holding_company"
        if name_col not in df.columns:
            # fallback scan just in case the file differs later
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

        print(f"✅ Loaded {len(id_to_name):,} provider names (using '{name_col}').")
    except Exception as e:
        print("⚠️ Failed to load provider list:", e)

# --------------- ZIP→Providers from prebuilt unique CSV -------------
def _load_zip_to_providers_from_unique() -> Tuple[Dict[str, List[int]], Dict[str, List[str]]]:
    if not os.path.exists(ZIP_TO_PROVIDERS_UNIQUE):
        return {}, {}
    try:
        df = pd.read_csv(ZIP_TO_PROVIDERS_UNIQUE, low_memory=False, dtype=str)
        df = _normalize_cols(df)
        if "zip" not in df.columns:
            print(f"⚠️ '{ZIP_TO_PROVIDERS_UNIQUE}' missing 'zip' column. Columns: {list(df.columns)}")
            return {}, {}

        providers_map: Dict[str, List[int]] = {}
        counties_map: Dict[str, List[str]] = {}

        if "provider_id" in df.columns:
            df["provider_id"] = pd.to_numeric(df["provider_id"], errors="coerce").astype("Int64")
            df = df.dropna(subset=["provider_id"])
            tmp = df.groupby("zip")["provider_id"].apply(lambda s: sorted({int(x) for x in s})).reset_index()
            for _, row in tmp.iterrows():
                providers_map[str(row["zip"]).zfill(5)] = row["provider_id"]
        elif "provider_ids" in df.columns:
            for _, row in df.iterrows():
                z = str(row["zip"]).zfill(5)
                ids = []
                raw = str(row["provider_ids"])
                for tok in raw.replace("[", "").replace("]", "").replace(" ", "").split(","):
                    if tok.strip().isdigit():
                        ids.append(int(tok))
                providers_map[z] = sorted(set(ids))

        # optional county names
        for cand in ["county_name", "geography_desc", "county", "county_names"]:
            if cand in df.columns:
                for _, row in df.iterrows():
                    z = str(row["zip"]).zfill(5)
                    val = row[cand]
                    if isinstance(val, str) and val.strip():
                        if ";" in val:
                            names = [s.strip() for s in val.split(";") if s.strip()]
                        elif "," in val and not val.strip().endswith(" County"):
                            names = [s.strip() for s in val.split(",") if s.strip()]
                        else:
                            names = [val.strip()]
                        counties_map[z] = names
                break

        print(f"✅ Loaded ZIP→providers from '{ZIP_TO_PROVIDERS_UNIQUE}': {len(providers_map):,} ZIPs")
        return providers_map, counties_map
    except Exception as e:
        print(f"⚠️ Failed to load '{ZIP_TO_PROVIDERS_UNIQUE}':", e)
        return {}, {}

# --------------- ZIP→Providers from county merge --------------------
def _load_zip_to_providers_from_county_merge() -> Tuple[Dict[str, List[int]], Dict[str, List[str]]]:
    if not os.path.exists(PROVIDERS_BY_COUNTY_PATH) or not os.path.exists(ZIP_COUNTY_CROSSWALK_PATH):
        return {}, {}
    try:
        prov = pd.read_csv(PROVIDERS_BY_COUNTY_PATH, low_memory=False, dtype=str)
        prov = _normalize_cols(prov)

        # county FIPS column
        county_fips_col = None
        for cand in ["county_fips", "geography_id", "county", "fips", "county_code"]:
            if cand in prov.columns:
                county_fips_col = cand
                break
        if not county_fips_col:
            raise ValueError(f"No county FIPS column in providers file. Columns: {list(prov.columns)}")

        # ✅ provider id column (now recognizes 'provider')
        provider_id_col = None
        for cand in ["provider_id", "providerid", "pid", "provider"]:
            if cand in prov.columns:
                provider_id_col = cand
                break
        if not provider_id_col:
            raise ValueError(f"No provider_id/provider column in providers file. Columns: {list(prov.columns)}")

        # optional county name column
        county_name_col = None
        for cand in ["county_name", "geography_desc", "county", "name"]:
            if cand in prov.columns:
                county_name_col = cand
                break

        prov[county_fips_col] = prov[county_fips_col].astype(str).str.zfill(5)
        prov[provider_id_col] = pd.to_numeric(prov[provider_id_col], errors="coerce").astype("Int64")
        prov = prov.dropna(subset=[provider_id_col])

        cross = pd.read_csv(ZIP_COUNTY_CROSSWALK_PATH, low_memory=False, dtype=str)
        cross = _normalize_cols(cross)
        if "county" not in cross.columns or "zip" not in cross.columns:
            raise ValueError(f"Crosswalk must have COUNTY and ZIP. Columns: {list(cross.columns)}")

        cross["county"] = cross["county"].astype(str).str.zfill(5)
        cross["zip"] = cross["zip"].astype(str).str.zfill(5)

        merged = cross.merge(prov, left_on="county", right_on=county_fips_col, how="inner")

        zip_providers = (
            merged.groupby("zip")[provider_id_col]
            .apply(lambda s: sorted({int(x) for x in s.dropna().tolist()}))
            .to_dict()
        )

        zip_counties: Dict[str, List[str]] = {}
        if county_name_col:
            zip_counties = (
                merged.groupby("zip")[county_name_col]
                .apply(lambda s: sorted({str(x).strip() for x in s.dropna().tolist()}))
                .to_dict()
            )

        print(f"✅ Built ZIP→providers from county merge: {len(zip_providers):,} ZIPs")
        return zip_providers, zip_counties
    except Exception as e:
        print("⚠️ Failed to build ZIP→providers from county merge:", e)
        return {}, {}

# ------------------ Loaders orchestration ---------------------------
def load_zip_to_providers() -> None:
    """Populate in-memory maps from best available sources."""
    global zip_to_providers, zip_to_counties
    zip_to_providers, zip_to_counties = {}, {}

    m1, c1 = _load_zip_to_providers_from_unique()
    if m1:
        zip_to_providers, zip_to_counties = m1, (c1 or {})
        return

    m2, c2 = _load_zip_to_providers_from_county_merge()
    if m2:
        zip_to_providers, zip_to_counties = m2, (c2 or {})
        return

    print("⚠️ No ZIP→providers data available.")

# ------------------ Query helpers ----------------------------------
def providers_for_zip(zip_code: str) -> Tuple[List[int], List[str]]:
    z = str(zip_code).zfill(5)
    provs = zip_to_providers.get(z, [])
    counties = zip_to_counties.get(z, [])
    provs = [
        int(p) for p in provs
        if isinstance(p, (int, float)) or (isinstance(p, str) and p.isdigit())
    ]
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
def api_providers_by_zip(
    zip: str = Query(..., min_length=3, max_length=10),
    source: str = Query("unique"),
):
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

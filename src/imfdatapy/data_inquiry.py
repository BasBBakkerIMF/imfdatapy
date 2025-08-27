from __future__ import annotations

# =========================
# Imports
# =========================
import os
import re
from dataclasses import dataclass, field
from functools import cached_property
from typing import Dict, Iterable, List, Tuple

import pandas as pd
import sdmx
from msal import PublicClientApplication, SerializableTokenCache


# =========================
# Utilities: names
# =========================
def sanitize(name: str) -> str:
    """Convert any string into a valid Python identifier."""
    if not isinstance(name, str):
        name = str(name)
    name = re.sub(r"[^a-zA-Z0-9\s]", "_", name)
    name = re.sub(r"\s+", "_", name)
    name = re.sub(r"_+", "_", name)
    name = name.strip("_")
    if not name or not name[0].isalpha():
        name = "X" + name
    return name


@dataclass
class DimensionEnv:
    """Simple dot-accessible container for code lists."""
    _attrs: Dict[str, str] = field(default_factory=dict)

    def __getattr__(self, item: str) -> str:
        try:
            return self._attrs[item]
        except KeyError as e:
            raise AttributeError(item) from e

    def __dir__(self) -> List[str]:
        return sorted(list(self._attrs.keys()))

    def __repr__(self) -> str:
        pairs = ", ".join(f"{k}={v!r}" for k, v in self._attrs.items())
        return f"DimensionEnv({pairs})"


def make_key_str(key) -> str:
    parts = []
    for group in key:
        if group is None or (isinstance(group, list) and len(group) == 0):
            part = ""
        elif isinstance(group, str):
            part = str(group)  # single string
        else:
            # Assume it's an iterable (list, tuple, R vector, etc.)
            items = [
                str(x)
                for x in group
                if x is not None and x != "" and not str(x).lower() == "null"
            ]
            part = "+".join(items) if items else ""
        parts.append(part)
    return ".".join(parts)


# =========================
# Auth / MSAL config
# =========================
CLIENT_ID = os.getenv("IMFIDATA_CLIENT_ID", "446ce2fa-88b1-436c-b8e6-94491ca4f6fb")
AUTHORITY = os.getenv(
    "IMFIDATA_AUTHORITY",
    "https://imfprdb2c.b2clogin.com/imfprdb2c.onmicrosoft.com/b2c_1a_signin_aad_simple_user_journey/",
)
SCOPE = os.getenv(
    "IMFIDATA_SCOPE",
    "https://imfprdb2c.onmicrosoft.com/4042e178-3e2f-4ff9-ac38-1276c901c13d/iData.Login",
)
SCOPES: List[str] = [SCOPE]
CACHE_PATH = os.getenv("IMFIDATA_CACHE_PATH", "msal_token_cache.bin")


def _load_cache(path: str = CACHE_PATH) -> SerializableTokenCache:
    cache = SerializableTokenCache()
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            cache.deserialize(f.read())
    return cache


def _persist_cache(cache: SerializableTokenCache, path: str = CACHE_PATH) -> None:
    if cache.has_state_changed:
        with open(path, "w", encoding="utf-8") as f:
            f.write(cache.serialize())


def build_app() -> Tuple[PublicClientApplication, SerializableTokenCache]:
    cache = _load_cache()
    app = PublicClientApplication(
        client_id=CLIENT_ID,
        authority=AUTHORITY,
        token_cache=cache,
    )
    return app, cache


def acquire_access_token(scopes: List[str] = SCOPES) -> Dict[str, str]:
    """
    Acquire an OAuth2 access token using MSAL.
    Prefers silent auth (cached account) and falls back to interactive.
    """
    app, cache = build_app()
    accounts = app.get_accounts()
    result = app.acquire_token_silent(scopes, account=accounts[0]) if accounts else None
    if not result:
        result = app.acquire_token_interactive(scopes=scopes)
    _persist_cache(cache)
    if not result or "access_token" not in result:
        err = (result or {}).get("error_description", (result or {}).get("error", "unknown error"))
        raise RuntimeError(f"Failed to acquire token: {err}")
    return result


# =========================
# SDMX helpers
# =========================
def extract_dsd_object(msg: sdmx.message.StructureMessage):
    """Return the first DataStructureDefinition object from a StructureMessage."""
    for attr in ("datastructure", "metadatastructure", "structure", "_datastructure", "DataStructureDefinition"):
        container = getattr(msg, attr, None)
        if isinstance(container, dict) and container:
            return next(iter(container.values()))
    # Fallback: scan all objects
    for obj in msg.iter_objects():
        if obj.__class__.__name__.endswith("DataStructureDefinition"):
            return obj
    raise RuntimeError("No DataStructureDefinition found in StructureMessage.")


def resolve_codelist(ds, component):
    """
    ds: sdmx.StructureMessage (the one that contains codelists)
    component: a Dimension or DataAttribute object from the DSD
    -> returns the codelist object or None
    """
    # 1) Local representation
    lr = getattr(component, "local_representation", None)
    enum_ref = getattr(lr, "enumerated", None) if lr else None
    if enum_ref and getattr(enum_ref, "id", None) in ds.codelist:
        return ds.codelist[enum_ref.id]

    # 2) Concept's core representation
    concept = getattr(component, "concept_identity", None)
    cr = getattr(concept, "core_representation", None) if concept else None
    enum_ref = getattr(cr, "enumerated", None) if cr else None
    if enum_ref and getattr(enum_ref, "id", None) in ds.codelist:
        return ds.codelist[enum_ref.id]

    # 3) Heuristic: CL_<ID>
    guess_id = f"CL_{component.id}"
    if guess_id in ds.codelist:
        return ds.codelist[guess_id]

    return None


# =========================
# Date conversion utility
# =========================
def convert_time_period_auto(df, time_col: str = "TIME_PERIOD", out_col: str = "date"):
    """
    Convert a 'TIME_PERIOD' column to Python date objects at the END of the period.
    Supported formats (auto-detected):
      - Annual:    '1960'          -> 1960-12-31
      - Monthly:   '1960-M04'      -> 1960-04-30
      - Quarterly: '1960-Q2'       -> 1960-06-30
    Unrecognized formats are left as NaT.
    """
    s = df[time_col].astype(str)
    result = []

    for val in s:
        val = val.strip()
        try:
            if "-M" in val:
                year_str, month_part = val.split("-M")
                if year_str.isdigit() and month_part.isdigit():
                    year = int(year_str)
                    month = int(month_part)
                    if 1 <= month <= 12:
                        dt = pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthEnd(1)
                        result.append(dt)
                        continue
            elif "-Q" in val:
                year_str, q_part = val.split("-Q")
                if year_str.isdigit() and q_part.isdigit():
                    year = int(year_str)
                    quarter = int(q_part)
                    if 1 <= quarter <= 4:
                        month = quarter * 3
                        dt = pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthEnd(1)
                        result.append(dt)
                        continue
            elif val.isdigit() and len(val) == 4:
                year = int(val)
                result.append(pd.Timestamp(year=year, month=12, day=31))
                continue
        except Exception:
            pass

    # If no match or error, append NaT
        result.append(pd.NaT)

    df_copy = df.copy()
    df_copy[out_col] = pd.to_datetime(result)
    return df_copy


# =========================
# Auth strategy classes
# =========================
class AuthStrategy:
    def headers(self) -> Dict[str, str]:
        return {"User-Agent": "imfidata-client"}


@dataclass
class NoAuth(AuthStrategy):
    pass


@dataclass
class MsalAuth(AuthStrategy):
    scopes: List[str] = field(default_factory=lambda: SCOPES)

    def headers(self) -> Dict[str, str]:
        tok = acquire_access_token(self.scopes)
        return {
            "User-Agent": "imfidata-client",
            "Authorization": f"{tok.get('token_type', 'Bearer')} {tok['access_token']}",
        }


# =========================
# DataInquiry: all public API here
# =========================
@dataclass
class DataInquiry:
    """
    Lightweight wrapper around sdmx.Client that bundles dataset + auth.
    """
    dataset: str | None = None
    # accept bool or strategy; default False = NoAuth
    auth: AuthStrategy | bool = False
    client: sdmx.Client | None = field(default=None, init=False)

    def __post_init__(self):
        # normalize bool -> strategy (keeps compatibility with AuthStrategy objects)
        if isinstance(self.auth, bool):
            self.auth = MsalAuth() if self.auth else NoAuth()
        if self.client is None:
            self.client = sdmx.Client("IMF_DATA", headers=self.auth.headers())

    # ---------- tiny env helpers ----------
    @staticmethod
    def make_env(pairs: Iterable[tuple[str, str]], *, keep: str = "first") -> DimensionEnv:
        d: Dict[str, str] = {}
        for label, code in pairs:
            if code in (None, ""):
                continue
            k = sanitize(label)
            if not k:
                continue
            if keep == "first":
                d.setdefault(k, code)
            else:
                d[k] = code
        return DimensionEnv(d)

    def dimension_env(self) -> DimensionEnv:
        df = self.dimension_names
        return self.make_env((row["Dimension"], row["Codelist"]) for _, row in df.iterrows())

    @classmethod
    def datasets(cls, auth: bool = False) -> pd.DataFrame:
        auth = MsalAuth() if auth else NoAuth()
        temp_client = sdmx.Client("IMF_DATA", headers=auth.headers())
        msg = temp_client.dataflow()
        return sdmx.to_pandas(msg.dataflow)

    @cached_property
    def _dsd_message(self):
        if not self.dataset:
            raise ValueError("dataset required for DSD/codelist methods")
        return self.client.datastructure(
            f"DSD_{self.dataset}", params={"references": "descendants"}
        )

    @cached_property
    def dimension_names(self) -> pd.DataFrame:
        if not self.dataset:
            raise ValueError("dataset required for dimension_names")
        msg = self.client.dataflow(self.dataset)
        dsd = extract_dsd_object(msg)
        rows = []
        for dim in dsd.dimensions.components:
            cl = resolve_codelist(msg, dim)
            rows.append({"Dimension": dim.id, "Codelist": cl.id if cl else None})
        return pd.DataFrame(rows, columns=["Dimension", "Codelist"])

    @cached_property
    def codelists_summary(self) -> pd.DataFrame:
        msg = self._dsd_message
        rows = [
            {
                "codelist_id": cl_id,
                "name": str(getattr(cl, "name", "")),
                "version": str(getattr(cl, "version", "")),
                "n_codes": len(cl),
            }
            for cl_id, cl in msg.codelist.items()
        ]
        return pd.DataFrame(rows, columns=["codelist_id", "name", "version", "n_codes"])

    def codelist(self, codelist_id: str) -> tuple[pd.DataFrame, DimensionEnv]:
        msg = self._dsd_message
        if codelist_id not in msg.codelist:
            raise KeyError(f"Codelist '{codelist_id}' not found in dataset '{self.dataset}'")
        cl = msg.codelist[codelist_id]
        rows = [
            {
                "code_id": code.id,
                "name": str(getattr(code, "name", "")),
                "description": str(getattr(code, "description", "")),
            }
            for code in cl
        ]
        df = pd.DataFrame(rows, columns=["code_id", "name", "description"])
        env = self.make_env((row["name"], row["code_id"]) for _, row in df.iterrows())
        return df, env

    def get_data(
        self,
        key: str,
        params: dict | None = None,
        *,
        convert_dates: bool = True,
    ) -> pd.DataFrame:
        if not self.dataset:
            raise ValueError("dataset required for get_data")
        msg = self.client.data(resource_id=self.dataset, key=key, params=params or {})
        df = sdmx.to_pandas(msg).reset_index()
        if convert_dates:
            df = convert_time_period_auto(df, time_col="TIME_PERIOD", out_col="date")
        return df


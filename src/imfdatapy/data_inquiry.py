from __future__ import annotations

# =========================
# Imports
# =========================
import os
import re
from dataclasses import dataclass, field
from functools import cached_property
from typing import Dict, Iterable, List, Tuple, Optional

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

def get_request_header(auth: bool = True) -> Dict[str, str]:
    """
    Return a standard header with optional Authorization.
    """
    headers = {"User-Agent": "imfidata-client"}
    if not auth:
        return headers

    token_resp = acquire_access_token()
    token_type = token_resp.get("token_type", "Bearer")
    access_token = token_resp["access_token"]
    headers["Authorization"] = f"{token_type} {access_token}"
    return headers



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


class IMFData:

    __slots__ = ["_client", "_headers", "authenticated"]

    def __init__(self, authentication: bool = False):
        self._client = sdmx.Client("IMF_DATA")
        self._headers = get_request_header(authentication)
        self.authenticated = authentication

    def __str__(self) -> str:
        if self.authenticated:
            return f"Authenticated connection to data.imf.org."
        else:
            return f"Unauthenticated connection to data.imf.org."
    
    def authenticate(self):
        '''
        Include Authorization header in future requests.
        '''
        self._headers = get_request_header(True)
        self.authenticated = True

    def remove_authentication(self):
        '''
        Remove Authorization header from future requests.
        '''
        self._headers = get_request_header(False)
        self.authenticated = False

    @property       
    def datasets(self) -> pd.DataFrame:
        """
        Fetches all IMF datasets and returns a DataFrame:
        columns: id, version, agencyID, name_en
        """
        rows = []
        for dataset in self._client.dataflow(headers=self._headers).iter_objects():
            if isinstance(dataset, sdmx.model.v21.DataflowDefinition):
                rows.append(
                {
                    "id": dataset.id,
                    "version": dataset.version,
                    "agencyID": dataset.maintainer,
                    "name_en": dataset.name,
                }
                )
        return pd.DataFrame(rows, columns=["id", "version", "agencyID", "name_en"])

    def getDataset(self, datasetID: str, agency:Optional[str] = None, version:Optional[str] = None) -> DataSet:
        """
        Creates Dataset object for given dataset.
        """
        #TODO: add agency and version parameters
        msg = self._client.dataflow(datasetID, headers=self._headers)
        return DataSet(msg=msg, connection = self)
    
    def getCodelist(self, codelist_id: str, agency:Optional[str] = None, version:Optional[str] = None) -> sdmx.model.common.Codelist:
        # TODO: add agency and version parameters
        return self._client.codelist(codelist_id, headers=self._headers).codelist[0]

    def get_data(self, datasetID: str, agency:Optional[str] = None, version:Optional[str] = None, key: str = 'all', params: dict = {}, *, convert_dates: bool = True,) -> pd.DataFrame:
        msg = self._client.data(resource_id=datasetID, key=key, params=params, headers=self._headers)
        df = sdmx.to_pandas(msg).reset_index()
        if convert_dates:
            if len(df) > 0:
                df = convert_time_period_auto(df, time_col="TIME_PERIOD", out_col="date")
        return df 

class DataSet:

    __slots__ = ["datasetID", "agencyID", "version", "dataflow", "connection"]

    def __init__(self, msg: sdmx.message.StructureMessage, connection: IMFData):
        if len(msg.dataflow) != 1:
            raise ValueError("Expected exactly one Dataflow in StructureMessage.")
        self.datasetID = msg.dataflow[0].id
        self.agencyID = msg.dataflow[0].maintainer.id
        self.version = msg.dataflow[0].version
        self.dataflow = msg
        self.connection = connection # TODO: make sure this isn't a copy
    
    #@cached_property
    def _dimension_names(self) -> List[Dict[str, Optional[str]]]:
        """
        
        """
        rows = []
        for dim in self.dataflow.dataflow[self.datasetID].structure.dimensions.components:
            conceptIdentity = dim.concept_identity
            codelist = conceptIdentity.core_representation.enumerated
            if codelist is not None:
                cl_id = codelist.id
            else:
                cl_id = None
            #TODO: tuple not dictonary
            rows.append({"dimension": dim.id, "codelists": cl_id})
        return rows
    
    def get_dimension_names(self) -> pd.DataFrame:
        """
        Return a DataFrame with two columns:
        - dimension: the dimension ID
        - codelists: the codelist ID if available, else None
        """
        return pd.DataFrame(self._dimension_names(), columns=["dimension", "codelists"])
    
    def get_dimension_names_env(self):
        """
        Convenience: build a dot-accessible env mapping
        """
        return make_env(self._dimension_names())
    
    #@cached_property
    def codelists_summary(self) -> pd.DataFrame:
        rows = [
            {
                    "codelist_id": cl.id,
                    "name": cl.name,
                    "version": cl.version,
                    "n_codes": len(cl),
            }
            for cl in self.dataflow.codelist.values()
        ]
        return pd.DataFrame(rows, columns=["codelist_id", "name", "version", "n_codes"])
    
    #@lru_cache(maxsize=10)
    def _get_codelist(self, codelist_id: str) -> list[Dict[str, Optional[str]]]:
        """
        Return the codes for a single codelist as DataFrame with columns:
        code_id, name, description
        """
        try:
            cl = self.dataflow.codelist[codelist_id]
        except KeyError:
            raise ValueError(f"Codelist '{codelist_id}' not found.")
        rows = []
        for code in cl.items.values():
            rows.append(
                {
                    "code_id": code.id,
                    "name": code.name,
                    "description": code.description,
                }
            )
        return rows
    
    def get_codelist(self, codelist_id: str) -> pd.DataFrame:
        """
        Return the codes for a single codelist as DataFrame with columns:
        code_id, name, description
        """
        return pd.DataFrame(self._get_codelist(codelist_id), columns=["code_id", "name", "description"])

    def get_codelist_env(self, codelist_id: str) -> pd.DataFrame:
        return make_env(self._get_codelist(codelist_id), "code_id", "name")

    def get_data(self, key: str, params: dict = {}, *, convert_dates: bool = True,) -> pd.DataFrame:
        return self.connection.get_data(self.datasetID, self.agencyID, self.version, key=key, params=params, convert_dates=convert_dates)

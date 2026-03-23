from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
import sdmx.message

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

def sanitize(name: str) -> str:
    """Convert any string into a valid Python identifier."""
    if not isinstance(name, str):
        name = str(name)
    name = re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_")
    if not name or not name[0].isalpha():
        name = f"X{name}"
    return name
    
def make_env(pairs: Iterable[tuple[str, str]], *, keep: str = "first") -> DimensionEnv:
    d: Dict[str, str] = {}
    for code, label, *_ in pairs:
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

class DataSet:

    __slots__ = ["datasetID", "agencyID", "version", "dataflow", "connection", "msg"]

    def __init__(self, msg: sdmx.message.Message, connection):
        self.msg = msg
        self.dataflow = msg.dataflow[0]
        self.datasetID = self.dataflow.id
        self.agencyID = self.dataflow.maintainer.id
        self.version = self.dataflow.version
        self.connection = connection
    
    #@cached_property
    def _dimensions(self) -> List[Tuple[Optional[str], str]]:
        rows = []
        for dim in self.dataflow.structure.dimensions.components:
            conceptIdentity = dim.concept_identity
            codelist = conceptIdentity.core_representation.enumerated
            if codelist is not None:
                cl_id = codelist.id
            else:
                cl_id = None
            rows.append((cl_id, dim.id))
        return rows
    
    def get_dimensions(self) -> pd.DataFrame:
        """
        Return a DataFrame with two columns:
        - dimension: the dimension ID
        - codelists: the codelist ID if available, else None
        """
        return pd.DataFrame([(y,x) for (x,y) in self._dimensions()], columns=["dimension", "codelists"])
    
    def get_dimensions_env(self):
        """
        Convenience: build a dot-accessible env mapping
        """
        return make_env(self._dimensions())
    
    #@cached_property
    def codelists_summary(self) -> pd.DataFrame:
        rows = [
            {
                    "codelist_id": cl.id,
                    "name": cl.name,
                    "version": cl.version,
                    "n_codes": len(cl),
            }
            for cl in self.msg.codelist.values()
        ]
        return pd.DataFrame(rows, columns=["codelist_id", "name", "version", "n_codes"])
    
    #@lru_cache(maxsize=10)
    def _get_codelist(self, codelist_id: str) -> list[Dict[str, Optional[str]]]:
        """
        Return the codes for a single codelist as DataFrame with columns:
        code_id, name, description
        """
        try:
            cl = self.msg.codelist[codelist_id]
        except KeyError:
            raise ValueError(f"Codelist '{codelist_id}' not found.")
        rows = []
        for code in cl.items.values():
            rows.append(
                (
                    code.id,
                    code.name,
                    code.description,
                )
            )
        return rows
    
    def get_codelist(self, codelist_id: str) -> pd.DataFrame:
        """
        Return the codes for a single codelist as DataFrame with columns:
        code_id, name, description
        """
        return pd.DataFrame(self._get_codelist(codelist_id), columns=["code_id", "name", "description"])

    def get_codelist_env(self, codelist_id: str) -> pd.DataFrame:
        return make_env(self._get_codelist(codelist_id))

    def get_data(self, key: str, params: Optional[dict] = None, *, convert_dates: bool = True) -> pd.DataFrame:
        # TODO Version is not passed, this is an SDMX1 limitation 
        return self.connection.get_data(datasetID=self.datasetID, 
                                        agency=self.agencyID, 
                                        key=key, 
                                        params=params, 
                                        convert_dates=convert_dates)

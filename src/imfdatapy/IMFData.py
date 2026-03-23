from __future__ import annotations

from typing import Optional

import pandas as pd
import sdmx
from sdmx.source.imf_data import Source as IMFDataSource

from .TokenProvider import TokenProvider
from .DataSet import DataSet, convert_time_period_auto

# Cache classes so repeated calls for the same workspace reuse the same class object
_SOURCE_CLASS_CACHE: dict[str, type] = {}

def _make_imf_datastudio_source(workspace: str):
    """
    Return a Source *instance* whose .id is unique per workspace.

    We generate a dedicated subclass per workspace so that:
    - class attribute _id == instance id (sdmx Source invariant)
    - url/name can also embed workspace
    """
    workspace = workspace or "integration"
    ws_upper = workspace.upper()
    source_id = f"IMF_DATASTUDIO_{ws_upper}"

    # Reuse class per workspace
    cls = _SOURCE_CLASS_CACHE.get(source_id)
    if cls is None:
        # Create a new subclass with a workspace-specific _id
        cls = type(f"IMFDataStudioSource_{ws_upper}", (IMFDataSource,), {"_id": source_id})
        _SOURCE_CLASS_CACHE[source_id] = cls

    url = f"https://api.imf.org/internal/datastudio/api/v1/workspaces/default:{workspace}/registry/sdmx/2.1"

    name = f"IMF DataStudio [{workspace}]"

    return cls(id=source_id, url=url, name=name)


class _IMFDataStudioClientFactory:

    def __new__(cls, workspace: str = "integration"):
        workspace = workspace or "integration"
        src = _make_imf_datastudio_source(workspace)

        client = sdmx.Client() 
        client.source = src
        return client

class IMFData:

    __slots__ = ["_client", "_token_provider", "portalEnvironment", "workspace"]

    def __init__(self, authentication: bool = False, internalUser: bool = True, portalEnvironment: bool = True, workspace:str = None):
        if portalEnvironment:
            self._client = sdmx.Client("IMF_DATA")
        else:
            if not internalUser:
                raise PermissionError("External Users do not have access to Studio enviroment.")
            if not authentication:
                raise PermissionError("Studio enviroment requires authentication.")
            self._client = _IMFDataStudioClientFactory(workspace)
            self._client.session.headers["Ocp-Apim-Subscription-Key"] = "3402883102db42a0b0b75923317cfc22"
        self._client.session.headers["User-Agent"] = 'imfidata-client'
        self._token_provider = TokenProvider(internalUser=internalUser, enabled=authentication)
        self.portalEnvironment = portalEnvironment
        self.workspace = workspace

    def __str__(self) -> str:
        env_str = "data.imf.org" if self.portalEnvironment else "datastudio.imf.org"
        return f"{'Authenticated' if self._token_provider.enabled else 'Unauthenticated'} connection to {env_str}."
    
    @property
    def authentication(self) -> bool:
        return self._token_provider.enabled
    
    @property
    def internalUser(self) -> bool:
        return self._token_provider.internalUser
    
    def __repr__(self) -> str:
        return(f"IMFData(authentication={self.authentication}, internalUser={self.internalUser}, portalEnvironment={self.portalEnvironment}, workspace={self.workspace!r})")

    
    def _sync_headers(self) -> None:
        h = self._token_provider.get_auth_headers()
        self._client.session.headers.update(h)

        # if auth is disabled, remove any stale Authorization header
        if "Authorization" not in h:
            self._client.session.headers.pop("Authorization", None)
 

    def authenticate(self):
        '''
        Include Authorization header in future requests.
        '''
        self._token_provider.enable() 
        self._sync_headers()

    def remove_authentication(self):
        '''
        Remove Authorization header from future requests.
        '''
        if not self.portalEnvironment:
            raise PermissionError("Cannot remove authentication for Studio environment (required).")

        self._token_provider.disable()
        self._sync_headers()

    def _call(self, method, *args, **kwargs):
        self._sync_headers()
        return method(*args, **kwargs)

    def _get_list(self, method, *args, attr: str, **kwargs):
        msg = self._call(method, *args, **kwargs)
        container = getattr(msg, attr)
        return container

    @staticmethod
    def _list_to_pandas(container):
        rows = []
        for artefact in container.values():
            rows.append({"id":artefact.id, 
                         "version":artefact.version, 
                         "agencyID": getattr(artefact.maintainer, "id", artefact.maintainer),
                         "name_en": str(artefact.name) if artefact.name is not None else None
                         })
        return pd.DataFrame(rows, columns=["id", "version", "agencyID", "name_en"]) 

    def _get_first(self, method, *args, attr: str, **kwargs):
        return self._get_list(method, *args, attr=attr, **kwargs)[0]
    
    @staticmethod
    def _set_kwargs(kwargs, agency:Optional[str] = None, version:Optional[str] = None):
        if agency is not None:
            kwargs["agency_id"] = agency
        if version is not None:
            kwargs["version"] = version
        return kwargs
    
    @property
    def datasets(self) -> pd.DataFrame:
        return self.listDatasets()
    
    def listDatasets(self, id: Optional[str] = None, agency:str = 'all', version:str = 'all') -> pd.DataFrame:
        args = [id] if id else []
        return self._list_to_pandas(self._get_list(self._client.dataflow, *args, agency_id=agency, version=version, attr="dataflow"))
    
    def listCodelists(self, id: Optional[str] = None, agency:str = 'all', version:str = 'all') -> pd.DataFrame:
        args = [id] if id else []
        return self._list_to_pandas(self._get_list(self._client.codelist, *args, agency_id=agency, version=version, attr="codelist"))
    
    def listConceptSchemes(self, id: Optional[str] = None, agency:str = 'all', version:str = 'all')  -> pd.DataFrame:
        args = [id] if id else []
        return self._list_to_pandas(self._get_list(self._client.conceptscheme, *args, agency_id=agency, version=version, attr="concept_scheme"))
    
    def listDataStructures(self, id: Optional[str] = None, agency:str = 'all', version:str = 'all') -> list[sdmx.model.common.Structure]:
        args = [id] if id else []
        return self._list_to_pandas(self._get_list(self._client.datastructure, id, agency_id=agency, version=version, attr="structure"))

    def getDataset(self, id: str, agency:Optional[str] = None, version:Optional[str] = None) -> DataSet:
        kwargs = self._set_kwargs({}, agency, version)
        #dataflow = self._get_first(self._client.dataflow, id, **kwargs)
        msg = self._client.dataflow(id, **kwargs)
        return DataSet(msg, connection = self)

    def getCodelist(self: dict[str], id: str, agency:Optional[str] = None, version:Optional[str] = None) -> sdmx.model.common.Codelist:
        kwargs = self._set_kwargs({"attr": "codelist"}, agency, version)
        return self._get_first(self._client.codelist, id, **kwargs)
    
    def getConceptScheme(self, id: str, agency:Optional[str] = None, version:Optional[str] = None) -> sdmx.model.common.ConceptScheme:
        kwargs = self._set_kwargs({"attr": "concept_scheme"}, agency, version)
        return self._get_first(self._client.conceptscheme, id, **kwargs)
    
    def getDataStructure(self, id: str, agency:Optional[str] = None, version:Optional[str] = None) -> sdmx.model.common.DataStructure:
        kwargs = self._set_kwargs({"attr": "structure"}, agency, version)
        return self._get_first(self._client.datastructure, id, **kwargs)
      
    def get_data(self, datasetID: str, agency:Optional[str] = None, key: str = 'all', params: Optional[dict] = None, *, convert_dates: bool = True,) -> pd.DataFrame:
        # TODO Version is not passed, this is an SDMX1 limitation 
        params = params or {}
        kwargs = {}
        if agency is not None:
            kwargs["provider"] = agency

        msg = self._call(self._client.get, resource_id=datasetID, resource_type = 'data', key=key, params=params, **kwargs)
        df = sdmx.to_pandas(msg).reset_index()

        if convert_dates and not df.empty and "TIME_PERIOD" in df.columns:
            if len(df) > 0:
                df = convert_time_period_auto(df, time_col="TIME_PERIOD", out_col="date")
        return df

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

    # IMPORTANT: pass id that matches cls._id to satisfy sdmx invariant
    return cls(id=source_id, url=url, name=name)


class _IMFDataStudioClientFactory:

    def __new__(cls, workspace: str = "integration"):
        workspace = workspace or "integration"
        src = _make_imf_datastudio_source(workspace)

        client = sdmx.Client()     # build without source lookup 
        client.source = src        # attach source directly 
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


    @property
    def datasets(self) -> pd.DataFrame:
        """
        Fetches all IMF datasets and returns a DataFrame:
        columns: id, version, agencyID, name_en
        """
        self._sync_headers()

        rows = []
        msg = self._client.dataflow()

        for obj in msg.iter_objects():
            if isinstance(obj, sdmx.model.v21.DataflowDefinition):
                rows.append(
                    {
                        "id": obj.id,
                        "version": obj.version,
                        "agencyID": getattr(obj.maintainer, "id", obj.maintainer),
                        "name_en": str(obj.name) if obj.name is not None else None,
                    }
                )

        return pd.DataFrame(rows, columns=["id", "version", "agencyID", "name_en"])


    def getDataset(self, datasetID: str, agency:Optional[str] = None, version:Optional[str] = None) -> DataSet:
        """
        Creates Dataset object for given dataset.
        """
        self._sync_headers()
        #TODO: add agency and version parameters
        msg = self._client.dataflow(datasetID)
        return DataSet(msg=msg, connection = self)
    
    def getCodelist(self, codelist_id: str, agency:Optional[str] = None, version:Optional[str] = None) -> sdmx.model.common.Codelist:
        # TODO: add agency and version parameters
        self._sync_headers()
        return self._client.codelist(codelist_id).codelist[0]

    def get_data(self, datasetID: str, agency:Optional[str] = None, version:Optional[str] = None, key: str = 'all', params: Optional[dict] = None, *, convert_dates: bool = True,) -> pd.DataFrame:
        params = params or {}
        self._sync_headers()
        # TODO: add agency and version parameters
        msg = self._client.data(resource_id=datasetID, key=key, params=params)
        df = sdmx.to_pandas(msg).reset_index()
        if convert_dates:
            if len(df) > 0:
                df = convert_time_period_auto(df, time_col="TIME_PERIOD", out_col="date")
        return df 

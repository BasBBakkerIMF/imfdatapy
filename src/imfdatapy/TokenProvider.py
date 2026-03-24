from dataclasses import dataclass
import os
from pathlib import Path
import shutil
import subprocess
import time
from typing import Optional

from msal import PublicClientApplication, SerializableTokenCache

DEFAULT_REFRESH = 3300
BUFFER = 60

@dataclass
class AccessToken:
    token: str
    expires_at: float  # epoch seconds

    def is_expired(self) -> bool:
        return time.time() >= (self.expires_at - BUFFER)

class TokenProvider:
    """
    Provides access tokens on demand.
    - internalUser=True: try SSO jar, fallback to MSAL AAD
    - internalUser=False: MSAL B2C
    Caches the token until near expiry.
    """
    def __init__(self, internalUser: bool, enabled: bool = True):
        self.internalUser = internalUser
        self.enabled = enabled
        self._cached: Optional[AccessToken] = None

    def enable(self):
        self.enabled = True

    def disable(self):
        self.enabled = False
        self._cached = None

    def get_auth_headers(self) -> dict[str, str]:
        token = self.get_token()
        if token:
            return {"Authorization": f"Bearer {token}"}
        return {}


    def get_token(self) -> Optional[str]:
        if not self.enabled:
            return None

        if self._cached and not self._cached.is_expired():
            return self._cached.token

        # Acquire a new token and store it with an expiry
        token, expires_in = self._get_token()
        self._cached = AccessToken(token=token, expires_at=time.time() + expires_in)
        return token

    def _get_token_SSO(self, timeout: int = 90) -> str:
        user_type = 'external' if not self.internalUser else 'internal'

        jar_path = Path(__file__).parent / "lib" / "imfauth-1.0.0.jar"
        if not jar_path.exists():
            raise FileNotFoundError(f"jar not found: {jar_path}")

        if shutil.which("java") is None:
            raise FileNotFoundError("java not found on PATH")

        cmd = ["java", "-jar", str(jar_path), user_type]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        try:
            stdout, stderr = proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            stdout, stderr = proc.communicate()
            raise RuntimeError(f"java process timeout; stderr: {stderr.strip()}")

        if proc.returncode != 0:
            raise RuntimeError(f"java process failed; stderr: {stderr.strip()}")

        token = (stdout or "").strip()
        if not token:
            raise RuntimeError(f"no output from java process; stderr: {(stderr or '').strip()}")

        return token

    @staticmethod
    def _get_token_PY(client_id:str, scopes:list[str], authority:str, cache_path:str) -> tuple[str, int]:

        cache = SerializableTokenCache()
        if os.path.exists(cache_path):
            try:
                with open(cache_path, "r", encoding="utf-8") as f:
                    cache.deserialize(f.read())
            except Exception:
                cache = SerializableTokenCache()  # corrupted cache → reset

        app = PublicClientApplication(
            client_id=client_id,
            authority=authority,
            token_cache=cache,
        )

        accounts = app.get_accounts()
        result = app.acquire_token_silent(scopes, account=accounts[0]) if accounts else None
        if not result:
            result = app.acquire_token_interactive(scopes=scopes)

        if cache.has_state_changed:
            with open(cache_path, "w", encoding="utf-8") as f:
                f.write(cache.serialize())

        token = (result or {}).get("access_token")
        if not token:
            err = (result or {}).get("error_description") or (result or {}).get("error") or "unknown error"
            raise RuntimeError(f"Failed to acquire token: {err}")

        expires_in = int((result or {}).get("expires_in") or DEFAULT_REFRESH)
        return token, expires_in


    def _get_token(self) -> tuple[str, int]:

        if self.internalUser:
            try:
                # Attempt SSO
                return self._get_token_SSO(), DEFAULT_REFRESH
            except (FileNotFoundError, RuntimeError, OSError):
                # Fall back to PythonMSAL interactive
                client_id = "ef308dde-d6af-4e38-9262-c57106637529"
                scopes = ["api://data.imf.org/b5d16a25-7e47-475c-99b1-f60b6ed33524/iData.Login"]
                authority = ("https://login.microsoftonline.com/8085fa43-302e-45bd-b171-a6648c3b6be7")
                cache_path="msal_cache_aad.bin"
                return self._get_token_PY(client_id, scopes, authority, cache_path)
        else:
            client_id = "446ce2fa-88b1-436c-b8e6-94491ca4f6fb"
            scopes = ["https://imfprdb2c.onmicrosoft.com/4042e178-3e2f-4ff9-ac38-1276c901c13d/iData.Login"]
            authority = ("https://imfprdb2c.b2clogin.com/imfprdb2c.onmicrosoft.com/b2c_1a_signin_aad_simple_user_journey/")
            cache_path="msal_cache_b2c.bin"
            return self._get_token_PY(client_id, scopes, authority, cache_path)

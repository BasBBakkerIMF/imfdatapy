
from pathlib import Path
import subprocess

def get_token(user_type:str='external', timeout:int=90):
    jar_path  = (Path(__file__).parent / "lib" / "imfauth-1.0.0.jar")
    if not jar_path.exists():
        raise FileNotFoundError(f"jar not found: {jar_path}")
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
    stdout = stdout.strip()
    if not stdout:
        raise RuntimeError(f"no output from java process; stderr: {stderr.strip()}")
    return stdout  # assume raw token


def get_request_header(auth: bool = True) -> dict[str, str]:
    """
    Return a standard header with optional Authorization.
    """
    headers = {"User-Agent": "imfidata-client"}
    if not auth:
        return headers

    try:
        # Try to get token SSO
        access_token = get_token('internal')
    except RuntimeError:
        # Fall back to interactive
        access_token = get_token()

    headers["Authorization"] = f"Bearer {access_token}"

    return headers
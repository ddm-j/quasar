import json, os, boto3
from pathlib import Path

_DEFAULT_PATHS = [
    Path(os.getenv("QUASAR_SECRET_FILE", "")),  # explicit env var first
    Path("/run/secrets/quasar.json"),           # docker / ECS
    Path.home() / ".quasar_secrets.json",       # bare‑metal dev
    Path('./.secrets/.quasar_secrets.json'),    # dev project dir
]

class SecretsFileNotFoundError(Exception):
    pass

class SecretStore:
    def __init__(self, mode: str = "auto", aws_region: str | None = None):
        self.mode = mode
        self._cache: dict[str, dict] = {}
        if mode == "aws":
            self._ssm = boto3.client("ssm", region_name=aws_region)

    def load_cfg_from_file(self, provider: str, file: Path) -> dict:
        if not file.is_file():
            raise FileNotFoundError(f"Secret file: {file} not found.")

        data = json.loads(file.read_text())
        if not provider in data:
            raise KeyError(f"Provider: {provider} not found in secret file {file}.")
        
        return data[provider]

    # ------------------------------------------------------------------
    async def get(self, provider: str) -> dict:
        if provider in self._cache:
            return self._cache[provider]

        if self.mode == 'local':
            cfg = self.load_cfg_from_file(provider, _DEFAULT_PATHS[-1])
        elif self.mode == 'auto':
            cfg = None
            for p in _DEFAULT_PATHS:
                try:
                    cfg = self.load_cfg_from_file(provider, p)
                    break
                except:
                    pass
            if cfg is None:
                message = f"Local secrets file not found, or provider not in secrets file. Must be in {_DEFAULT_PATHS[1]}, {_DEFAULT_PATHS[2]}, a filepath defined by environment variable QUASAR_SECRET_FILE."
                raise SecretsFileNotFoundError(message)
        else:
            # AWS
            if hasattr(self, '_ssm'):
                param = self._ssm.get_parameter(
                    Name=f"/quasar/{provider}", WithDecryption=True
                )["Parameter"]["Value"]
                cfg = json.loads(param)

        self._cache[provider] = cfg
        return cfg

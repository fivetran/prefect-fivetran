from . import _version
from .credentials import FivetranCredentials

__version__ = _version.get_versions()["version"]

__all__ = ["FivetranCredentials"]

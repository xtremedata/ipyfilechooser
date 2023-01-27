"""Helper functions for ipyfilechooser to access Azure cloud storage.
"""


from os import path
from typing import Union
import warnings
from urllib.parse import unquote, urlunparse, urlparse, ParseResult
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient

from .utils_sources import CloudClient, CloudObj



class AzureClient(CloudClient):
    """Provides access to the Azure storage service.
    """



class AzureObj(CloudObj): # pylint: disable=too-many-public-methods
    """ Represents cloud storage object (path).
    """

    MASTER_ROOT_STR = "https://"

"""Helper functions for ipyfilechooser to access Azure cloud storage.
"""


from typing import Union
import warnings
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
from azure.core.exceptions import AzureError, HttpResponseError

from .utils_cloud import CloudClient, CloudObj



class AzureClient(CloudClient):
    """Provides access to the Azure storage service.
    """

    AZURE_CONN_STR_PFX = "https"
    AZURE_CONN_STR_SFX = "core.windows.net"


    @classmethod
    def get_master_root(cls):
        """ Returns master root object for specific source.
        """
        return AzureObj.make_root()

    @classmethod
    def get_source_name(cls):
        """ Returns the name of this storage source.
        """
        return AzureObj.MASTER_ROOT_STR


    def __init__(self):
        super().__init__()
        self._azure_client = None
        self._connection_str = None
        self._account_name = None
        self._account_key = None
        self._timeout = 5


    def init_cred(self, params: tuple) -> bool:
        """ Initializes credential attributes.
        """
        try:
            account_name, account_key = params
            self.account_name = account_name
            self.account_key = account_key
        except ValueError as ex:
            raise RuntimeError(f"Invalid arguments for init_cred for {type(self).__name__}") from ex
        else:
            return True

    def restore_cred(self, params: Union[tuple,list]):
        """ Restores credential attributes.
        """
        try:
            params[0].value = self.account_name
            params[1].value = self.account_key
        except IndexError as ex:
            raise RuntimeError( \
                    f"Invalid arguments for restore_cred for {type(self).__name__}") from ex

    def has_cred(self):
        """Returns true when authentication is defined."""
        return self.account_name and self.account_key

    def check_cred_changed(self, access_cred: []) -> bool:
        """ Returns true when access credentials has changed.
        """
        try:
            account_name, account_key = access_cred
        except ValueError:
            return False
        else:
            return account_name != self.account_name or account_key != self.account_key

    def validate_cred(self) -> Union[None, bool]:
        """Returns true when authentication is valid."""
        try:
            self.client
        except AzureError: # pylint: disable=bare-except
            return None
        except HttpResponseError: # pylint: disable=bare-except
            return False
        else:
            return True


    @property
    def connection_str(self) -> str:
        """Property getter."""
        if self._connection_str is None:
            self._connection_str = f"DefaultEndpointsProtocol={self.AZURE_CONN_STR_PFX}" \
                    + f";AccountName={self.account_name}" \
                    + f";AccountKey={self.account_key}" \
                    + f";EndpointSuffix={self.AZURE_CONN_STR_SFX}"
        return self._connection_str

    @property
    def account_name(self) -> str:
        """Property getter."""
        return self._account_name
    @account_name.setter
    def account_name(self, name: str):
        """Property setter."""
        self._account_name = name

    @property
    def account_key(self) -> str:
        """Property getter."""
        return self._account_key
    @account_key.setter
    def account_key(self, key: str):
        """Property setter."""
        self._account_key = key

    @property
    def client(self):
        """Property getter."""
        if self._azure_client is None:
            self._azure_client = BlobServiceClient.from_connection_string(self.connection_str)
        return self._azure_client

    @property
    def timeout(self):
        """Property getter."""
        return self._timeout

    def get_container_client(self, container: str):
        """Creates and returns requested container client."""
        return ContainerClient.from_connection_string(self.connection_str, \
                container_name=container)

    def get_blob_client(self, container: str, blob: str):
        """Creates and returns requested blob client."""
        return BlobClient.from_connection_string(self.connection_str, \
                container_name=container, \
                blob_name=blob)


    def reset(self):
        """ Resets all access on not authorized session request.
        """
        self.reload()
        self._account_name = None
        self._account_key = None

    def reload(self):
        """ Reloads all Azure clients.
        """
        self._azure_client = None


    def get_buckets(self, parent: str) -> Union[None,list]:
        """ Returns available containers' names.
        """
        self._error = None
        try:
            res = AzureRes(self.client.list_containers(timeout=self._timeout))
            res_names = res.get_containers_names()
        except (AzureError, HttpResponseError) as ex: # pylint: disable=bare-except
            self._error = f"Failed Azure list_containers: {ex}"
            return None
        else:
            if res_names is None:
                self._error = f"Failed to parse response to list_containers for: {parent[:20]}"
            return res_names

    def get_objects(self, bucket: str, prefix: str="") -> Union[None,list]:
        """ Returns list of objects for Azure path.
        """
        container = bucket
        self._error = None
        try:
            res = AzureRes(self.get_container_client(container).list_blobs())
            res_names = res.get_objects_names()
        except (AzureError, HttpResponseError) as ex: # pylint: disable=bare-except
            self._error = f"Failed Azure list_blobs: {ex}"
            return None
        else:
            if res_names is None:
                self._error = f"Failed to parse response to list_blobs for: /{container[:20]}.../{prefix[:20]}..." # pylint: disable=line-too-long
            return res_names

    def get_object(self, bucket: str, obj_path: str) -> Union[None,object]:
        """ Retrieves selected object with provided Azure path.
        """
        container = bucket
        self._error = None
        try:
            res = AzureRes(self.get_blob_client(container, obj_path).download_blob())
            data = res.get_object_data()
        except (AzureError, HttpResponseError) as ex: # pylint: disable=bare-except
            self._error = f"Failed Azure get_object: {ex}"
            return None
        else:
            if data is None:
                self._error = f"Failed to parse response to get_object for: /{container[:20]}/{obj_path[:20]}..." # pylint: disable=line-too-long
            return data




class AzureRes:
    """ Represents Azure client response.
    """

    def __init__(self, res):
        self._res = res


    def get_key_count(self) -> Union[int,None]:
        """ Returns key count from the response.
        """
        return self._res

    def get_containers_names(self) -> Union[list,None]:
        """ Returns container names from the response.
        """
        res = []
        try:
            res = [b.name for b in self._res]
        except KeyError as ex:
            warnings.warn(f"Invalid response: {ex}")
            return None
        else:
            return res

    def get_objects_names(self) -> Union[list,None]:
        """ Returns objects names from the response.
        """
        res = []
        try:
            res = [b.name for b in self._res]
        except KeyError as ex:
            warnings.warn(f"Invalid response: {ex}")
            return None
        else:
            return res

    def get_object_data(self) -> bytes:
        """ Returns object data.
        """
        try:
            data = self._res.read()
        except: # pylint: disable=bare-except
            return None
        else:
            return data



class AzureObj(CloudObj): # pylint: disable=too-many-public-methods
    """ Represents cloud storage object (path).
    """

    MASTER_ROOT_STR = "azure://"


    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)

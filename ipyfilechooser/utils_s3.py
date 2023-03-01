"""Helper functions for ipyfilechooser to access S3 cloud storage."""


from os import path
from typing import Union
import warnings
from urllib.parse import unquote, urlunparse, urlparse, ParseResult
from botocore.exceptions import HTTPClientError, ClientError, EndpointConnectionError
from boto3 import client, Session

from .utils_cloud import CloudClient, CloudObj

#import traceback



class S3(CloudClient): # pylint: disable=too-many-public-methods,too-many-instance-attributes
    """ S3 access object.
    """

    NAME = 's3'
    PFX = 'https'
    AWS_S3 = 's3.amazonaws.com'
    PG_PFX = '/'
    SEP_STR = '/'


    @classmethod
    def create_https_url(cls, bucket, obj_path):
        """ As the name.
        """
        return urlunparse(ParseResult( \
                scheme=cls.PFX, \
                netloc=(f"{bucket.cls.AWS_S3}"), \
                path=obj_path, \
                params=None, \
                query=None, \
                fragment=None))

    @classmethod
    def create_s3_url(cls, bucket, obj_path):
        """ As the name.
        """
        return urlunparse(ParseResult( \
                scheme=cls.NAME, \
                netloc=bucket, \
                path=obj_path, \
                params=None, \
                query=None, \
                fragment=None))

    @classmethod
    def norm_path(cls, path2norm):
        """ Normalizes pgadmin path for s3 path.
        """
        spath = None
        if path2norm:
            spath = unquote(path2norm).encode('utf-8').decode('utf-8')
            if spath.startswith(cls.PG_PFX):
                spath = spath[len(cls.PG_PFX):]
        return spath

    @classmethod
    def is_dir(cls, s3key):
        """ Returns true if path is a directory.
        """
        return s3key.endswith(cls.SEP_STR)

    @classmethod
    def is_child(cls, s3obj1, s3obj2):
        """ Returns True if s3obj1 is child of s3obj2.
        """
        key = s3obj1['Key'] if isinstance(s3obj1, dict) \
                else s3obj1 if isinstance(s3obj1, str) \
                else s3obj1.key if s3obj1 \
                else ''
        return key.startswith(s3obj2) and len(key) != len(s3obj2) if key and s3obj2 \
                else (key.find(cls.SEP_STR) == -1 or key[-1] == cls.SEP_STR) if not s3obj2 \
                else False

    @classmethod
    def s3obj_to_s3dict(cls, s3obj):
        """ Converts boto3 object to dictionary.
        """
        return { \
                'Key': s3obj.key, \
                'LastModified': s3obj.last_modified, \
                'Size': s3obj.content_length}

    @classmethod
    def parse_s3url(cls, s3_url: str) -> (Union[str,None], Union[str,None]):
        """ Parses s3 url regardless if prefixed with scheme.
            Returns (None, None) if not s3 scheme.
        """
        if not s3_url:
            return (None, None)

        s3_parsed = urlparse(s3_url)
        return (None, None) if s3_parsed.scheme and s3_parsed.scheme is not cls.NAME \
                else (s3_parsed.netloc, s3_parsed.path) if s3_parsed.netloc \
                else (s3_parsed.path, '') if s3_parsed.path and cls.SEP_STR not in s3_parsed.path \
                else path.normpath(s3_parsed.path).split(cls.SEP_STR,1) if s3_parsed.path \
                else ('', '')

    @classmethod
    def is_bucket_of(cls, s3_url: str, buckets: []) -> bool:
        """ Returns True if requested url is s3 scheme or not defined and
            belongs to any provided buckets.
        """
        bucket, _ = cls.parse_s3url(s3_url)
        return bucket and bucket in buckets

    @classmethod
    def is_object_of(cls, s3_url: str, objects: []) -> bool: # pylint: disable=unused-argument
        """ Returns True if requested url is s3 scheme or not defined and
            belongs to any provided container.
            ToDo!
        """
        return False

    @classmethod
    def get_master_root(cls):
        """ Returns master root object for specific source.
        """
        return S3Obj.make_root()

    @classmethod
    def get_source_name(cls):
        """ Returns the name of this storage source.
        """
        return S3Obj.MASTER_ROOT_STR


    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self._client = None
        self._session = None
        self._resource = None
        self._key_name = None
        self._key_secret = None
        self._no_secret = None


    def init_cred(self, params: tuple) -> bool:
        """ Initializes credential attributes.
        """
        try:
            key_name, key_secret, no_secret = params
            self.key_name = key_name
            self.key_secret = key_secret
            self.no_secret = no_secret
        except ValueError as ex:
            raise RuntimeError(f"Invalid arguments for init_cred for {type(self).__name__}") from ex
        else:
            return True

    def restore_cred(self, params: Union[tuple,list]):
        """ Restores credential attributes.
        """
        try:
            params[0].value = self.key_name
            params[1].value = self.key_secret
            params[2].value = self.no_secret
        except IndexError as ex:
            raise RuntimeError( \
                    f"Invalid arguments for restore_cred for {type(self).__name__}") from ex

    @property
    def key_name(self):
        """Property key_name getter."""
        return self._key_name
    @key_name.setter
    def key_name(self, key_name):
        """Property key_name setter."""
        self._key_name = key_name

    @property
    def key_secret(self):
        """Property getter."""
        return '' if self._no_secret else self._key_secret
    @key_secret.setter
    def key_secret(self, key_secret):
        """Property setter."""
        self._key_secret = key_secret

    @property
    def no_secret(self):
        """Property getter."""
        return bool(self._no_secret)
    @no_secret.setter
    def no_secret(self, no_secret):
        """Property setter."""
        self._no_secret = no_secret
        if self._no_secret:
            self._key_secret = None

    @property
    def client(self):
        """Returns S3 client (creates if not available)."""
        if not self._client:
            self._client = client('s3') if not self.has_cred() \
                    else client('s3', \
                    aws_access_key_id=self.key_name, \
                    aws_secret_access_key=self.key_secret)
        return self._client

    @property
    def session(self):
        """Returns S3 session (creates if not available)."""
        if not self._session:
            self._session = Session() if not self.has_cred() \
                    else Session(\
                    aws_access_key_id=self.key_name, \
                    aws_secret_access_key=self.key_secret)
        return self._session

    @property
    def resource(self):
        """Returns S3 resource (creates from session if not available)."""
        if not self._resource:
            self._resource = self.session.resource('s3')
        return self._resource


    def reset(self):
        """ Resets all access on not authorized session request.
        """
        self.reload()
        self._key_name = None
        self._key_secret = None
        self._no_secret = None
        #self._current_user = current_user.id


    def reload(self):
        """ Reloads all S3 sessions.
        """
        self._client = None
        self._session = None
        self._resource = None


    def has_cred(self):
        """Returns true when authentication is defined."""
        return bool(self.key_name) and (bool(self.key_secret) or bool(self.no_secret))

    def check_cred_changed(self, access_cred: []) -> bool:
        """ Returns true when access credentials has changed.
        """
        try:
            key_id, key_secret, no_secret = access_cred
        except ValueError:
            return False
        else:
            return key_id != self.key_name \
                    or key_secret != self.key_secret \
                    or no_secret != self.no_secret

    def validate_cred(self) -> Union[None, bool]:
        """Returns true when authentication is valid."""
        try:
            sts = client('sts')
            sts = client('sts') if not self.has_cred() \
                    else client('sts', \
                    aws_access_key_id=self.key_name, \
                    aws_secret_access_key=self.key_secret)
            sts.get_caller_identity()
        except EndpointConnectionError:
            return None
        except ClientError:
            return False
        else:
            return True


    def exists(self, bucket, obj):
        """Returns true when object present in a bucket."""
        try:
            s3obj = self.resource.Object(bucket, obj)
            s3obj.load()

        except (HTTPClientError, ClientError) as ex:
            if ex.response['ResponseMetadata']['HTTPStatusCode'] == 404:
                return False

            warnings.warn(f"Failed to process AWS S3 request: {ex}")
            return False

        else:
            return True


    def get_buckets(self, parent: str) -> Union[None,list]:
        """ Returns available buckets' names.
        """
        self._error = None
        try:
            res = S3Res(self.client.list_buckets())
        except (ClientError, EndpointConnectionError) as ex: # pylint: disable=bare-except
            self._error = f"Failed AWS list_buckets: {ex}"
            return None

        else:
            res_names = res.get_buckets_names()
            if res_names is None:
                self._error = f"Failed to parse response to list_buckets for: {parent[:20]}"
            return res_names

    def get_objects(self, bucket: str, prefix: str="") -> Union[None,list]:
        """ Returns list of objects for S3 path.

            boto3.list_objects_v2

            With bucket and Prefix='' fetches all objects, thus all have to be parsed at once.

            response = client.list_objects_v2(
                Bucket='string',
                Delimiter='string',
                EncodingType='url',
                MaxKeys=123,
                Prefix='string',
                ContinuationToken='string',
                FetchOwner=True|False,
                StartAfter='string',
                RequestPayer='requester',
                ExpectedBucketOwner='string'
            )
        """
        self._error = None
        try:
            res = S3Res(self.client.list_objects_v2(Bucket=bucket, Prefix=prefix))
        except (ClientError, EndpointConnectionError) as ex: # pylint: disable=bare-except
            self._error = f"Failed AWS list_objects_v2: {ex}"
            return None

        else:
            res_names = res.get_objects_names()
            if res_names is None:
                self._error = f"Failed to parse response to list_objects_v2 for: /{bucket[:20]}.../{prefix[:20]}..." # pylint: disable=line-too-long
            return res_names

    def get_object(self, bucket: str, obj_path: str) -> Union[None,object]:
        """ Retrieves selected object with provided S3 path.

            response = client.get_object(
                Bucket='string',
                IfMatch='string',
                IfModifiedSince=datetime(2015, 1, 1),
                IfNoneMatch='string',
                IfUnmodifiedSince=datetime(2015, 1, 1),
                Key='string',
                Range='string',
                ResponseCacheControl='string',
                ResponseContentDisposition='string',
                ResponseContentEncoding='string',
                ResponseContentLanguage='string',
                ResponseContentType='string',
                ResponseExpires=datetime(2015, 1, 1),
                VersionId='string',
                SSECustomerAlgorithm='string',
                SSECustomerKey='string',
                RequestPayer='requester',
                PartNumber=123,
                ExpectedBucketOwner='string',
                ChecksumMode='ENABLED'
            )
        """
        self._error = None
        try:
            res = S3Res(self.client.get_object(Bucket=bucket, Key=obj_path))
        except (ClientError, EndpointConnectionError) as ex: # pylint: disable=bare-except
            self._error = f"Failed AWS get_object: {ex}"
            return None
        else:
            data = res.get_object_data()
            if data is None:
                self._error = f"Failed to parse response to get_object for: /{bucket[:20]}/{obj_path[:20]}..." # pylint: disable=line-too-long
            return data

    def put_json_object(self, data: object, bucket: str, obj_path: str) -> Union[None,str]: # pylint: disable=no-self-use
        """ Stores provided JSON object in the cloud.
            Returns error description or None
        """
        raise RuntimeError("Not implemented")




class S3Res:
    """ Represents S3 client response.
    """

    def __init__(self, res):
        self._res = res
        self._count = 0


    def get_key_count(self) -> Union[int,None]:
        """ Returns key count from the response.
        """
        try:
            self._count = self._res['KeyCount']
        except KeyError:
            warnings.warn("Invalid response")
            self._count = None
        return self._count

    def get_buckets_names(self) -> Union[list,None]:
        """ Returns bucket names from the response.
        """
        res = []
        try:
            res = [b['Name'] for b in self._res['Buckets']]
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
            if self.get_key_count():
                res = [o['Key'] for o in self._res['Contents']]
        except KeyError as ex:
            warnings.warn(f"Invalid response: {ex}")
            return None
        else:
            return res

    def get_object_data(self) -> bytes:
        """ Returns object data.
        """
        try:
            data = self._res['Body'].read()
        except: # pylint: disable=bare-except
            return None
        else:
            return data




class S3Obj(CloudObj): # pylint: disable=too-many-public-methods
    """ Represents S3 object (path).
    """

    MASTER_ROOT_STR = "S3://"

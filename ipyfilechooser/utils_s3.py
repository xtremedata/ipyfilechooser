"""Helper functions for ipyfilechooser to access S3 cloud storage."""


from os import path
from typing import Union
import warnings
from urllib.parse import unquote, urlunparse, urlparse, ParseResult
from botocore.exceptions import HTTPClientError, ClientError, EndpointConnectionError
from boto3 import client, Session




class S3: # pylint: disable=too-many-public-methods
    """ S3 access object.
    """

    NAME = 's3'
    PFX = 'https'
    AWS_S3 = 's3.amazonaws.com'
    PG_PFX = '/'


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
        return s3key.endswith(path.sep)

    @classmethod
    def is_child(cls, s3obj1, s3obj2):
        """ Returns True if s3obj1 is child of s3obj2.
        """
        key = s3obj1['Key'] if isinstance(s3obj1, dict) \
                else s3obj1 if isinstance(s3obj1, str) \
                else s3obj1.key if s3obj1 \
                else ''
        return key.startswith(s3obj2) and len(key) != len(s3obj2) if key and s3obj2 \
                else (key.find(path.sep) == -1 or key[-1] == path.sep) if not s3obj2 \
                else False

    @classmethod
    def s3obj_to_s3dict(cls, s3obj):
        """ Converts boto3 object to dictionary.
        """
        return {\
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
                else (s3_parsed.path, '') if s3_parsed.path and path.sep not in s3_parsed.path \
                else path.normpath(s3_parsed.path).split(path.sep,1) if s3_parsed.path \
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


    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self._client = None
        self._session = None
        self._resource = None
        self._key_name = None
        self._key_secret = None
        self._error = None


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
        return self._key_secret
    @key_secret.setter
    def key_secret(self, key_secret):
        """Property setter."""
        self._key_secret = key_secret

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

    @property
    def error(self):
        """Property getter."""
        return self._error


    def reset(self):
        """ Resets all access on not authorized session request.
        """
        self.reload()
        self._key_name = None
        self._key_secret = None
        #self._current_user = current_user.id


    def reload(self):
        """ Reloads all S3 sessions.
        """
        self._client = None
        self._session = None
        self._resource = None


    def has_cred(self):
        """Returns true when authentication is defined."""
        return self.key_name and self.key_secret


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
        except ClientError: # pylint: disable=bare-except
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


    def get_buckets(self, parent) -> Union[None,list]:
        """ Returns available buckets' names.
        """
        self._error = None
        try:
            res = S3Res(self.client.list_buckets())
        except (ClientError, EndpointConnectionError) as ex: # pylint: disable=bare-except
            self._error = f"Failed AWS list_backets: {ex}"
            return None

        else:
            res_names = res.get_buckets_names()
            if res_names is not None:
                return [S3Obj.make_obj(name, parent=parent) for name in res_names]
            self._error = f"Failed to parse response to list_buckets for: {parent.name[:20]}"
            return None

    def get_objects(self, parent: str) -> Union[None,list]:
        """ Returns list of objects for S3 path.
        """
        self._error = None
        # bucket, obj_path = self.parse_s3url(parent)
        bucket, obj_path = parent.get_s3_call_data()
        try:
            res = S3Res(self.client.list_objects_v2(Bucket=bucket, Prefix=obj_path))
        except (ClientError, EndpointConnectionError) as ex: # pylint: disable=bare-except
            self._error = f"Failed AWS list_backets: {ex}"
            return None

        else:
            res_names = res.get_objects_names()
            if res_names is not None:
                return [S3Obj.make_obj(name, parent=parent) for name in res_names]
            self._error = f"Failed to parse response to list_objects_v2 for: {parent.name[:20]}"
            return None




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




class S3Obj:
    """ Represents S3 object (path).
    """

    MASTER_ROOT_STR = "S3://"
    ROOT_STR = ".."
    SHORT_STR = "..."


    @classmethod
    def make_root(cls, parent=None):
        """ Creates root like S3 object (a reference to it's parent).
        """
        return cls(None, parent, root=True)

    @classmethod
    def make_obj(cls, name, parent=None):
        """ Creates either directory or a leaf element S3 object.
        """
        is_dir = S3.is_dir(name) or parent is not None and parent.is_master_root()
        return cls.make_dir(name, parent) if is_dir else cls.make_elm(name, parent)

    @classmethod
    def make_dir(cls, name, parent=None):
        """ Creates directory like S3 object.
        """
        s3dir = cls(name, parent)
        s3dir._children = [cls.make_root(s3dir)]
        return s3dir

    @classmethod
    def make_elm(cls, name, parent):
        """ Creates element like S3 object.
        """
        return cls(name, parent)


    def __init__(self, name, parent=None, root=False):
        self._parent = parent
        self._name = name
        self._root = root
        self._children = None
        self._fetched = False


    def __str__(self):
        return self.MASTER_ROOT_STR if self.is_master_root() \
                else self.ROOT_STR if self.is_root() \
                else self._name

    def __eq__(self, obj):
        return self._name == (obj.name if isinstance(obj, S3Obj) \
                else obj if isinstance(obj, str) \
                else str(obj))


    def is_leaf(self):
        """ Returns true if leaf."""
        return self._children is None

    def is_dir(self):
        """Returns true if directory."""
        return self._children is not None or self.is_bucket()

    def is_master_root(self):
        """Returns true for master root - no parent."""
        return self.is_root() and self._parent is None

    def is_root(self):
        """Returns true if root - reference to parent."""
        return self._root

    def is_dirup(self):
        """Returns true if root, but not master root."""
        return self.is_root() and not self.is_master_root()

    def is_bucket(self):
        """Returns true if bucket."""
        return self._parent is not None and self._parent.is_master_root()

    def get_bucket(self):
        """Recursively traces root parent for S3 bucket name."""
        return self.name if self.is_bucket() else self._parent.get_bucket()

    def get_s3_path(self):
        """Recursively collects S3 path excluding bucket name."""
        return path.join(self._parent.get_s3_path(), self._name) if not self.is_bucket() else ""

    def get_s3_call_data(self):
        """Returns tuple (bucket, s3_path) to fetch object details."""
        bucket = self.get_bucket()
        s3_path = self.get_s3_path()
        return (bucket, s3_path)

    def get_ancestry(self, parents: list) -> list:
        """Lists all parents including self order."""
        if self._parent:
            self._parent.get_ancestry(parents)
        parents.append(self)
        return parents

    def filename(self):
        """Returns filename."""
        return self._name

    def fetch_children(self, s3_handle):
        """Fetches children if not loaded for directory type object."""
        if s3_handle and not self._fetched:
            if self.is_master_root():
                self._children = []
                children = s3_handle.get_buckets(self)
            elif self.is_dir():
                children = s3_handle.get_objects(self)

            # Checking response for AWS call exceptions/errors
            if children is not None:
                self._fetched = True
                self._children.extend(children)
            else:
                self._fetched = False
        return self._children


    @property
    def name(self):
        """Property getter."""
        return self._name

    @property
    def parent(self):
        """Property getter."""
        return self._parent

    @property
    def root(self):
        """Property getter."""
        return self._root

    @property
    def fetched(self):
        """Property getter."""
        return self._fetched

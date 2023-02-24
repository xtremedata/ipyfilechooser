"""Helper functions for ipyfilechooser related to cloud storage sources.
"""

from os import path
from typing import Union

from .utils import match_item




class CloudClient:
    """ Interface for cloud handles.
    """

    @classmethod
    def get_master_root(cls): # pylint: disable=no-self-use
        """ Returns master root object for specific source.
        """
        raise RuntimeError("Not implemented")

    @classmethod
    def get_source_name(cls): # pylint: disable=no-self-use
        """ Returns the name of this storage source.
        """
        raise RuntimeError("Not implemented")


    def __init__(self):
        self._error = None


    def init_cred(self, params: tuple) -> bool: # pylint: disable=no-self-use
        """ Initializes credential attributes.
        """
        raise RuntimeError("Not implemented")

    def restore_cred(self, params: Union[tuple,list]): # pylint: disable=no-self-use
        """ Restores credential attributes.
        """
        raise RuntimeError("Not implemented")

    def check_cred_changed(self, access_cred: []) -> bool: # pylint: disable=no-self-use
        """ Returns true when access credentials has changed.
        """
        raise RuntimeError("Not implemented")

    def validate_cred(self) -> Union[None, bool]: # pylint: disable=no-self-use
        """Returns true when authentication is valid."""
        raise RuntimeError("Not implemented")

    def get_buckets(self, parent: str) -> Union[None,list]: # pylint: disable=no-self-use
        """ Returns available buckets/containers names.
        """
        raise RuntimeError("Not implemented")

    def get_objects(self, bucket: str, prefix: str="") -> Union[None,list]: # pylint: disable=no-self-use
        """ Returns list of objects for cloud path.
        """
        raise RuntimeError("Not implemented")

    def get_object(self, bucket: str, obj_path: str) -> Union[None,object]: # pylint: disable=no-self-use
        """ Retrieves selected object with provided cloud path.
        """
        raise RuntimeError("Not implemented")


    @property
    def error(self):
        """Property getter."""
        return self._error




class CloudObj: # pylint: disable=too-many-public-methods
    """ Represents cloud storage object (path).
    """

    MASTER_ROOT_STR = "//"
    ROOT_STR = ".."
    SHORT_STR = "..."
    SEP_STR = "/"

    # JS icon names
    #DEF_BUCKET_ICON = 'database'
    #DEF_DIR_ICON = 'folder'
    #DEF_FILE_ICOM = 'file'

    # Unicode emojis
    #DEF_BUCKET_ICON = '\U0001F5C3'
    DEF_BUCKET_ICON = '\U0001F5C4'
    DEF_DIR_ICON = '\U0001F4C1'
    DEF_FILE_ICOM = ''


    @classmethod
    def _make_dir(cls, name: str, parent=None):
        """ Creates directory like S3 object.
        """
        return cls(name, parent).init_children()

    @classmethod
    def _make_elm(cls, name: str, parent):
        """ Creates element like S3 object.
        """
        return cls(name, parent)

    @classmethod
    def make_root(cls, parent=None):
        """ Creates root like S3 object (a reference to it's parent).
        """
        return cls(None, parent, root=True)

    @classmethod
    def make_obj(cls, obj_path: str, parent=None):
        """ Creates either directory or a leaf element S3 object.
        """
        if obj_path is None:
            return None
        try:
            head,_ = obj_path.split(cls.SEP_STR,1)
            return cls._make_dir(head, parent)
        except ValueError:
            return cls._make_elm(obj_path, parent)


    def __init__(self, name: str, parent=None, root: bool=False):
        self._parent = parent
        self._name = name
        self._root = root
        self._children = None
        self._fetched = False
        self._sorted = False


    def __repr__(self) -> str:
        return type(self).__name__ + ":" + self.__str__()

    def __str__(self) -> str:
        return self.short_name()

    def __eq__(self, obj) -> bool:
        return self._name == (obj.name if isinstance(obj, CloudObj) \
                else obj if isinstance(obj, str) \
                else str(obj))

    def __hash__(self):
        return hash(self._name)

    def __lt__(self, other) -> bool:
        if self.is_master_root() or other.is_master_root():
            return not other.is_master_root()
        if self.is_dirup() or other.is_dirup():
            return not other.is_dirup()
        if self.is_bucket() or other.is_bucket():
            return True if not other.is_bucket() \
                    else self.name < other.name if self.is_bucket() \
                    else False
        if self.has_children() or other.has_children():
            return True if not other.has_children() \
                    else self.name < other.name if self.has_children() \
                    else False

        return self.name < other.name


    def init_children(self):
        """Initializes children - adds parent reference for directories."""
        self._children = [self.make_root(self)]
        return self

    def is_leaf(self) -> bool:
        """ Returns true if leaf."""
        return self._children is None

    def is_dir(self) -> bool:
        """Returns true if directory."""
        return self.has_children() or self.is_bucket() or self.is_dirup()

    def is_file(self) -> bool:
        """Returns true if file."""
        return not self.is_dir()

    def is_master_root(self) -> bool:
        """Returns true for master root - no parent."""
        return self.is_root() and self._parent is None

    def is_root(self) -> bool:
        """Returns true if root - reference to parent."""
        return bool(self._root)

    def is_dirup(self) -> bool:
        """Returns true if root, but not master root."""
        return self.is_root() and not self.is_master_root()

    def is_bucket(self) -> bool:
        """Returns true if bucket."""
        return self._parent is not None and self._parent.is_master_root()

    def has_children(self) -> bool:
        """Returns true if has children."""
        return self._children is not None

    def get_bucket(self) -> str:
        """Recursively traces root parent for S3 bucket name."""
        return self.name if self.is_bucket() else self._parent.get_bucket()

    def get_cloud_path_with_bucket(self) -> str:
        """ Recursively collects S3 path including bucket name.
            This path is intended for getting objects, thus as boto3 requires
            for 'get_object' it starts with '/'.
        """
        return path.join(self._parent.get_cloud_path_with_bucket(), self.name) \
                if not self.is_master_root() else self.MASTER_ROOT_STR # self.SEP_STR

    def get_cloud_path(self) -> str:
        """Recursively collects S3 path excluding bucket name."""
        return path.join(self._parent.get_cloud_path(), self.name) \
                if not self.is_master_root() and not self.is_bucket() else ""

    def get_cloud_call_data(self) -> tuple:
        """Returns tuple (bucket, obj_path) to fetch object details."""
        bucket = self.get_bucket()
        obj_path = self.get_cloud_path()
        return (bucket, obj_path)

    def get_ancestry(self, parents: list) -> list:
        """Lists all parents including self order."""
        if self._parent:
            self._parent.get_ancestry(parents)
        parents.append(self)
        return parents


    def filename(self) -> Union[str,None]:
        """Returns filename."""
        return self._name

    def check_cloud(self, obj_path) -> bool:
        """ Returns true for a matching storage source.
        """
        # pylint: disable=unidiomatic-typecheck
        return type(self) == type(obj_path) \
                or isinstance(obj_path, str) and obj_path.startswith(self.MASTER_ROOT_STR)
        # pylint: enable=unidiomatic-typecheck

    def check_short_name(self, short_name) -> bool:
        """ Compares short name with self - special for the master root.  """
        return short_name == self.MASTER_ROOT_STR \
                if self.is_master_root() else short_name == self.short_name()

    def short_name(self) -> Union[str,None]:
        """Returns short name."""
        return self.MASTER_ROOT_STR if self.is_master_root() \
                else self.ROOT_STR if self.is_root() \
                else self._name

    def find_path(self, obj_path: str, cloud_handle, filter_pattern: Union[None,str]=None): # pylint: disable=too-many-return-statements
        """Returns object mathing path argument or None."""
        if not obj_path:
            return None
        if not obj_path.startswith(self.short_name()):
            return None
        try:
            root_name, tail_name = obj_path.strip(self.SEP_STR).split(self.SEP_STR, 1)
        except ValueError:
            return self if self.check_short_name(obj_path) else None
        if not self._fetched:
            self.fetch_children(cloud_handle, filter_pattern=filter_pattern)
        if not self.has_children():
            return self
        for child in self._children:
           res = child.find_path(tail_name.strip(self.SEP_STR), cloud_handle)
           if res is not None:
               return res
        return None

    def find_path_ancestry(self, obj_path: str, cloud_handle) -> []:
        """Returns list of ancestry matching string path."""
        res = self.find_path(obj_path, cloud_handle)
        ancestry = []
        return res.get_ancestry(ancestry) if res else ancestry

    def find(self, cloud_obj):
        """Returns object matching argument or None."""
        if not self.has_children():
            return None
        try:
            return self._children[self._children.index(cloud_obj)]
        except ValueError:
            return None

    def _add(self, cloud_obj):
        """Adds a child, returns existing if already a member."""
        orig_cloud_obj = self.find(cloud_obj)
        if orig_cloud_obj is not None:
            return orig_cloud_obj
        if not self.has_children():
            self._children = []
        self._children.append(cloud_obj)
        cloud_obj.parent = self
        self._sorted = False
        return cloud_obj

    def fetch_object(self, cloud_handle) -> object:
        """Fetches AWS S3 object."""
        bucket, obj_path = self.get_cloud_call_data()
        return cloud_handle.get_object(bucket, obj_path)

    def fetch_children(self, \
            cloud_handle, \
            filter_pattern: Union[str,None]=None) -> Union[list,None]:
        """ Fetches children if not loaded for directory type object.

            @see self.parse_objpaths and self._parse_children methods
        """
        if cloud_handle and not self._fetched:
            if self.is_master_root():
                self._children = []
                return self._parse_children( \
                        cloud_handle.get_buckets(self.name), \
                        filter_pattern, \
                        buckets=True)
            if self.is_dir():
                bucket, prefix = self.get_cloud_call_data()
                return self._parse_children( \
                        cloud_handle.get_objects(bucket, prefix), \
                        filter_pattern, \
                        buckets=False)
        return self._children

    def parse_objpaths(self, \
            paths: Union[list,None], \
            filter_pattern: Union[None,str]=None) -> Union[list,None]:
        """ Parses AWS S3 objects paths (not buckets).

            Marks "fetched" flag, to allow this method to also be used to test
            parsing paths without fetching actual data.
        """
        children_map = {}
        for child in paths:
            if child:
                try:
                    head, tail = child.split(self.SEP_STR, 1)
                except ValueError:
                    # detected leaf - just adding to children and forget
                    if match_item(child, filter_pattern):
                        self._children.append(self._make_elm(child, self))
                else:
                    # detected directory - requires recursive processing
                    try:
                        children_map[head].append(tail)
                    except KeyError:
                        children_map[head] = [tail]
        for child, descendents in children_map.items():
            dir_child = self._make_dir(child, self)
            if descendents:
                dir_child.parse_objpaths(descendents, filter_pattern)
            self._children.append(dir_child)
        # All AWS S3 objects are retrieved at once
        self._fetched = True
        self._sorted = False
        return self._children

    def _parse_children(self, \
            children: Union[list,None], \
            filter_pattern: Union[str,None]=None, \
            buckets: bool=True) -> Union[list,None]:
        """Parses fetched from AWS string data of available objects in a bucket."""
        # Checking response for AWS call exceptions/errors
        if children is None:
            self._fetched = False
            self._sorted = False
        elif buckets:
            self._children = [self._make_dir(bname, parent=self) for bname in children]
            self._fetched = True
            self._sorted = False
        elif children:
            self.parse_objpaths(children, filter_pattern)
        return self._children

    def _prep_children(self) -> bool:
        """Prepares list of children (fetches if needed, sorts, etc.)."""
        if self._fetched and self.has_children():
            if not self._sorted:
                self._children.sort()
                self._sorted = True
        return self.has_children()

    def ui_name_1(self, bucket_icon, dir_icon, file_icon) -> str:
        """Returns string representation of this object for UI display."""
        if self.is_bucket():
            return f"{bucket_icon} {self.short_name()}" if bucket_icon else f"{self.short_name()}"
        if self.is_dir():
            return f"{dir_icon} {self.short_name()}" if dir_icon else f"{self.short_name()}"
        return f"{self.short_name()} {file_icon}" if file_icon else f"{self.short_name()}"

    def ui_fullpath(self) -> str:
        """Returns full path for the S3 object."""
        return path.join(self._parent.ui_fullpath(), self.short_name()) \
                if not self.is_master_root() else self.short_name()

    def get_path_tuple(self) -> tuple:
        """Returns tuple for UI widget with full path."""
        return (self.ui_fullpath(), self)

    def get_path_list(self) -> [tuple]:
        """Prepares list of children for UI display as strings."""
        return [o.get_path_tuple() for o in self.get_ancestry([])]

    def get_dir_tuple(self, bucket_icon=None, dir_icon=None, file_icon=None) -> tuple:
        """Returns tuple for UI widget with basename and icons."""
        return (self.ui_name_1(bucket_icon, dir_icon, file_icon), self)

    # pylint: disable=too-many-arguments
    def get_dir_list(self, \
            cloud_handle, \
            bucket_icon=None, \
            dir_icon=None, \
            file_icon=None, \
            filter_pattern: Union[None,str]=None) -> [tuple]:
        """Prepares list of children for UI display as strings."""
        bucket_icon = bucket_icon if bucket_icon else self.DEF_BUCKET_ICON
        dir_icon = dir_icon if dir_icon else self.DEF_DIR_ICON
        file_icon = file_icon if file_icon else self.DEF_FILE_ICOM
        if not self._fetched:
            self.fetch_children(cloud_handle, filter_pattern=filter_pattern)
        return [o.get_dir_tuple(bucket_icon, dir_icon, file_icon) for o in self._children] \
                if self._prep_children() else []
    # pylint: enable=too-many-arguments


    @property
    def name(self):
        """Property getter."""
        #return path.basename(self._name) if self._name else ''
        return self._name

    @property
    def parent(self):
        """Property getter."""
        return self._parent
    @parent.setter
    def parent(self, new_parent):
        """Property setter."""
        self._parent = new_parent

    @property
    def root(self):
        """Property getter."""
        return self._root

    @property
    def fetched(self):
        """Property getter."""
        return self._fetched

    @property
    def children(self):
        """Property getter."""
        return self._children

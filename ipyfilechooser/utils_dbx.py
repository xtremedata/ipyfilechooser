"""
    Helper functions for ipyfilechooser to provide dbX support.
"""


from os import path
from typing import Union


class DbxMeta:
    """ Represents dbX support for metadata files.
    """

    SFX_SEP = '_'
    DBX_EXT = ".json"
    DBX_SFX = ('meta','histo','pattern','rank','summary','topn')
    DBX_SFX_SET = set(DBX_SFX)
    META_LABEL = 'label'

    @classmethod
    def get_dbx_suffixes(cls) -> tuple:
        """ Returns dbX metadata set suffixes.
        """
        return cls.DBX_SFX

    @classmethod
    def is_dbx_metafile(cls, dbx_sfx: str, ext: str) -> bool:
        """ Returns True for file matching dbX metadata file.
        """
        return dbx_sfx.lower() in cls.DBX_SFX_SET and ext.lower() == cls.DBX_EXT

    @classmethod
    def split_dbx_metafile(cls, filename: str) -> Union[tuple,None]:
        """ Returns root dbX suffix and extension for dbX metafile None otherwise
        """
        try:
            fileroot, ext = path.splitext(filename)
            fileroot, dbx_sfx = fileroot.rsplit(cls.SFX_SEP, 1)
        except (ValueError, TypeError):
            return None

        if not cls.is_dbx_metafile(dbx_sfx, ext):
            return None

        return (fileroot, dbx_sfx, ext)

    @classmethod
    def check_for_dbx_meta_member(cls, fileroot:str, filename: str, fobj: object, results: dict):
        """ Checks for file if belongs to dbX meta group and properly updates results.
        """
        try:
            root, dbx_sfx, ext = cls.split_dbx_metafile(filename)
            if cls.is_dbx_metafile(dbx_sfx, ext) and fileroot == root:
                results[dbx_sfx] = fobj
        except (ValueError, TypeError):
            pass

    @classmethod
    def get_dbx_files(cls, files: list, pattern: str='') -> []:
        """ Retuns files matching dbX metadata from the provided set.
        """
        res = []
        for file in files:
            if not pattern or file.startswith(pattern):
                res.append(file)
        return res

    @classmethod
    def get_dbx_like_files(cls, files: list, filename: str) -> list:
        """ Returns all files from the list matching filename complementing for dbX metdata.
            @files      a dictionary with records: filename: object
            @filename   a selected filename
        """
        res = {}
        try:
            fileroot, _, _ = cls.split_dbx_metafile(filename)
        except (ValueError, TypeError):
            pass
        else:
            for fname, fobj in files.items():
                cls.check_for_dbx_meta_member(fileroot, fname, fobj, res)
            res[cls.META_LABEL] = fileroot
        return res

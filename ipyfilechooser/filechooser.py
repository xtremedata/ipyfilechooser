"""
ipywidget: file chooser.

ToDo:
 - _set_form_values called twice per dircontent selection (possibly others) - the same stack!
 - get_objects called 7 times

 Debugging: traceback.print_stack()
"""

import os
import warnings
#import traceback

from enum import Enum
from typing import Optional, Sequence, Mapping, Callable, Union
from ipywidgets import \
        Widget, Dropdown, Text, Select, Button, HTML, \
        Layout, GridBox, Box, HBox, VBox, ValueWidget, \
        Checkbox

# Local Imports
from .errors import ParentPathError, InvalidFileNameError, InvalidSourceError
from .utils import \
        get_subpaths,\
        get_dir_contents,\
        match_item,\
        strip_parent_path, \
        is_valid_filename, \
        get_drive_letters, \
        normalize_path, \
        has_parent_path, \
        read_data as read_local_data, \
        save_data as save_local_data
from .utils_sources import \
        SupportedSources, \
        AccCred, \
        is_valid_source
from .utils_cloud import \
        CloudObj, \
        read_data as read_cloud_data
from .utils_s3 import S3, S3Obj
from .utils_azure import AzureClient, AzureObj
from .utils_dbx import DbxMeta


class FileChooser(VBox, ValueWidget): # pylint: disable=too-many-public-methods, too-many-ancestors, too-many-instance-attributes
    """FileChooser class."""

    _LBL_TEMPLATE = '<span style="color:{1};">{0}</span>'
    _LBL_NOFILE = 'No selection'

    def __init__(
            self,
            path: str = os.getcwd(),
            filename: str = '',
            title: str = '',
            source: SupportedSources = SupportedSources.LOCAL,
            select_desc: str = 'Select',
            change_desc: str = 'Change',
            read_desc: str = 'Read',
            read_json_desc: str = 'Read JSON',
            read_dbx_meta_desc: str = 'Read dbX Metadata',
            save_desc: str = 'Save',
            save_json_desc: str = 'Save JSON',
            save_overwrite_desc: str = 'Overwrite',
            save_dbx_meta_desc: str = 'Save dbX Metadata',
            download_desc: str = 'Download',
            show_hidden: bool = False,
            select_default: bool = False,
            dir_icon: Optional[str] = '\U0001F4C1 ',
            dir_icon_append: bool = False,
            show_only_dirs: bool = False,
            disable_source: bool = False,
            filter_pattern: Optional[Sequence[str]] = None,
            sandbox_path: Optional[str] = None,
            layout: Layout = Layout(width='90%'),
            **kwargs): # pylint: disable=too-many-arguments, too-many-locals, too-many-statements
        """Initialize FileChooser object."""
        # Check if path and sandbox_path align
        if sandbox_path and not self._has_parent_path( \
                self._normalize_path(path, source), \
                self._normalize_path(sandbox_path, source), \
                source):
            raise ParentPathError(path, sandbox_path)

        # Verify the filename is valid
        if not is_valid_filename(filename):
            raise InvalidFileNameError(filename)

        # Verify the source is valid
        if not is_valid_source(source):
            raise InvalidSourceError(source)

        self._default_filename = filename
        self._default_source = source
        self._selected_path: Optional[str] = None
        self._selected_filename: Optional[str] = None
        self._show_hidden = show_hidden
        self._select_desc = select_desc
        self._change_desc = change_desc
        self._read_desc = read_desc
        self._read_json_desc = read_json_desc
        self._read_dbx_meta_desc = read_dbx_meta_desc
        self._save_desc = save_desc
        self._save_json_desc = save_json_desc
        self._save_overwrite_desc = save_overwrite_desc
        self._save_dbx_meta_desc = save_dbx_meta_desc
        self._download_desc = download_desc
        self._select_default = select_default
        self._dir_icon = dir_icon
        self._dir_icon_append = dir_icon_append
        self._show_only_dirs = show_only_dirs
        self._disable_source = disable_source
        self._filter_pattern = filter_pattern
        self._sandbox_path = self._normalize_path(sandbox_path, source) if sandbox_path is not None else None
        self._callback: Optional[Callable] = None
        self._local = None # A placeholder to move local paths/status into a separate object
        self._cloud = None
        self._cloud_clients = {}
        self._sources_backup = {}
        self._map_name_to_disp = None
        self._map_disp_to_name = None
        self._file_size_limit = 1 << 17 # 127kB
        self._data = None
        self._data_error = None

        # Widgets
        self._sourcelist = Dropdown(
            description="Select Source Storage:",
            options=SupportedSources.elements(),
            value=self._default_source,
            disabled=self._disable_source,
            layout=Layout(
                width='auto',
                grid_area='sourcelist'
            )
        )
        self._access_cred = AccCred.create(
            self._default_source,
            self._access_cred_name()
        )
        self._pathlist = Dropdown(
            description="",
            layout=Layout(
                width='auto',
                grid_area='pathlist'
            )
        )
        self._filename = Text(
            placeholder='output filename',
            layout=Layout(
                width='auto',
                grid_area='filename',
                display=(None, "none")[self._show_only_dirs]
            ),
            disabled=self._show_only_dirs
        )
        self._dircontent = Select(
            rows=8,
            layout=Layout(
                width='auto',
                grid_area='dircontent'
            )
        )
        self._select = Button(
            description=self._select_desc,
            layout=Layout(
                min_width='6em',
                width='6em'
            )
        )
        self._cancel = Button(
            description='Cancel',
            layout=Layout(
                min_width='6em',
                width='6em'
            )
        )
        self._read = Button(
            description=self._read_desc,
            layout=Layout(
                min_width='6em',
                width='6em'
            )
        )
        self._read_json = Checkbox(
            description=self._read_json_desc,
            value=True,
            indent=False,
            layout=Layout(
                min_width='8em',
                width='10em'
            )
        )
        self._read_dbx_meta = Checkbox(
            description=self._read_dbx_meta_desc,
            value=False,
            indent=False,
            layout=Layout(
                min_width='8em',
                width='10em'
            )
        )
        self._save = Button(
            description=self._save_desc,
            layout=Layout(
                min_width='6em',
                width='6em'
            )
        )
        self._save_json = Checkbox(
            description=self._save_json_desc,
            value=True,
            indent=False,
            layout=Layout(
                min_width='8em',
                width='10em'
            )
        )
        self._save_overwrite = Checkbox(
            description=self._save_overwrite_desc,
            indent=False,
            layout=Layout(
                min_width='8em',
                width='10em'
            )
        )
        self._save_dbx_meta = Checkbox(
            description=self._save_dbx_meta_desc,
            value=False,
            indent=False,
            layout=Layout(
                min_width='8em',
                width='10em'
            )
        )
        self._download = Button(
            description=self._download_desc,
            layout=Layout(
                min_width='6em',
                width='8em',
                display = 'none'
            )
        )
        self._title = HTML(
            value=title
        )

        if title == '':
            self._title.layout.display = 'none'

        # Handling default path due to possible different sources - needs widgets
        normalized_path = self._normalize_path(path, self._default_source)
        if self._check_integrity(normalized_path):
            self._default_path = normalized_path
        elif SupportedSources.is_cloud(self._default_source):
            self._default_path = ''
        else:
            self._default_path = normalized_path

        # Widgets' style settings
        self._sourcelist.style.description_width = 'auto' # pylint: disable=no-member

        # Widget observe handlers
        #self._observe_sourcelist()
        #self._pathlist.observe(self._on_pathlist_select, names='value')
        #self._dircontent.observe(self._on_dircontent_select, names='value')
        #self._filename.observe(self._on_filename_change, names='value')
        self._select.on_click(self._on_select_click)
        self._cancel.on_click(self._on_cancel_click)
        self._read.on_click(self._on_read_click)
        self._save.on_click(self._on_save_click)

        # Selected file label
        self._label = HTML(
            value=self._LBL_TEMPLATE.format(self._LBL_NOFILE, 'black'),
            placeholder='',
            description='',
            layout=Layout(margin='0 0 0 1em')
        )

        # All GridBox children with membership conditions
        self._all_gb_children = {
                self._get_sourcelist: lambda: True,
                self._get_access_cred: lambda: self._sourcelist.value.req_access_cred(),
                self._get_pathlist: lambda: True,
                self._get_filename: lambda: not self._show_only_dirs,
                self._get_dircontent: lambda: True
        }

        # Layout
        self._gb = GridBox(
            layout=Layout(
                display='none',
                width='auto',
                grid_gap='0px 0px',
                grid_template_rows='auto auto auto auto',
                grid_template_columns='60% 40%',
            )
        )
        self._update_gridbox()

        statusbar = HBox(
            children=[
                Box([self._label], layout=Layout(overflow='auto'))
            ],
            layout=Layout(width='auto')
        )

        buttonbar_layout = Layout(width='auto', \
                justify_content='space-between')

        buttonbar = HBox(
            children=[
                self._select,
                self._cancel,
                self._download
            ],
            layout=Layout(width='auto')
        )

        buttonbar_read = HBox(
            children=[
                self._read,
                Box(
                    children=[
                        self._read_json,
                        self._read_dbx_meta
                    ],
                )
            ],
            layout=buttonbar_layout
        )

        buttonbar_save = HBox(
            children=[
                self._save,
                Box(
                    children=[
                        self._save_json,
                        self._save_overwrite,
                        self._save_dbx_meta
                    ],
                )
            ],
            layout=buttonbar_layout
        )

        # Call setter to set initial form values
        self._init_cloud()
        self._process_access_cred_change()

        # Use the defaults as the selected values
        if self._select_default:
            self._apply_selection()

        # Call VBox super class __init__
        super().__init__(
            children=[
                self._title,
                self._gb,
                statusbar,
                buttonbar,
                buttonbar_read,
                buttonbar_save
            ],
            layout=layout,
            **kwargs
        )

    def _get_sourcelist(self) -> Widget:
        return self._sourcelist
    def _get_access_cred(self) -> Widget:
        return self._access_cred.widget
    def _get_pathlist(self) -> Widget:
        return self._pathlist
    def _get_filename(self) -> Widget:
        return self._filename
    def _get_dircontent(self) -> Widget:
        return self._dircontent

    def _update_gridbox(self) -> None:
        """ Updates GridBox attributes based on user requests.
            Changed attributes:
            - children
            - layout.grid_template_areas
        """
        # disabling widget
        access_cred_name = self._access_cred_name()
        req_acc_cred = self._sourcelist.value.req_access_cred()

        self._gb.disabled = True
        self._gb.layout.display = 'none'
        self._gb.children = [child_fun() \
                for child_fun,cond_fun in self._all_gb_children.items() \
                if cond_fun()]
        self._gb.layout.grid_template_areas = \
                "\n'sourcelist sourcelist'" \
                "\n{access_cred_tmpl}'pathlist {filename_tmpl}'" \
                "\n'dircontent dircontent'\n".format( \
                    access_cred_tmpl=('', f"'{access_cred_name} {access_cred_name}'\n")[req_acc_cred], \
                    filename_tmpl=('filename', 'pathlist')[self._show_only_dirs]) # pylint: disable=all
        # restoring view
        self._gb.disabled = False
        self._gb.layout.display = None

    def _has_parent_path(self, path: str, parent_path: Optional[str], source: Union[SupportedSources,None]=None) -> bool:
        """Verifies if path falls under parent_path."""
        if (self._sourcelist.value if source is None else source) == SupportedSources.LOCAL:
            return has_parent_path(path, parent_path)
        return True # no verification for cloud sources

    def _normalize_path(self, path: str, source: Union[SupportedSources,None]=None) -> str:
        """Normalize a path string."""
        if (self._sourcelist.value if source is None else source) == SupportedSources.LOCAL:
            return normalize_path(path)
        return path

    def _has_access_cred(self) -> bool:
        """ Returns True if proper access credentials are provided.
            Access Credentials widget has to be visible.
        """
        return self._access_cred.is_set()

    def _access_cred_changed(self) -> bool:
        """ Returns True if access credentials changed.
        """
        return self._cloud and self._cloud.check_cred_changed(self._access_cred.values)

    def _access_cred_name(self) -> str:
        """ Returns access credentials widget name for layout template.
            Name different for different storage sources to differentiate browser passwords.
        """
        return f"access_cred_{self._sourcelist.value.name}"

    def _validate_cred(self) -> bool:
        try:
            self._deactivate()
            res = self._cloud is not None \
                    and self._cloud.init_cred(self._access_cred.values) \
                    and self._cloud.validate_cred()
        except Exception as ex:
            warnings.warn(f"Failed to validate access credential: {ex}")
            return False
        else:
            return res
        finally:
            self._activate()

    def _show_access_cred(self, enable: Optional[bool] = None) -> None:
        """ Disables(hides)/enables(shows) access credentials widgets.
            All widgets are activates/deactivated as well.
        """
        if enable is None:
            enable = self._sourcelist.value.req_access_cred()
        self._access_cred.enabled = enable
        self._observe_access_cred(enable)

    def _observe_access_cred(self, observe: Optional[bool] = None) -> None:
        """Activates(observes)/deactivates(unobservs) access credentials widgets."""
        if observe is None:
            observe = self._sourcelist.value.req_access_cred()
        self._access_cred.observe = self._on_access_cred_change if observe else None

    def _observe_sourcelist(self, enable: Optional[bool] = True) -> None:
        if self._disable_source:
            pass
        elif enable:
            self._sourcelist.observe(self._on_sourcelist_select, names='value')
        else:
            try:
                self._sourcelist.unobserve(self._on_sourcelist_select, names='value')
            except (KeyError, ValueError):
                pass

    def _backup_source(self, old: Enum) -> None:
        """ Saves/restores selection on source change.
        """
        if old:
            if old == SupportedSources.LOCAL:
                path = self._expand_path(self._pathlist.value)
                try:
                    filename = self._map_disp_to_name[self._dircontent.value]
                except (KeyError, AttributeError):
                    filename = None
            else:
                path = self._pathlist.value.get_cloud_path_with_bucket() if self._pathlist.value else None
                filename = self._dircontent.value.filename() if self._dircontent.value else None
            self._sources_backup[old] = (path, filename)

    def _process_source_change(self, old: Union[Enum,None] = None, new: Union[Enum,None] = None) -> None:
        """Processes storage source change."""
        self._show_access_cred(False)
        self._access_cred = AccCred.create(
            self._sourcelist.value,
            self._access_cred_name())
        # Reset the dialog
        path, filename = (None, None)
        self._backup_source(old)
        try:
            path, filename = self._sources_backup[new]
        except (TypeError, KeyError):
            pass
        self.reset(path=path, filename=filename)
        self._show_access_cred(True)
        self._update_gridbox()

    def _process_access_cred_change(self) -> None:
        """Processes cloud storage access credentials change."""
        self._clear_form_values(clear_access_cred=False)
        # Fail early - test connection
        if self._validate_cred():
            self._set_form_values( \
                    self._sourcelist.value, \
                    None, \
                    None)
        elif not self._sourcelist.value.req_access_cred():
            self._set_form_values( \
                    self._default_source, \
                    self._default_path, \
                    self._default_filename)
        else:
            self._cloud_storage_error("Invalid Credentials or connection error")

    def _init_s3(self, client_enum: Enum) -> None:
        """ Creates/initializes the S3 client.
        """
        try:
            self._cloud = self._cloud_clients[client_enum]
            self._cloud.restore_cred(self._access_cred.children)
        except KeyError:
            self._cloud_clients[client_enum] = S3()
            self._cloud = self._cloud_clients[client_enum]
            self._cloud.init_cred(self._access_cred.values)

    def _init_azure(self, client_enum: Enum) -> None:
        """ Creates/initializes the Azure client.
        """
        try:
            self._cloud = self._cloud_clients[client_enum]
            self._cloud.restore_cred(self._access_cred.children)
        except KeyError:
            self._cloud_clients[client_enum] = AzureClient()
            self._cloud = self._cloud_clients[client_enum]
            self._cloud.init_cred(self._access_cred.values)

    def _init_cloud(self) -> None:
        """ Creates/initializes the cloud storage client.
        """
        if self._sourcelist.value == SupportedSources.LOCAL:
            self._cloud = None
        elif self._sourcelist.value == SupportedSources.AWS:
            self._init_s3(self._sourcelist.value)
        elif self._sourcelist.value == SupportedSources.AZURE:
            self._init_azure(self._sourcelist.value)
        else:
            warnings.warn(f"Storage source '{self._sourcelist.value:.10}' not implemented/uknown")

    def _check_integrity(self, path: str) -> bool:
        """ Returns true when data structure matches selected data source.
        """
        obj = self._pathlist.value
        source = self._sourcelist.value
        cloud = self._cloud
        if obj is None:
            return True
        if source == SupportedSources.LOCAL and not isinstance(obj, str) \
                or source == SupportedSources.AWS and not isinstance(obj, S3Obj) and (not isinstance(cloud, S3) or cloud is None) \
                or source == SupportedSources.AZURE and not isinstance(obj, AzureObj) and (not isinstance(cloud, AzureClient) or cloud is None):
                    return False
        return obj.check_cloud(path) if path and obj else False


    def _cloud_storage_error(self, msg: str) -> None:
        """ Reports cloud storage error."""
        self._label.value = self._LBL_TEMPLATE.format(msg, 'red')

    def _clear_form_values(self, clear_access_cred: bool) -> None:
        """ Clears values for widgets presenting directories and files on source change.
            Called on:
            - storage source change
            - access credentials change
            - reset
        """
        self._pathlist.options = []
        self._filename.value = ''
        self._dircontent.options = []
        self._label.value = self._LBL_TEMPLATE.format(self._LBL_NOFILE, 'black')
        if clear_access_cred:
            self._access_cred.clear()
            self._init_cloud()

    def _deactivate(self) -> None:
        """ Deactivates triggers.
        """
        self._sourcelist.disabled = True
        self._pathlist.disabled = True
        self._dircontent.disabled = True
        self._observe_sourcelist(enable=False)
        try:
            self._pathlist.unobserve(self._on_pathlist_select, names='value')
        except (KeyError, ValueError):
            pass
        try:
            self._dircontent.unobserve(self._on_dircontent_select, names='value')
        except (KeyError, ValueError):
            pass
        try:
            self._filename.unobserve(self._on_filename_change, names='value')
        except (KeyError, ValueError):
            pass
        self._observe_access_cred(observe=False)

    def _activate(self) -> None:
        """ Activate triggers.
        """
        self._observe_sourcelist()
        self._pathlist.observe(self._on_pathlist_select, names='value')
        self._dircontent.observe(self._on_dircontent_select, names='value')
        self._filename.observe(self._on_filename_change, names='value')
        if self._sourcelist.value.req_access_cred():
            self._observe_access_cred()
        self._sourcelist.disabled = self._disable_source
        self._pathlist.disabled = False
        self._dircontent.disabled = False

    def _update_widgets_on_set(self, \
            is_valid_file: bool, \
            is_file: bool=None, \
            deactivate_dialog: bool=False) -> None:
        """ Updates widgets on change.
            Notably allows action buttons based on selected object type.
        """
        if is_file is None:
            is_file = is_valid_file
        if deactivate_dialog:
            self._select.disabled = False
            self._cancel.disabled = True
            self._download.disabled = True # not is_valid_file
            self._read.disabled = True # not is_valid_file
            self._save.disabled = True # not is_valid_file
        elif is_valid_file:
            self._select.disabled = False
            self._cancel.disabled = False
            self._download.disabled = False
            self._read.disabled = False
            self._save.disabled = False or self._data is None
        else:
            self._select.disabled = self._gb.layout.display is None
            self._cancel.disabled = False
            self._download.disabled = True
            self._read.disabled = True
            self._save.disabled = not is_file or self._data is None

    def _set_form_values_cloud(self, path: CloudObj, filename: str) -> None: # pylint: disable=too-many-branches
        """Set the form values for the cloud storage."""
        proceed = True
        # Process only with provided credentials
        if self._has_access_cred():
            if not self._cloud:
                self._init_cloud()

            # Preps
            if self._show_only_dirs:
                filename = ''
            elif filename is None:
                filename = ''

            # Fetch buckets
            if path is None:
                path = self._cloud.get_master_root()
                filename = ''
            elif not self._check_integrity(path):
                warnings.warn("Runtime error: invalid object for cloud storage" \
                        + f": {type(path).__name__}:'{path:10}'")
                self._clear_form_values(clear_access_cred=True)
                path = self._cloud.get_master_root()
                filename = ''
                proceed = False
            elif isinstance(path, str):
                root_obj = self._cloud.get_master_root()
                path_obj = self._pathlist.value
                dir_obj = self._dircontent.value
                obj = None
                if dir_obj and path_obj and path_obj.ui_fullpath() == path and dir_obj.filename() == filename:
                    obj = path_obj
                if not obj:
                    # need to handle to restore path on source change
                    obj = root_obj.find_path(path, self._cloud)
                if not obj:
                    self._clear_form_values(clear_access_cred=True)
                    obj = root_obj
                # restoring pre-change selection or start from root if not found
                #print("### po:", path_obj, ", do:", dir_obj, ", o:", obj, ", p:", path)
                path = obj

            if proceed:
                if path.is_dirup():
                    path = path.parent.parent
                    filename = ''

                self._pathlist.options = path.get_path_list()
                self._pathlist.value = path
                self._dircontent.options = path.get_dir_list(self._cloud, filter_pattern=self._filter_pattern)
                if not filename:
                    self._dircontent.value = None
                else:
                    for s,o in self._dircontent.options:
                        if o.filename() == filename:
                            self._dircontent.value = s
                            break
                self._filename.value = filename
                if not path.fetched:
                    self._cloud_storage_error(self._cloud.error)
                else:
                    self._label.value = self._LBL_TEMPLATE.format(self._LBL_NOFILE, 'black')

            self._update_widgets_on_set(is_valid_file=bool(filename))

    def _set_form_values_local(self, path: str, filename: str) -> None: # pylint: disable=too-many-locals
        """Set the form values for the local storage."""
        # Check if the path falls inside the configured sandbox path
        if path is None:
            path = self._default_path

        if self._sandbox_path and not self._has_parent_path(path, self._sandbox_path):
            raise ParentPathError(path, self._sandbox_path)

        try:
            # Fail early if the folder can not be read
            _ = os.listdir(path)

            # In folder only mode zero out the filename
            if self._show_only_dirs:
                filename = ''

            # Set form values
            restricted_path = self._restrict_path(path)
            subpaths = get_subpaths(restricted_path)

            if os.path.splitdrive(subpaths[-1])[0]:
                # Add missing Windows drive letters
                drives = get_drive_letters()
                subpaths.extend(list(set(drives) - set(subpaths)))

            self._pathlist.options = subpaths
            self._pathlist.value = restricted_path
            self._filename.value = filename

            # file/folder real names
            dircontent_real_names = get_dir_contents(
                path,
                show_hidden=self._show_hidden,
                show_only_dirs=self._show_only_dirs,
                dir_icon=None,
                filter_pattern=self._filter_pattern,
                top_path=self._sandbox_path
            )

            # file/folder display names
            dircontent_display_names = get_dir_contents(
                path,
                show_hidden=self._show_hidden,
                show_only_dirs=self._show_only_dirs,
                dir_icon=self._dir_icon,
                dir_icon_append=self._dir_icon_append,
                filter_pattern=self._filter_pattern,
                top_path=self._sandbox_path
            )

            # Dict to map real names to display names
            self._map_name_to_disp = dict(zip(
                        dircontent_real_names,
                        dircontent_display_names))

            # Dict to map display names to real names
            self._map_disp_to_name = {
                disp_name: real_name
                for real_name, disp_name in self._map_name_to_disp.items()
            }

            # Set _dircontent form value to display names
            self._dircontent.options = dircontent_display_names

            # If the value in the filename Text box equals a value in the
            # Select box and the entry is a file then select the entry.
            if ((filename in dircontent_real_names) \
                and os.path.isfile(os.path.join(path, filename))):
                self._dircontent.value = self._map_name_to_disp[filename]
            else:
                self._dircontent.value = None

            # Update the state of the select button
            if self._gb.layout.display is None:
                # Disable the select button if path and filename
                # - equal an existing folder in the current view
                # - contains an invalid character sequence
                # - equal the already selected values
                # - don't match the provided filter pattern(s)
                check1 = filename in dircontent_real_names
                check2 = os.path.isdir(os.path.join(path, filename))
                check3 = not is_valid_filename(filename)
                check4 = False
                check5 = False

                # Only check selected if selected is set
                if ((self._selected_path is not None) and (self._selected_filename is not None)):
                    selected = os.path.join(self._selected_path, self._selected_filename)
                    check4 = os.path.join(path, filename) == selected

                # Ensure only allowed extensions are used
                if self._filter_pattern:
                    check5 = not match_item(filename, self._filter_pattern)

                #self._select.disabled = (check1 and check2) or check3 or check4 or check5
                not_a_valid_file = (check1 and check2) or check3 or check4 or check5
                self._update_widgets_on_set(not not_a_valid_file, bool(filename))
        except PermissionError:
            # Deselect the unreadable folder and generate a warning
            self._dircontent.value = None
            warnings.warn(f'Permission denied for {path}', RuntimeWarning)

    def _set_form_values(self, source: str, path: Union[str,CloudObj], filename: str) -> None:
        """Set the form values."""
        try:
            # Disable triggers to prevent selecting an entry in the Select
            # box from automatically triggering a new event.
            self._deactivate()
            if self._sourcelist.value == SupportedSources.LOCAL:
                self._set_form_values_local(path, filename)
            elif SupportedSources.is_cloud(self._sourcelist.value):
                self._set_form_values_cloud(path, filename)
            else:
                warnings.warn(f"Storage source '{source.name:.10}' not implemented/uknown")
        except Exception as ex:
            warnings.warn(f"Failed to set form values: {ex}")
            raise
        finally:
            # Reenable triggers
            self._activate()

    def _on_sourcelist_select(self, change: Mapping[Enum, Enum]) -> None: # pylint: disable=unused-argument
        """Handles selecting a storage source."""
        self._process_source_change(change['old'], change['new'])

    def _on_access_cred_change(self, change: Mapping[Enum, Enum]) -> None: # pylint: disable=unused-argument
        """Handles changing storage source access credentials."""
        if self._has_access_cred():
            if self._access_cred_changed():
                self._process_access_cred_change()

    def _on_pathlist_select_local(self, change: Mapping[str, str]) -> None:
        """Handle selecting a path entry."""
        self._set_form_values( \
                self._sourcelist.value, \
                self._expand_path(change['new']), \
                self._filename.value)

    def _on_pathlist_select_cloud(self, change: Mapping[str, str]) -> None:
        """Handle selecting a path entry."""
        self._set_form_values( \
                self._sourcelist.value, \
                change['new'], \
                self._filename.value)

    def _on_pathlist_select(self, change: Mapping[str, str]) -> None:
        """Handle selecting a path entry."""
        if self._sourcelist.value == SupportedSources.LOCAL:
            self._on_pathlist_select_local(change)
        elif SupportedSources.is_cloud(self._sourcelist.value):
            self._on_pathlist_select_cloud(change)

    def _on_dircontent_select_local(self, change: Mapping[str, str]) -> None:
        """Handle selecting a folder entry for local storage."""
        new_path = os.path.realpath(os.path.join(
            self._expand_path(self._pathlist.value),
            self._map_disp_to_name[change['new']]
        ))

        # Check if folder or file
        if os.path.isdir(new_path):
            path = new_path
            filename = self._filename.value
        else:
            path = self._expand_path(self._pathlist.value)
            filename = self._map_disp_to_name[change['new']]

        self._set_form_values( \
                self._sourcelist.value, \
                path, \
                filename)

    def _on_dircontent_select_cloud(self, change: Mapping[str, str]) -> None:
        """Handle selecting a folder entry for cloud."""
        selected = change['new']

        if selected is None:
            path = None
            filename = None

        elif selected.is_dir():
            path = selected
            filename = self._filename.value

        else:
            path = self._pathlist.value
            filename = selected.filename()

        self._set_form_values( \
                self._sourcelist.value, \
                path, \
                filename)

    def _on_dircontent_select(self, change: Mapping[str, str]) -> None:
        """Handle selecting a folder entry."""
        if self._sourcelist.value == SupportedSources.LOCAL:
            self._on_dircontent_select_local(change)
        elif SupportedSources.is_cloud(self._sourcelist.value):
            self._on_dircontent_select_cloud(change)

    def _on_filename_change_local(self, change: Mapping[str, str]) -> None:
        """Handle filename field changes for local storage."""
        self._set_form_values( \
                self._sourcelist.value, \
                self._expand_path(self._pathlist.value), \
                change['new'])

    def _on_filename_change_cloud(self, change: Mapping[str, str]) -> None:
        """Handle filename field changes for cloud."""
        self._set_form_values( \
                self._sourcelist.value, \
                self._pathlist.value, \
                change['new'])

    def _on_filename_change(self, change: Mapping[str, str]) -> None:
        """Handle filename field changes."""
        if self._sourcelist.value == SupportedSources.LOCAL:
            self._on_filename_change_local(change)
        elif SupportedSources.is_cloud(self._sourcelist.value):
            self._on_filename_change_cloud(change)

    def _process_selection(self) -> None:
        """ Handles actions on selection/read/read dbX meta.
        """
        # Execute callback function
        if self._callback is not None:
            try:
                self._callback(self)
            except TypeError:
                # Support previous behaviour of not passing self
                self._callback()

    def _on_select_click(self, _b) -> None:
        """Handle select button clicks."""
        if self._gb.layout.display == 'none':
            # If not shown, open the dialog
            self._show_dialog()
        else:
            # If shown, close the dialog and apply the selection
            self._apply_selection()
            self._process_selection()

    def _on_read_click(self, _b) -> None:
        """Handle read button clicks."""
        if self._gb.layout.display is None:
            self._apply_selection()
            if SupportedSources.is_cloud(self._sourcelist.value):
                files = {o.filename(): o for _,o in self._dircontent.options}
                self._data, self._data_error = read_cloud_data(
                        self.selected_filename, \
                        files, \
                        self._cloud, \
                        self._read_json.value, \
                        self._read_dbx_meta.value)
            else:
                fnames = [self._map_disp_to_name[dname] for dname in self._dircontent.options]
                files = {fname: fname for fname in fnames}
                self._data, self._data_error = read_local_data( \
                        self.selected_path, \
                        self.selected_filename, \
                        files, \
                        self._read_json.value, \
                        self._read_dbx_meta.value)
            # If shown, close the dialog and apply the selection
            self._process_selection()

    def _on_save_click(self, _b) -> None:
        """Handle save button clicks."""
        if self._gb.layout.display is None:
            self._apply_selection()
            if SupportedSources.is_cloud(self._sourcelist.value):
                warnings.warn("Function not implemented yet")
            else:
                self._data_error = save_local_data( \
                        self._data, \
                        self.selected_path, \
                        self.selected_filename, \
                        self._save_json.value, \
                        self._save_overwrite.value, \
                        self._save_dbx_meta.value)

            # If shown, close the dialog and apply the selection
            self._process_selection()

    def _show_dialog(self) -> None:
        """Show the dialog."""
        # Show dialog and cancel button
        self._gb.layout.display = None
        # widgets shouldn't appear/dissapear this way - just enable
        #self._cancel.layout.display = None

        # Show the form with the correct path and filename
        if ((self._selected_path is not None) and (self._selected_filename is not None)):
            path = self._selected_path
            filename = self._selected_filename
        else:
            path = self._default_path
            filename = self._default_filename

        self._set_form_values( \
                self._sourcelist.value, \
                path, \
                filename)

    def _close_dialog(self, select: bool=True) -> None:
        """Closes/deactivates the dialog."""
        self._gb.layout.display = 'none'
        # widgets shouldn't appear/dissapear this way - just enable
        #self._cancel.layout.display = 'none'
        if select:
            self._select.description = self._change_desc
        self._update_widgets_on_set( \
                is_valid_file=select, \
                is_file=bool(self._filename.value), \
                deactivate_dialog=True)

    def _apply_selection_cloud(self) -> None:
        """Close the dialog and apply the selection for cloud source."""
        self._selected_path = self._pathlist.value.get_cloud_path_with_bucket()
        self._selected_filename = self._filename.value

        if self._selected_path and self._selected_filename:
            self._close_dialog()
            selected = os.path.join(self._selected_path, self._selected_filename)
            self._label.value = self._LBL_TEMPLATE.format(self._restrict_path(selected), 'orange')

    def _apply_selection_local(self) -> None:
        """Close the dialog and apply the selection for local source."""
        self._selected_path = self._expand_path(self._pathlist.value)
        self._selected_filename = self._filename.value

        if ((self._selected_path is not None) and (self._selected_filename is not None)):
            selected = os.path.join(self._selected_path, self._selected_filename)
            self._close_dialog()

            if os.path.isfile(selected):
                self._label.value = self._LBL_TEMPLATE.format( \
                        self._restrict_path(selected), 'orange')
            else:
                self._label.value = self._LBL_TEMPLATE.format( \
                        self._restrict_path(selected), 'green')

    def _apply_selection(self) -> None:
        """Close the dialog and apply the selection."""
        if self._sourcelist.value == SupportedSources.LOCAL:
            self._apply_selection_local()
        elif SupportedSources.is_cloud(self._sourcelist.value):
            self._apply_selection_cloud()

    def _on_cancel_click(self, _b) -> None:
        """Handle cancel button clicks."""
        self._close_dialog(select=False)

    def _expand_path(self, path) -> str:
        """Calculate the full path using the sandbox path."""
        if isinstance(path, str):
            if self._sandbox_path:
                path = self._default_path if not path \
                        else os.path.join(self._sandbox_path, path.lstrip(os.sep))
        return path

    def _restrict_path(self, path) -> str:
        """Calculate the sandboxed path using the sandbox path."""
        if self._sandbox_path == os.sep:
            pass
        elif self._sandbox_path == path:
            path = os.sep
        elif self._sandbox_path:
            if os.path.splitdrive(self._sandbox_path)[0] and len(self._sandbox_path) == 3:
                # If the value is 'c:\\', strip 'c:' so we retain the leading os.sep char
                path = strip_parent_path(path, os.path.splitdrive(self._sandbox_path)[0])
            else:
                path = strip_parent_path(path, self._sandbox_path)
        return path

    def reset(self, path: Optional[str] = None, filename: Optional[str] = None) -> None:
        """Reset the form to the default path and filename."""
        # Check if path and sandbox_path align
        if path is not None and self._sandbox_path \
                and not self._has_parent_path(self._normalize_path(path), self._sandbox_path): # pylint: disable=line-too-long
            raise ParentPathError(path, self._sandbox_path)

        # Verify the filename is valid
        if filename is not None and not is_valid_filename(filename):
            raise InvalidFileNameError(filename)

        # Remove selection
        self._selected_path = None
        self._selected_filename = None

        # Hide dialog and cancel button
        self._gb.layout.display = 'none'
        # widgets shouldn't appear/dissapear this way - just enable
        # self._cancel.layout.display = 'none'

        # Reset select button and label
        self._select.description = self._select_desc
        self._select.disabled = False
        self._read.description = self._read_desc
        self._download.description = self._download_desc
        self._download.disabled = True
        self._label.value = self._LBL_TEMPLATE.format(self._LBL_NOFILE, 'black')
        self._read.disabled = path is None or filename is None
        self._save.disabled = path is None or filename is None

        if path is not None:
            self._default_path = self._normalize_path(path)

        if filename is not None:
            self._default_filename = filename

        # Clear widgets
        self._clear_form_values(clear_access_cred=True)

        self._set_form_values( \
                self._sourcelist.value, \
                self._default_path, \
                self._default_filename)

        # Use the defaults as the selected values
        if self._select_default:
            self._apply_selection()

    def refresh(self) -> None:
        """Re-render the form."""
        self._set_form_values( \
                self._sourcelist.value, \
                self._expand_path(self._pathlist.value), \
                self._filename.value)

    @property
    def show_hidden(self) -> bool:
        """Get _show_hidden value."""
        return self._show_hidden

    @show_hidden.setter
    def show_hidden(self, hidden: bool) -> None:
        """Set _show_hidden value."""
        self._show_hidden = hidden
        self.refresh()

    @property
    def dir_icon(self) -> Optional[str]:
        """Get dir icon value."""
        return self._dir_icon

    @dir_icon.setter
    def dir_icon(self, dir_icon: Optional[str]) -> None:
        """Set dir icon value."""
        self._dir_icon = dir_icon
        self.refresh()

    @property
    def dir_icon_append(self) -> bool:
        """Get dir icon value."""
        return self._dir_icon_append

    @dir_icon_append.setter
    def dir_icon_append(self, dir_icon_append: bool) -> None:
        """Prepend or append the dir icon."""
        self._dir_icon_append = dir_icon_append
        self.refresh()

    @property
    def rows(self) -> int:
        """Get current number of rows."""
        return self._dircontent.rows

    @rows.setter
    def rows(self, rows: int) -> None:
        """Set number of rows."""
        self._dircontent.rows = rows

    @property
    def title(self) -> str:
        """Get the title."""
        return self._title.value

    @title.setter
    def title(self, title: str) -> None:
        """Set the title."""
        self._title.value = title

        if title == '':
            self._title.layout.display = 'none'
        else:
            self._title.layout.display = None

    @property
    def default(self) -> str:
        """Get the default value."""
        return os.path.join(self._default_path, self._default_filename)

    @property
    def default_path(self) -> str:
        """Get the default_path value."""
        return self._default_path

    @default_path.setter
    def default_path(self, path: str) -> None:
        """Set the default_path."""
        # Check if path and sandbox_path align
        if self._sandbox_path and not self._has_parent_path(self._normalize_path(path), self._sandbox_path):
            raise ParentPathError(path, self._sandbox_path)

        normalized_path = self._normalize_path(path)
        if self._check_integrity(normalized_path):
            self._default_path = normalized_path
            self._set_form_values( \
                    self._sourcelist.value, \
                    self._default_path, \
                    self._filename.value)
        else:
            self._default_path = ''
            warnings.warn(f"Invalid default path:{normalized_path[:10]} does not match selected storage")


    @property
    def default_filename(self) -> str:
        """Get the default_filename value."""
        return self._default_filename

    @default_filename.setter
    def default_filename(self, filename: str) -> None:
        """Set the default_filename."""
        # Verify the filename is valid
        if not is_valid_filename(filename):
            raise InvalidFileNameError(filename)

        self._default_filename = filename
        self._set_form_values( \
                self._sourcelist.value, \
                self._expand_path(self._pathlist.value), \
                self._default_filename)

    @property
    def default_source(self) -> str:
        """Gets the default_source value."""
        return self._default_source

    @default_source.setter
    def default_source(self, source: Enum) -> None:
        """Sets the default_source."""
        # Verify the source is valid and supported.
        if not is_valid_source(source):
            raise InvalidSourceError(source)

        self._default_source = source
        self._process_source_change()

    @property
    def sandbox_path(self) -> Optional[str]:
        """Get the sandbox_path."""
        return self._sandbox_path

    @sandbox_path.setter
    def sandbox_path(self, sandbox_path: str) -> None:
        """Set the sandbox_path."""
        # Check if path and sandbox_path align
        if sandbox_path and not self._has_parent_path(self._default_path, self._normalize_path(sandbox_path)):
            raise ParentPathError(self._default_path, sandbox_path)

        self._sandbox_path = self._normalize_path(sandbox_path) if sandbox_path is not None else None

        # Reset the dialog
        self.reset()

    @property
    def show_only_dirs(self) -> bool:
        """Get show_only_dirs property value."""
        return self._show_only_dirs

    @show_only_dirs.setter
    def show_only_dirs(self, show_only_dirs: bool) -> None:
        """Set show_only_dirs property value."""
        self._show_only_dirs = show_only_dirs

        # Update widget layout
        self._filename.disabled = self._show_only_dirs
        self._filename.layout.display = (None, "none")[self._show_only_dirs]
        self._update_gridbox()

        # Reset the dialog
        self.reset()

    @property
    def disable_source(self) -> bool:
        """Gets disable_source property value."""
        return self._disable_source

    @disable_source.setter
    def disable_source(self, disable_source: bool) -> None:
        """Sets disable_source property value."""
        # Unobserve widget layout
        self._observe_sourcelist(enable=False)
        self._disable_source = disable_source
        self._sourcelist.disabled = self._disable_source

    @property
    def filter_pattern(self) -> Optional[Sequence[str]]:
        """Get file name filter pattern."""
        return self._filter_pattern

    @filter_pattern.setter
    def filter_pattern(self, filter_pattern: Optional[Sequence[str]]) -> None:
        """Set file name filter pattern."""
        self._filter_pattern = filter_pattern
        self.refresh()

    @property
    def value(self) -> Optional[str]:
        """Get selected value."""
        return self.selected

    @property
    def selected(self) -> Optional[str]:
        """Get selected value."""
        selected = None

        if ((self._selected_path is not None) and (self._selected_filename is not None)):
            selected = os.path.join(self._selected_path, self._selected_filename)

        return selected

    @property
    def selected_path(self) -> Optional[str]:
        """Get selected_path value."""
        return self._selected_path

    @property
    def selected_filename(self) -> Optional[str]:
        """Get the selected_filename."""
        return self._selected_filename

    def __repr__(self) -> str:
        """Build string representation."""
        properties = f"path='{self._default_path}'"
        properties += f", filename='{self._default_filename}'"
        properties += f", title='{self._title.value}'"
        properties += f", show_hidden={self._show_hidden}"
        properties += f", select_desc='{self._select_desc}'"
        properties += f", change_desc='{self._change_desc}'"
        properties += f", select_default={self._select_default}"
        properties += f", show_only_dirs={self._show_only_dirs}"
        properties += f", dir_icon_append={self._dir_icon_append}"

        if self._sandbox_path is not None:
            properties += f", sandbox_path='{self._sandbox_path}'"

        if self._dir_icon:
            properties += f", dir_icon='{self._dir_icon}'"

        if self._filter_pattern:
            if isinstance(self._filter_pattern, str):
                properties += f", filter_pattern='{self._filter_pattern}'"
            else:
                properties += f", filter_pattern={self._filter_pattern}"

        return f"{self.__class__.__name__}({properties})"

    def register_callback(self, callback: Callable[[Optional['FileChooser']], None]) -> None:
        """Register a callback function."""
        self._callback = callback

    def get_interact_value(self) -> Optional[str]:
        """Return the value which should be passed to interactive functions."""
        return self.selected

    @property
    def file_size_limit(self) -> int:
        """Property getter."""
        return self._file_size_limit

    @file_size_limit.setter
    def file_size_limit(self, limit: int) -> None:
        """Property setter."""
        self._file_size_limit = limit

    @property
    def data(self) -> Union[dict,bytes,None]:
        """Property getter."""
        return self._data

    @property
    def data_error(self) -> Union[dict,str,None]:
        """Property getter."""
        return self._data_error

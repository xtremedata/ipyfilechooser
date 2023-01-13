import os
import warnings
from enum import Enum
from typing import Optional, Sequence, Mapping, Callable
from ipywidgets import Widget, Dropdown, Text, Select, Button, HTML
from ipywidgets import Layout, GridBox, Box, HBox, VBox, ValueWidget

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
        has_parent_path
from .utils_sources import \
        SupportedSources, \
        is_valid_source, \
        req_access_cred, \
        build_access_cred_widget
from .utils_s3 import S3


class FileChooser(VBox, ValueWidget):
    """FileChooser class."""

    _LBL_TEMPLATE = '<span style="color:{1};">{0}</span>'
    _LBL_NOFILE = 'No selection'

    def __init__(
            self,
            path: str = os.getcwd(),
            filename: str = '',
            title: str = '',
            source: SupportedSources = SupportedSources.Local,
            select_desc: str = 'Select',
            change_desc: str = 'Change',
            show_hidden: bool = False,
            select_default: bool = False,
            dir_icon: Optional[str] = '\U0001F4C1 ',
            dir_icon_append: bool = False,
            show_only_dirs: bool = False,
            disable_source: bool = False,
            filter_pattern: Optional[Sequence[str]] = None,
            sandbox_path: Optional[str] = None,
            layout: Layout = Layout(width='500px'),
            **kwargs):
        """Initialize FileChooser object."""
        # Check if path and sandbox_path align
        if sandbox_path and not has_parent_path(normalize_path(path), normalize_path(sandbox_path)):
            raise ParentPathError(path, sandbox_path)

        # Verify the filename is valid
        if not is_valid_filename(filename):
            raise InvalidFileNameError(filename)

        # Verify the source is valid
        if not is_valid_source(source):
            raise InvalidSourceError(source)

        self._default_path = normalize_path(path)
        self._default_filename = filename
        self._default_source = source
        self._selected_path: Optional[str] = None
        self._selected_filename: Optional[str] = None
        self._show_hidden = show_hidden
        self._select_desc = select_desc
        self._change_desc = change_desc
        self._select_default = select_default
        self._dir_icon = dir_icon
        self._dir_icon_append = dir_icon_append
        self._show_only_dirs = show_only_dirs
        self._disable_source = disable_source
        self._filter_pattern = filter_pattern
        self._sandbox_path = normalize_path(sandbox_path) if sandbox_path is not None else None
        self._callback: Optional[Callable] = None

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
        self._access_cred = build_access_cred_widget(
            self._default_source,
            'access_cred'
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
        self._cancel = Button(
            description='Cancel',
            layout=Layout(
                min_width='6em',
                width='6em',
                display='none'
            )
        )
        self._select = Button(
            description=self._select_desc,
            layout=Layout(
                min_width='6em',
                width='6em'
            )
        )
        self._title = HTML(
            value=title
        )

        if title == '':
            self._title.layout.display = 'none'

        # Widgets' style settings
        self._sourcelist.style.description_width = 'auto'

        # Widget observe handlers
        if self._disable_source:
            self._sourcelist.unobserve(self._on_sourcelist_select, names='value')
        else:
            self._sourcelist.observe(self._on_sourcelist_select, names='value')
        self._pathlist.observe(self._on_pathlist_select, names='value')
        self._dircontent.observe(self._on_dircontent_select, names='value')
        self._filename.observe(self._on_filename_change, names='value')
        self._select.on_click(self._on_select_click)
        self._cancel.on_click(self._on_cancel_click)

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
                self._get_access_cred: lambda: req_access_cred(self._sourcelist.value),
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

        buttonbar = HBox(
            children=[
                self._select,
                self._cancel,
                Box([self._label], layout=Layout(overflow='auto'))
            ],
            layout=Layout(width='auto')
        )

        # Call setter to set initial form values
        self._set_form_values( \
                self._default_source, \
                self._default_path, \
                self._default_filename)

        # Use the defaults as the selected values
        if self._select_default:
            self._apply_selection()

        # Call VBox super class __init__
        super().__init__(
            children=[
                self._title,
                self._gb,
                buttonbar
            ],
            layout=layout,
            **kwargs
        )

    def _get_sourcelist(self) -> Widget:
        return self._sourcelist
    def _get_access_cred(self) -> Widget:
        return self._access_cred
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
        self._gb.disabled = True
        self._gb.layout.display = 'none'

        self._gb.children = [child_fun() \
                for child_fun,cond_fun in self._all_gb_children.items() \
                if cond_fun()]
        self._gb.layout.grid_template_areas = \
                "\n'sourcelist sourcelist'" \
                "\n{access_cred}'pathlist {filename}'" \
                "\n'dircontent dircontent'\n".format( \
                    access_cred=('', "'access_cred access_cred'\n")[req_access_cred(self._sourcelist.value)], \
                    filename=('filename', 'pathlist')[self._show_only_dirs])
        # restoring view
        self._gb.disabled = False
        self._gb.layout.display = None

    def _has_access_cred(self) -> bool:
        """ Returns True if proper access credentials are provided.
            Access Credentials widget has to be visible.
        """
        return self._access_cred.layout == None \
                and not any(c.value for c in self._access_cred.children)

    def _update_access_cred(self, enable: Optional[bool] = None) -> None:
        """Disables/hides access credentials widgets."""
        if enable is None:
            enable = req_access_cred(self._sourcelist.value)
        disable = not enable
        for child in self._access_cred.children:
            child.layout.display = (None, 'none')[disable]
            if enable:
                child.observe(self._on_access_cred_change, names='value')
            else:
                child.unobserve(self._on_access_cred_change, names='value')
            child.disabled = disable
        self._access_cred.layout.display = (None, 'none')[disable]
        self._access_cred.disabled = disable

    def _process_source_change(self) -> None:
        """Processes storage source change."""
        self._access_cred = build_access_cred_widget(
            self._sourcelist.value,
            'access_cred'
        )
        self._update_gridbox()
        self._clear_form_values()
        # Reset the dialog
        self.refresh()

    def _clear_form_values(self) -> None:
        """ Clears values for widgets presenting directories and files on source change.
        """
        self._pathlist.options = []
        self._filename.value = ''
        self._dircontent.options = []

    def _set_form_values_aws(self, path: str, filename: str) -> None:
        """Set the form values for the AWS storage."""
        # Process only with provided credentials
        if self._has_access_cred():
            try:
                s3 = S3()
                s3.key_name = self._access_cred.children[0].value
                s3.key_secret = self._access_cred.children[1].value
                response = s3.client.list_buckets()
                buckets = response['Buckets']
                self._dicontent.options = buckets
            except Exception as e:
                warnings.warn("Failed to process S3 request")

    def _set_form_values_local(self, path: str, filename: str) -> None:
        """Set the form values for the local storage."""
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
            self._map_name_to_disp = {
                real_name: disp_name
                for real_name, disp_name in zip(
                    dircontent_real_names,
                    dircontent_display_names
                )
            }

            # Dict to map display names to real names
            self._map_disp_to_name = {
                disp_name: real_name
                for real_name, disp_name in self._map_name_to_disp.items()
            }

            # Set _dircontent form value to display names
            self._dircontent.options = dircontent_display_names

            # If the value in the filename Text box equals a value in the
            # Select box and the entry is a file then select the entry.
            if ((filename in dircontent_real_names) and os.path.isfile(os.path.join(path, filename))):
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

                if (check1 and check2) or check3 or check4 or check5:
                    self._select.disabled = True
                else:
                    self._select.disabled = False
        except PermissionError:
            # Deselect the unreadable folder and generate a warning
            self._dircontent.value = None
            warnings.warn(f'Permission denied for {path}', RuntimeWarning)

    def _set_form_values(self, source: str, path: str, filename: str) -> None:
        """Set the form values."""
        # Check if the path falls inside the configured sandbox path
        if self._sandbox_path and not has_parent_path(path, self._sandbox_path):
            raise ParentPathError(path, self._sandbox_path)

        # Disable triggers to prevent selecting an entry in the Select
        # box from automatically triggering a new event.
        self._sourcelist.unobserve(self._on_sourcelist_select, names='value')
        self._pathlist.unobserve(self._on_pathlist_select, names='value')
        self._dircontent.unobserve(self._on_dircontent_select, names='value')
        self._filename.unobserve(self._on_filename_change, names='value')
        self._update_access_cred(enable=False)

        if self._sourcelist.value == SupportedSources.Local:
            self._set_form_values_local(path, filename)
        elif self._sourcelist.value == SupportedSources.AWS:
            self._set_form_values_aws(path, filename)
        else:
            warnings.warn(f"Storage source '{source.name:.10}' not implemented/uknown")

        # Reenable triggers
        self._sourcelist.observe(self._on_sourcelist_select, names='value')
        self._pathlist.observe(self._on_pathlist_select, names='value')
        self._dircontent.observe(self._on_dircontent_select, names='value')
        self._filename.observe(self._on_filename_change, names='value')
        self._update_access_cred()

    def _on_sourcelist_select(self, change: Mapping[Enum, Enum]) -> None:
        """Handles selecting a storage source."""
        self._process_source_change()

    def _on_access_cred_change(self, change: Mapping[Enum, Enum]) -> None:
        """Handles changing storage source access credentials."""
        pass

    def _on_pathlist_select(self, change: Mapping[str, str]) -> None:
        """Handle selecting a path entry."""
        self._set_form_values( \
                self._sourcelist.value, \
                self._expand_path(change['new']), \
                self._filename.value)

    def _on_dircontent_select(self, change: Mapping[str, str]) -> None:
        """Handle selecting a folder entry."""
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
                path, filename)

    def _on_filename_change(self, change: Mapping[str, str]) -> None:
        """Handle filename field changes."""
        self._set_form_values( \
                self._sourcelist.value, \
                self._expand_path(self._pathlist.value), change['new'])

    def _on_select_click(self, _b) -> None:
        """Handle select button clicks."""
        if self._gb.layout.display == 'none':
            # If not shown, open the dialog
            self._show_dialog()
        else:
            # If shown, close the dialog and apply the selection
            self._apply_selection()

            # Execute callback function
            if self._callback is not None:
                try:
                    self._callback(self)
                except TypeError:
                    # Support previous behaviour of not passing self
                    self._callback()

    def _show_dialog(self) -> None:
        """Show the dialog."""
        # Show dialog and cancel button
        self._gb.layout.display = None
        self._cancel.layout.display = None

        # Show the form with the correct path and filename
        if ((self._selected_path is not None) and (self._selected_filename is not None)):
            path = self._selected_path
            filename = self._selected_filename
        else:
            path = self._default_path
            filename = self._default_filename

        self._set_form_values( \
                self._sourcelist.value, \
                path, filename)

    def _apply_selection(self) -> None:
        """Close the dialog and apply the selection."""
        self._selected_path = self._expand_path(self._pathlist.value)
        self._selected_filename = self._filename.value

        if ((self._selected_path is not None) and (self._selected_filename is not None)):
            selected = os.path.join(self._selected_path, self._selected_filename)
            self._gb.layout.display = 'none'
            self._cancel.layout.display = 'none'
            self._select.description = self._change_desc
            self._select.disabled = False

            if os.path.isfile(selected):
                self._label.value = self._LBL_TEMPLATE.format(self._restrict_path(selected), 'orange')
            else:
                self._label.value = self._LBL_TEMPLATE.format(self._restrict_path(selected), 'green')

    def _on_cancel_click(self, _b) -> None:
        """Handle cancel button clicks."""
        self._gb.layout.display = 'none'
        self._cancel.layout.display = 'none'
        self._select.disabled = False

    def _expand_path(self, path) -> str:
        """Calculate the full path using the sandbox path."""
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
        if path is not None and self._sandbox_path and not has_parent_path(normalize_path(path), self._sandbox_path):
            raise ParentPathError(path, self._sandbox_path)

        # Verify the filename is valid
        if filename is not None and not is_valid_filename(filename):
            raise InvalidFileNameError(filename)

        # Remove selection
        self._selected_path = None
        self._selected_filename = None

        # Hide dialog and cancel button
        self._gb.layout.display = 'none'
        self._cancel.layout.display = 'none'

        # Reset select button and label
        self._select.description = self._select_desc
        self._select.disabled = False
        self._label.value = self._LBL_TEMPLATE.format(self._LBL_NOFILE, 'black')

        if path is not None:
            self._default_path = normalize_path(path)

        if filename is not None:
            self._default_filename = filename

        self._set_form_values( \
                self._sourcelist.value, \
                self._default_path, self._default_filename)

        # Use the defaults as the selected values
        if self._select_default:
            self._apply_selection()

    def refresh(self) -> None:
        """Re-render the form."""
        self._set_form_values( \
                self._sourcelist.value, \
                self._expand_path(self._pathlist.value), self._filename.value)

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
        if self._sandbox_path and not has_parent_path(normalize_path(path), self._sandbox_path):
            raise ParentPathError(path, self._sandbox_path)

        self._default_path = normalize_path(path)
        self._set_form_values( \
                self._sourcelist.value, \
                self._default_path, \
                self._filename.value)

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
        if sandbox_path and not has_parent_path(self._default_path, normalize_path(sandbox_path)):
            raise ParentPathError(self._default_path, sandbox_path)

        self._sandbox_path = normalize_path(sandbox_path) if sandbox_path is not None else None

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
        self._disable_source = disable_source

        # Update widget layout
        self._sourcelist.disabled = self._disable_source
        if self._disable_source:
            self._sourcelist.unobserve(self._on_sourcelist_select, names='value')
        else:
            self._sourcelist.observe(self._on_sourcelist_select, names='value')

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

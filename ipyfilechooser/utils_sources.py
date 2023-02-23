"""Helper functions for ipyfilechooser related to storage source.
"""

from enum import Enum, unique
from ipywidgets import Layout, VBox, HBox, Text, Password, Checkbox



def is_valid_source(source: Enum) -> bool:
    """Verifies if a source is valid and supported."""
    return isinstance(source, Enum) and source in SupportedSources



@unique
class SupportedSources(Enum):
    """Supported cloud storage."""

    LOCAL = 0
    AWS = 1
    AZURE = 2

    @classmethod
    def names(cls) -> [str]:
        """Returns known source names."""
        return [e.name for e in cls]

    @classmethod
    def elements(cls) -> [Enum]:
        """Returns known source enum as list."""
        return list(cls)

    @classmethod
    def is_cloud(cls, source: Enum) -> bool:
        """Returns True for cloud source."""
        return source in (cls.AWS, cls.AZURE)

    def __str__(self) -> str:
        return self.name

    def req_access_cred(self) -> bool:
        """Returns True if requested source requires access credentials."""
        return not self == SupportedSources.LOCAL



class AccCred:
    """ Represents/manages access credentials widgets.
    """

    @classmethod
    def _create_layout(cls, source: Enum, area_name: str) -> Layout:
        """Creates and returns a default layout for access credentials widgets."""
        return Layout(
                width='auto',
                grid_area=area_name,
                display=('none', None)[source.req_access_cred()]
            )

    @classmethod
    def _create_aws_widgets(cls, source: Enum) -> []:
        """ Creates widgets for AWS cloud.
        """
        return [
                Text(
                    description="AWS Access Key ID:",
                    value='',
                    disabled=False,
                    placeholder='provide AWS access key ID',
                    style={'description_width': 'auto'},
                    layout=cls._create_layout(source, 'object')
                ),
                Password(
                    description="AWS Access Key Secret:",
                    value='',
                    disabled=False,
                    placeholder='provide AWS access key secret',
                    style={'description_width': 'auto'},
                    layout=cls._create_layout(source, 'secret')
                ),
                Checkbox(
                    value=False,
                    disabled=False,
                    description='No Secret',
                    style={'description_width': 'auto'},
                    layout=cls._create_layout(source, 'no_passwd')
                )
        ]

    @classmethod
    def _create_azure_widgets(cls, source: Enum) -> []:
        """ Creates widgets for Azure cloud.
        """
        return [
                Text(
                    description="Azure Storage Account:",
                    value='',
                    disabled=False,
                    placeholder='provide Azure storage account name',
                    style={'description_width': 'auto'},
                    layout=cls._create_layout(source, 'object')
                ),
                Password(
                    description="Azure Storage Access Key:",
                    value='',
                    disabled=False,
                    placeholder='provide Azure storage account key',
                    style={'description_width': 'auto'},
                    layout=cls._create_layout(source, 'secret')
                ),
                Checkbox(
                    value=False,
                    disabled=False,
                    description='No Key',
                    style={'description_width': 'auto'},
                    layout=cls._create_layout(source, 'no_passwd')
                )
        ]

    @classmethod
    def _create_widgets(cls, source: Enum) -> []:
        """ Returns template for access credentials for the requested source."""
        if SupportedSources.is_cloud(source):
            return cls._create_aws_widgets(source) if source == SupportedSources.AWS \
                            else cls._create_azure_widgets(source)
        else:
            return tuple()

    @classmethod
    def create(cls, source: Enum, area_name: str) -> VBox:
        """Builds proper access credentials widget for requested storage source."""
        return cls(VBox(
            description=f'{source.name} Access Credentials',
            children=cls._create_widgets(source),
            disabled=False,
            layout=cls._create_layout(source, area_name)
        ))


    def __init__(self, acc_cred):
        """ Initializes instance.
        """
        self._acc_cred = acc_cred
        self._enabled = self.is_visible()
        self._observe = None


    def clear(self):
        """ Clears values/resets to default.
        """
        on_change = self.observe
        self.observe = None
        for child in self.children:
            child.value = '' if isinstance(child, Text) else False
        if on_change is not None:
            self.observe = on_change

    def is_valid(self) -> bool:
        return True

    def is_set(self) -> bool:
        return self.is_visible() \
                and self.has_children() \
                and bool(self.children[0].value) \
                and (bool(self.children[1].value) or self.children[2].value)

    def is_visible(self) -> bool:
        """ Returns true if active and enabled.
        """
        return self.has_layout() and self.layout.display is None

    def has_children(self) -> bool:
        """ Returns true if has children.
        """
        return bool(self.children)

    def has_layout(self) -> bool:
        """ Returns true if has layout.
        """
        return self.layout is not None

    @property
    def enabled(self):
        """ Property getter for 'enabled'.
        """
        return self._enabled

    @enabled.setter
    def enabled(self, enabled: bool):
        """ Property setter for 'enabled'.
        """
        self._enabled = enabled
        try:
            self.layout.display = ('none', None)[self._enabled]
        except (IndexError, AttributeError):
            pass

    @property
    def observe(self):
        """ Property getter for 'observe'.
        """
        return self._observe

    @observe.setter
    def observe(self, on_change):
        """ Property setter for 'observe'.
        """
        for child in self.children:
            if self._observe is not None:
                child.disabled = True
                try:
                    child.unobserve(self._observe, names='value')
                except (KeyError, ValueError):
                    pass
            if on_change is not None:
                child.observe(on_change, names='value')
                child.disabled = False
        self._observe = on_change

    @property
    def layout(self):
        """ Property getter for access credentials layout.
        """
        try:
            return self._acc_cred.layout
        except (IndexError, AttributeError):
            return None

    @property
    def children(self):
        """ Property getter for access credentials key/secret children.
        """
        try:
            return self._acc_cred.children
        except (IndexError, AttributeError):
            return tuple()

    @property
    def values(self):
        """ Property getter for access credentials key/secret children.
        """
        try:
            return [c.value for c in self.children]
        except (IndexError, AttributeError):
            return tuple()

    @property
    def widget(self):
        """ Property getter for access credentials key/secret children.
        """
        return self._acc_cred


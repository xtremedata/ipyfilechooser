"""Helper functions for ipyfilechooser related to storage source.
"""

from enum import Enum, unique
from ipywidgets import Layout, VBox, HBox, Text, Password


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


def is_valid_source(source: Enum) -> bool:
    """Verifies if a source is valid and supported."""
    return isinstance(source, Enum) and source in SupportedSources

def req_access_cred(source: Enum) -> bool:
    """Returns True if requested source requires access credentials."""
    return not source == SupportedSources.LOCAL

def get_access_cred_layout(source: Enum, area_name: str) -> Layout:
    """Creates and returns a default layout for access credentials widgets."""
    return Layout(
            width='auto',
            grid_area=area_name,
            display=('none', None)[req_access_cred(source)]
        )

def get_access_cred_widgets(source: Enum) -> []:
    """Returns template for access credentials for the requested source."""
    if source == SupportedSources.LOCAL:
        return ()
    if source == SupportedSources.AWS:
        return VBox(
                   children = [
                       HBox(
                           children = [
                               Text(
                               description="AWS Access Key ID:",
                               value='',
                               disabled=False,
                               placeholder='provide AWS access key ID',
                               style={'description_width': 'auto'},
                               layout=get_access_cred_layout(source, 'object')
                               ),
                           Password(
                               description="AWS Access Key Secret:",
                               value='',
                               disabled=False,
                               placeholder='provide AWS access key secret',
                               style={'description_width': 'auto'},
                               layout=get_access_cred_layout(source, 'secret')
                               )
                           ],
                           layout=get_access_cred_layout(source, 'container')
                       )
                   ]
               )
    if source == SupportedSources.AZURE:
        return [
                Text(
                    description="Azure Storage Account:",
                    value='',
                    disabled=False,
                    placeholder='provide Azure storage account name',
                    style={'description_width': 'auto'},
                    layout=get_access_cred_layout(source, 'object')
                ),
                Password(
                    description="Azure Storage Access Key:",
                    value='',
                    disabled=False,
                    placeholder='provide Azure storage account key',
                    style={'description_width': 'auto'},
                    layout=get_access_cred_layout(source, 'secret')
                )
        ]
    return ()

def build_access_cred_widget(source: Enum, area_name: str) -> VBox:
    """Builds proper access credentials widget for requested storage source."""
    return VBox(
        description=f'{source.name} Access Credentials',
        children=get_access_cred_widgets(source),
        disabled=False,
        layout=get_access_cred_layout(source, area_name)
    )

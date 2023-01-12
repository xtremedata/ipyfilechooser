"""Helper functions for ipyfilechooser related to storage source."""

from enum import Enum, unique
from ipywidgets import Layout, VBox, Text, Password


@unique
class SupportedSources(Enum):
    """Supported cloud storage."""

    Local = 0,
    AWS = 1,
    Azure = 2

    @classmethod
    def names(cls) -> [str]:
        return [e.name for e in cls]

    @classmethod
    def elements(cls) -> [Enum]:
        return [e for e in cls]

    def __str__(self) -> str:
        return self.name



def is_valid_source(source: Enum) -> bool:
    """Verifies if a source is valid and supported."""
    return isinstance(source, Enum) and source in SupportedSources

def req_access_cred(source: Enum) -> bool:
    """Returns True if requested source requires access credentials."""
    return False if source == SupportedSources.Local else True

def get_access_cred_widgets(source: Enum) -> {}:
    """Returns template for access credentials for the requested source."""
    if source == SupportedSources.Local:
        return ()
    if source == SupportedSources.AWS:
        return [
                Text(
                    description="AWS Access Key ID:",
                    value='',
                    placeholder='provide AWS access key ID',
                    style={'description_width': 'auto'}
                ),
                Password(
                    description="AWS Access Key Secret:",
                    value='',
                    placeholder='provide AWS access key secret',
                    style={'description_width': 'auto'}
                )
        ]
    if source == SupportedSources.Azure:
        return [
                Text(
                    description="Azure Storage Account:",
                    value='',
                    placeholder='provide Azure storage account name',
                    style={'description_width': 'auto'}
                ),
                Password(
                    description="Azure Storage Access Key:",
                    value='',
                    placeholder='provide Azure storage account key',
                    style={'description_width': 'auto'}
                )
        ]
    return ()

def build_access_cred_widget(source: Enum, area_name: str) -> VBox:
    """Builds proper access credentials widget for requested storage source."""
    return VBox(
        description=f'{source.name} Access Credentials',
        rows=2,
        children=get_access_cred_widgets(source),
        layout=Layout(
            width='auto',
            grid_area=area_name,
            display=(None, "none")[req_access_cred(source)]
        )
    )

"""Helper functions for ipyfilechooser related to storage source."""

from enum import Enum, unique


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



def is_valid_source(source: str) -> bool:
    """Verifies if a source is valid and supported."""
    return isinstance(source, Enum) and source in SupportedSources



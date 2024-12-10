from enum import IntEnum, auto


class LanguageType(IntEnum):
    """
    Enum of all supported languages.
    """
    JAVA = 1
    JAVASCRIPT = auto()
    TYPESCRIPT = auto()
    PYTHON = auto()
    C = auto()
    GO = auto()

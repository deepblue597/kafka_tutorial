from enum import Enum


class PropertyType(str, Enum):
    """
    Enum of all supported property types.
    """
    BOOL = 'bool'
    INT = 'int'
    FLOAT = 'float'
    DOUBLE = 'double'
    STRING = 'string'
    REGEX = 'regex'

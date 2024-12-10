from .name_converter import NamingConventionType
from .generator_naming_conventions import GeneratorNamingConventions


class LanguageConfigNamingConventions(GeneratorNamingConventions):
    """
    Encapsulates the naming conventions which are used by the LanguageConfigBase class.
    """
    file_naming_convention: NamingConventionType = None

from ..base.generator_naming_conventions import GeneratorNamingConventions

from .configuration_base import ConfigurationBase


class GeneratorConfiguration(ConfigurationBase):
    """
    Encapsulates the configuration properties used by the GeneratorBase class.
    """
    type_name: str
    """
    Name of the generated type. HINT: This acts more like a template than the
    real name as some conventions must be met and therefore the default convention
    specified by the deriving class will be used if no naming convention for the
    type name was provided (see _default_type_naming_convention).
    """
    naming_conventions: GeneratorNamingConventions = None
    """
    Specifies which case convention to use for the properties. If not provided,
    the name as specified will be used. Defaults to None
    """

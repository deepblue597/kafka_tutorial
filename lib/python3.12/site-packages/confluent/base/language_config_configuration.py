from typing import List, Type

from .configuration_base import _DEFAULT_INDENT, ConfigurationBase
from .language_type import LanguageType
from .language_config_naming_conventions import LanguageConfigNamingConventions
from .generator_base import GeneratorBase
from .distributor_base import DistributorBase
from .generator_configuration import GeneratorConfiguration


class NoConfigNameProvidedException(Exception):
    def __init__(self):
        super().__init__('No config name has been provided')


class LanguageConfigConfiguration(ConfigurationBase):
    """
    Encapsulates the configuration properties used by the LanguageConfigBase class.
    """
    config_name: str
    """
    Name of the generated type and config. HINT: This acts more like a template
    for the type name than the real name as some conventions must be met and
    therefore the default convention specified by the deriving class of
    GeneratorBase will be used if no naming convention for the type name
    was provided (see GeneratorBase._default_type_naming_convention).
    """
    language_type: LanguageType
    """
    Which language type is this config for.
    """
    file_extension: str
    """
    Which file extension to use for the output file.
    """
    generator_type: Type[GeneratorBase]
    """
    Which generator to use to generate the config.
    """
    transform: str
    """
    Function string to transform property values.
    """
    naming_conventions: LanguageConfigNamingConventions
    """
    Specifies which case convention to use for the properties. If not provided,
    the name as specified will be used.
    """

    distributors: List[DistributorBase]
    """
    Specifies which distributors to use for spreading the generated file.
    """

    def __init__(
        self,
        config_name: str,
        language_type: LanguageType,
        file_extension: str,
        generator_type: Type[GeneratorBase],
        indent: int = _DEFAULT_INDENT,
        transform: str = None,
        naming_conventions: LanguageConfigNamingConventions = None,
        distributors: List[DistributorBase] = None,
    ) -> None:
        super().__init__()

        self.config_name = config_name
        self.language_type = language_type
        self.file_extension = file_extension.lstrip('.')
        self.generator_type = generator_type
        self.indent = indent
        self.transform = transform
        self.naming_conventions = naming_conventions
        self.distributors = distributors

    def validate(self):
        """
        Validates the current configuration.

        :raises NoConfigNameProvidedException: Raised if no config name has been provided.
        """
        if not self.config_name:
            raise NoConfigNameProvidedException()
        
        # Make sure that the naming conventions are available.
        if not self.naming_conventions:
            self.naming_conventions = LanguageConfigNamingConventions()

    def get_generator_config(self) -> GeneratorConfiguration:
        """
        Creates the corresponding GeneratorConfig from the current LanguageConfigConfiguration.

        :return: GeneratorConfiguration based on the current LanguageConfigConfiguration.
        :rtype:  GeneratorConfiguration
        """
        generator_config = GeneratorConfiguration()

        generator_config.type_name = self.config_name
        generator_config.indent = self.indent
        generator_config.transform = self.transform
        generator_config.naming_conventions = self.naming_conventions

        return generator_config

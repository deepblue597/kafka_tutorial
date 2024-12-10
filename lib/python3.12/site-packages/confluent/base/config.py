from __future__ import annotations
import os
from typing import Dict, List, Tuple, Type

import yaml
from schema import Schema, Use, Optional, Or

from .config_language_mapping import ConfigLanguageMapping
from .name_converter import NamingConventionType
from .property import Property
from .property_type import PropertyType
from .language_type import LanguageType
from .language_config_base import LanguageConfigBase
from .language_config_naming_conventions import LanguageConfigNamingConventions
from .distributor_base import DistributorBase, DistributorCredential

from ..distributors.git_distributor import GitDistributor

# Main categories.
_KEY_INCLUDES = 'includes'
_KEY_LANGUAGES = 'languages'
_KEY_PROPERTIES = 'properties'

# Include keys.
_INCLUDE_KEY_PATH = 'path'
_INCLUDE_KEY_AS = 'as'

# Language keys.
_LANGUAGE_KEY_FILE_NAMING = 'file_naming'
_LANGUAGE_KEY_DISTRIBUTIONS = 'distributions'
_LANGUAGE_KEY_URL = 'url'
_LANGUAGE_KEY_PATH = 'path'
_LANGUAGE_KEY_PROPERTY_NAMING = 'property_naming'
_LANGUAGE_KEY_TYPE_NAMING = 'type_naming'
_LANGUAGE_KEY_INDENT = 'indent'
_LANGUAGE_KEY_TRANSFORM = 'transform'
_LANGUAGE_KEY_TYPE = 'type'
_LANGUAGE_KEY_NAME = 'name'
_LANGUAGE_KEY_USER = 'user'
_LANGUAGE_KEY_PASSWORD = 'password'
_LANGUAGE_KEY_AS = 'as'

# Property keys.
_PROPERTY_KEY_VALUE = 'value'
_PROPERTY_KEY_HIDDEN = 'hidden'
_PROPERTY_KEY_COMMENT = 'comment'

# Distribution types.
_DISTRIBUTION_TYPE_GIT = 'git'

# Load the glue.
_LANGUAGE_MAPPINGS = ConfigLanguageMapping.get_mappings()


class UnknownPropertyTypeException(Exception):
    def __init__(self, property_type: str):
        super().__init__(f'Unknown property type {property_type}')


class UnknownLanguageException(Exception):
    def __init__(self, language: str):
        super().__init__(f'Unknown language {language}')


class SeveralLanguagesException(Exception):
    def __init__(self, language: str):
        super().__init__(f'Several languages matched for {language}')


class NoLanguageConfigException(Exception):
    def __init__(self, language_type: LanguageType):
        super().__init__(f'No language config found for {language_type}')


class SeveralLanguageConfigsException(Exception):
    def __init__(self, language_type: LanguageType):
        super().__init__(f'Several languages configs found for {language_type}')


class AliasAlreadyInUseException(Exception):
    def __init__(self, alias: str):
        super().__init__(f'The include-alias \'{alias}\' is already in use')


class Config:
    """
    Handles the config evaluation by parsing the provided YAML string via the parse-method.
    """

    @staticmethod
    def read(path: str, distributor_credentials: List[DistributorCredential]) -> List[LanguageConfigBase]:
        """
        Reads the provided YAML configuration file and generates a list of language configurations.

        :param path: Path to load the YAML file from (see example/test-config.yaml for configuration details).
        :type path:  str

        :return: Language configurations which further can be dumped as config files.
        :rtype:  List[LanguageConfigBase]
        """
        return Config._read(path, distributor_credentials=distributor_credentials)[0]

    @staticmethod
    def parse(content: str, config_name: str, distributor_credentials: List[DistributorCredential]) \
        -> List[LanguageConfigBase]:
        """
        Parses the provided YAML configuration string and returns the corresponding language configurations.

        :param content:     YAML configuration strings. For config details, please check the test-config.yaml in
                            the example folder.
        :type content:      str
        :param config_name: Output config file name. NOTE: The actual file name format might be overruled by
                            the specified file_naming rule from the config.
        :type config_name:  str

        :return: Language configurations which further can be dumped as config files.
        :rtype:  List[LanguageConfigBase]
        """
        return Config._parse(content, config_name, distributor_credentials=distributor_credentials)[0]

    @staticmethod
    def _read(
        path: str,
        namespace: str='',
        namespaces: List[str]=None,
        distributor_credentials: List[DistributorCredential]=[],
    ) -> List[LanguageConfigBase]:
        """
        Reads the provided YAML configuration file and generates a list of language configurations.

        :param path:      Path to load the YAML file from (see example/test-config.yaml for configuration details).
        :type path:       str
        :param namespace: Specifies a namespace for the config. If None or empty, no namespace will be set.
        :type nammespace: str

        :return: Language configurations which further can be dumped as config files.
        :rtype:  List[LanguageConfigBase]
        """
        with open(path, 'r') as f:
            content = f.read()

        # Prepare config name.
        last_part = path.replace(r'\\', '/').split('/')[-1]

        if '.' in last_part:
            config_name = '.'.join(last_part.split('.')[0:-1])
        else:
            config_name = last_part
        return Config._parse(
            content,
            config_name,
            namespace,
            os.path.dirname(path),
            namespaces,
            distributor_credentials
        )

    @staticmethod
    def _parse(
        content: str,
        config_name: str,
        namespace: str='',
        directory: str='',
        namespaces: List[str]=None,
        distributor_credentials: List[DistributorCredential]=[],
    ) -> Tuple[List[LanguageConfigBase], List[Property]]:
        """
        Parses the provided YAML configuration string and returns the corresponding language configurations.

        :param content:     YAML configuration strings. For config details, please check the test-config.yaml in
                            the example folder.
        :type content:      str
        :param config_name: Output config file name. NOTE: The actual file name format might be overruled by
                            the specified file_naming rule from the config.
        :type config_name:  str
        :param namespace:   Specifies a namespace for the config. If None or empty, no namespace will be set.
        :type nammespace:   str

        :raises AliasAlreadyInUseException: Raised if an included config file uses an already defined alias.

        :return: Language configurations which further can be dumped as config files.
        :rtype:  List[LanguageConfigBase]
        """
        yaml_object = yaml.safe_load(content)
        validated_object = Config._schema().validate(yaml_object)
        language_configs: List[LanguageConfigBase] = []
        properties: List[Property] = []

        # Since a default list cannot be assigned to the namespaces variable in the method header, because it only
        # gets initialized once and then the list gets re-used (see https://stackoverflow.com/a/1145781), make sure
        # that namespaces gets set to a freshly created list if it hasn't already been until now.
        if not namespaces:
            namespaces = []

        # Evaluate included files and their properties.
        if _KEY_INCLUDES in validated_object:
            for inclusion in validated_object[_KEY_INCLUDES]:
                inclusion_namespace = inclusion[_INCLUDE_KEY_AS]

                # Make sure that a included config file does not re-define an alias.
                if inclusion_namespace in namespaces:
                    raise AliasAlreadyInUseException(inclusion_namespace)
                else:
                    namespaces.append(inclusion_namespace)
                inclusion_path = inclusion[_INCLUDE_KEY_PATH]

                # If the provided path is relative, incorporate the provided directory into the path.
                if not os.path.isabs(inclusion_path):
                    inclusion_path = os.path.join(directory, inclusion_path)

                # Read included config and put properties into property list.
                for inclusion_property in Config._read(inclusion_path, inclusion_namespace, namespaces)[1]:
                    inclusion_property.hidden = True  # Included properties are not being exported by default.
                    properties.append(inclusion_property)

        # Collect properties as they are the same for all languages.
        for property in validated_object[_KEY_PROPERTIES]:
            properties.append(Property(
                name=property[_LANGUAGE_KEY_NAME],
                value=property[_PROPERTY_KEY_VALUE],
                property_type=property[_LANGUAGE_KEY_TYPE],
                hidden=property[_PROPERTY_KEY_HIDDEN] if _PROPERTY_KEY_HIDDEN in property else None,
                comment=property[_PROPERTY_KEY_COMMENT] if _PROPERTY_KEY_COMMENT in property else None,
                namespace=namespace,
            ))

        # Evaluate each language setting one by one.
        if _KEY_LANGUAGES in validated_object:
            for language in validated_object[_KEY_LANGUAGES]:
                naming_conventions = LanguageConfigNamingConventions()
                language_type = language[_LANGUAGE_KEY_TYPE]
                indent = language[_LANGUAGE_KEY_INDENT] if _LANGUAGE_KEY_INDENT in language else None
                transform = language[_LANGUAGE_KEY_TRANSFORM] if _LANGUAGE_KEY_TRANSFORM in language else None

                # Evaluate file naming-convention.
                naming_conventions.file_naming_convention = Config._evaluate_naming_convention_type(
                    language[_LANGUAGE_KEY_FILE_NAMING] if _LANGUAGE_KEY_FILE_NAMING in language else None
                )

                # Evaluate properties naming-convention.
                naming_conventions.properties_naming_convention = Config._evaluate_naming_convention_type(
                    language[_LANGUAGE_KEY_PROPERTY_NAMING] if _LANGUAGE_KEY_PROPERTY_NAMING in language else None
                )

                # Evaluate type naming-convention.
                naming_conventions.type_naming_convention = Config._evaluate_naming_convention_type(
                    language[_LANGUAGE_KEY_TYPE_NAMING] if _LANGUAGE_KEY_TYPE_NAMING in language else None
                )
                config_type = Config._evaluate_config_type(language_type)

                language_configs.append(config_type(
                    config_name=config_name,
                    properties=properties,
                    indent=indent,
                    transform=transform,
                    naming_conventions=naming_conventions,
                    distributors=Config._evaluate_distributors(language, distributor_credentials),

                    # Pass all language props as additional_props to let the specific
                    # generator decide which props it requires additionally.
                    additional_props=language,
                ))

        return language_configs, properties
    
    @staticmethod
    def _schema() -> Schema:
        """
        Returns the config validation schema.

        :return: Config validation schema.
        :rtype:  Schema
        """
        return Schema({
            Optional(_KEY_INCLUDES): [{
                _INCLUDE_KEY_PATH: str,
                _INCLUDE_KEY_AS: str,
            }],
            Optional(_KEY_LANGUAGES): [{
                _LANGUAGE_KEY_TYPE: Use(Config._evaluate_language_type),
                Optional(_LANGUAGE_KEY_FILE_NAMING): str,
                Optional(_LANGUAGE_KEY_INDENT): int,
                Optional(_LANGUAGE_KEY_DISTRIBUTIONS): [{
                    _LANGUAGE_KEY_TYPE: str,
                    Optional(_LANGUAGE_KEY_AS): str,
                    Optional(object): object  # Collect other properties.
                }],
                Optional(object): object  # Collect other properties.
            }],
            _KEY_PROPERTIES: [{
                _LANGUAGE_KEY_TYPE: Use(Config._evaluate_data_type),
                _LANGUAGE_KEY_NAME: str,
                _PROPERTY_KEY_VALUE: Or(str, bool, int, float),
                Optional(_PROPERTY_KEY_HIDDEN): bool,
                Optional(_PROPERTY_KEY_COMMENT): str,
            }]
        })
    
    @staticmethod
    def _evaluate_data_type(type: str) -> PropertyType:
        """
        Evaluates a properties data type.

        :param type: Property type string (e.g., bool | string | ...).
        :type type:  str

        :raises UnknownPropertyTypeException: Raised if an unsupported property type was used in the config.

        :return: The corresponding PropertyType enum value.
        :rtype:  PropertyType
        """
        try:
            type = PropertyType(type)
        except ValueError:
            raise UnknownPropertyTypeException(type)
        return type

    @staticmethod
    def _evaluate_language_type(language: str) -> LanguageType:
        """
        Evaluates the requested language type.

        :param language: Language to generate a config for (e.g., java | typescript | ...).
        :type language:  str

        :raises UnknownLanguageException:  Raised if an unsupported language was used in the config.
        :raises SeveralLanguagesException: Raised if several mappings were found for the requested language. If this
                                           error arises, it's a package error. Please open an issue at
                                           https://github.com/monstermichl/confluent/issues.

        :return: The corresponding LanguageType enum value.
        :rtype:  LanguageType
        """
        found = [mapping.type for mapping in _LANGUAGE_MAPPINGS if mapping.name == language]
        length = len(found)

        if length == 0:
            raise UnknownLanguageException(language)
        elif length > 1:
            raise SeveralLanguagesException(language)
        return found[0]
    
    @staticmethod
    def _evaluate_config_type(language_type: LanguageType) -> Type[LanguageConfigBase]:
        """
        Evaluates the languages config type to use for further evaluation.

        :param language_type: Language type to search the corresponding language config for (e.g., LanguageType.JAVA).
        :type language_type:  LanguageType

        :raises NoLanguageConfigException:       Raised if no language config mapping was provided for the specified
                                                 language type. If this error arises, it's a package error. Please open
                                                 an issue at https://github.com/monstermichl/confluent/issues.
        :raises SeveralLanguageConfigsException: Raised if several language config mappings were found for the specified
                                                 language type. If this error arises, it's a package error. Please open
                                                 an issue at https://github.com/monstermichl/confluent/issues.

        :return: The corresponding LanguageConfigBase derivate type (e.g., Type[JavaConfig]).
        :rtype:  Type[LanguageConfigBase]
        """
        found = [mapping.config_type for mapping in _LANGUAGE_MAPPINGS if mapping.type == language_type]
        length = len(found)

        if length == 0:
            raise NoLanguageConfigException(language_type)
        elif length > 1:
            raise SeveralLanguageConfigsException('Several language configs found')
        return found[0]
    
    @staticmethod
    def _evaluate_distributors(
        language_config: Dict[str, any],
        distributor_credentials: List[DistributorCredential]=[]
    ) -> List[DistributorBase]:
        """
        Evaluates specified distributors of a language.

        :param language_config:         Language config object.
        :type language_config:          Dict[str, any]
        :param distributor_credentials: Potentially required credentials, defaults to []
        :type distributor_credentials:  List[DistributorCredential], optional

        :return: List of evaluated distributors for the given language.
        :rtype:  List[DistributorBase]
        """

        distributors = []
        credentials_map = {}

        # Map credential list to dictionary based on the credential alias for easer access.
        for distributor_credential in distributor_credentials:
            credentials_map[distributor_credential.distribution_alias] = distributor_credential

        # Get distributions config if provided.
        distributor_configs = language_config[_LANGUAGE_KEY_DISTRIBUTIONS] \
            if _LANGUAGE_KEY_DISTRIBUTIONS in language_config \
            else None
        
        # Check if distributions are provided and 
        if distributor_configs:
            distributor = None

            # Make sure distributor_configs is a list.
            if not isinstance(distributor_configs, list):
                distributor_configs = [distributor_configs]

            for config in distributor_configs:
                def from_config(key: str):
                    return config[key] if key in config else None
                type = from_config(_LANGUAGE_KEY_TYPE)

                # Build Git distributor.
                if type == _DISTRIBUTION_TYPE_GIT:
                    path = from_config(_LANGUAGE_KEY_PATH)
                    user = from_config(_LANGUAGE_KEY_USER)
                    password = from_config(_LANGUAGE_KEY_PASSWORD)
                    alias = from_config(_LANGUAGE_KEY_AS)

                    # If the alias is available in the credentials-map, use provided values.
                    if alias in credentials_map:
                        credential = credentials_map[alias]

                        # Overwrite YAML defined user and password value if provided.
                        if credential.user:
                            user = credential.user
                        if credential.password:
                            password = credential.password

                    # Build distributor.
                    distributor = GitDistributor(
                        from_config(_LANGUAGE_KEY_URL),
                        path,
                        user,
                        password,
                    )
                else:
                    pass  # No other types are supported yet.

                # If a distributor was created, add it to the distributors list.
                if distributor:
                    distributors.append(distributor)
        return distributors
    
    @staticmethod
    def _evaluate_naming_convention_type(naming_convention: str) -> NamingConventionType:
        """
        Evaluates which naming convention type to use for the output file.

        :param naming_convention: Naming convention string (e.g., snake | camel | ...).
        :type naming_convention:  str

        :return: The corresponding NamingConventionType enum value.
        :rtype:  NamingConventionType
        """
        if naming_convention == 'snake':
            naming_convention = NamingConventionType.SNAKE_CASE
        elif naming_convention == 'screaming_snake':
            naming_convention = NamingConventionType.SCREAMING_SNAKE_CASE
        elif naming_convention == 'camel':
            naming_convention = NamingConventionType.CAMEL_CASE
        elif naming_convention == 'pascal':
            naming_convention = NamingConventionType.PASCAL_CASE
        elif naming_convention == 'kebap':
            naming_convention = NamingConventionType.KEBAP_CASE
        return naming_convention

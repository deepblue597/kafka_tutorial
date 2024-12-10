from __future__ import annotations
from abc import ABC, abstractmethod
import copy
from typing import Callable, List

from .info import VERSION
from .configuration_base import _DEFAULT_INDENT
from .generator_configuration import GeneratorConfiguration
from .generator_naming_conventions import GeneratorNamingConventions
from .name_converter import NamingConventionType, NameConverter
from .property import Property


class PropertyAlreadyExistsException(Exception):
    def __init__(self, property: str):
        super().__init__(f'Property {property} already exists')


class NoTypeNameProvidedException(Exception):
    def __init__(self):
        super().__init__('No type name has been provided')


class GeneratorBase(ABC):
    """
    Abstract class that acts as the base for all Generator implementations.
    """

    def __init__(
        self,
        config: GeneratorConfiguration,
        properties: List[Property] = [],
        additional_props = {}
    ):
        """
        Constructor

        :param config:           Generator configuration.
        :type config:            GeneratorConfiguration
        :param properties:       List of properties to generator by the GeneratorBase derivate, defaults to []
        :type properties:        List[Property], optional
        :param additional_props: All props that might need to be used by the derivating class, defaults to {}
        :type additional_props:  dict, optional
        """
        type_name = config.type_name
        indent = config.indent

        self.transform = config.transform
        self._properties: List[Property] = []
        self._naming_conventions = \
            config.naming_conventions if config.naming_conventions else GeneratorNamingConventions()
        self._additional_props = additional_props

        self._set_type_name(type_name)
        self.set_indent(indent)

        # Add properties one by one.
        [self.add_property(property) for property in properties]

    def add_property(self, property: Property):
        """
        Adds a property to the properties list. IMPORTANT: Property names must be unique.

        :param property: Property to add.
        :type property:  Property

        :raises PropertyAlreadyExistsException: Raised if the instance already contains a property with the same name.

        :return: The current generator instance.
        :rtype:  Self
        """
        found_property = len([
            p for p in self._properties if p.name == property.name and p.namespace == property.namespace
        ]) > 0

        # Make sure that the name doesn't already exist.
        if found_property:
            raise PropertyAlreadyExistsException(property.name)

        self._properties.append(property)
        return self

    def set_indent(self, indent: int):
        """
        Sets the whitespace indent for the properties.

        :param indent: Indent value. If this value is less than 0, _DEFAULT_INDENT gets used.
        :type indent:  int

        :return: The current generator instance.
        :rtype:  Self
        """
        self._indent = indent if indent and indent >= 0 else _DEFAULT_INDENT
        return self

    def dump(self) -> str:
        """
        Generates a config file string.

        :return: Config file string.
        :rtype:  str
        """
        def add_newline(s):
            # Add trailing newline if required.
            if s[-1] != '\n':
                s += '\n'
            return s
        
        # Create copies of the properties to avoid messing around with the originals.
        properties_copy = [copy.deepcopy(property) for property in self._properties]

        # Transform properties if transform function was provided.
        self._apply_transformation(properties_copy)

        # Substitute property values.
        for property in properties_copy:
            Property.substitute(property, properties_copy)

        # Remove hidden properties.
        properties_copy = [property for property in properties_copy if not property.hidden]

        # Create the string for properties which shall be added before the class definition.
        properties_before_type = self._create_properties_string(self._property_before_type, properties_copy)

        # Create the string for properties which shall be added after the class definition.
        properties_after_type = self._create_properties_string(self._property_after_type, properties_copy)

        s = self._before_type()
        s += f'{properties_before_type}\n' if properties_before_type else ''
        s += f'{self._property_comment(f"Generated with confluent v{VERSION} (https://pypi.org/project/confluent/).").strip()}\n'
        s += f'{self._start_type(self._type_name)}\n'
        s += self._create_properties_string(self._create_property_in_type, properties_copy)

        class_end = self._end_type()
        s += f'\n{class_end}'
        s = add_newline(s)

        s += f'{properties_after_type}\n' if properties_after_type else ''
        s += self._after_type()
        s = add_newline(s)

        return s
    
    def get_type_name(self) -> str:
        """
        Returns the evaluated type name.

        :return: Evaluated type name.
        :rtype:  str
        """
        return self._type_name
    
    @abstractmethod
    def _default_type_naming_convention(self) -> NamingConventionType:
        """
        Abstract method which must be implemented by the deriving class to specify the default type naming convention.

        :return: Default naming convention.
        :rtype:  NamingConventionType
        """
        pass

    @abstractmethod
    def _before_type(self) -> str:
        """
        Abstract method which must be implemented by the deriving class to generate a possible string which will
        be added in front of the generated class/struct. If not required, this method shall return an empty string.

        :return: String to insert before the generated class/struct.
        :rtype:  str
        """
        pass
    
    @abstractmethod
    def _property_before_type(self, property: Property) -> str:
        """
        Abstract method which must be implemented by the deriving class to generate a single property string before the
        type definition starts. This might be useful in some cases to do some extra processing of the properties. If
        it's not required, an empty string shall be returned.

        :param property: Property to generate a property string from.
        :type property:  Property

        :return: A language specific property string which is added in front of the type definition (e.g.,
                 "const MY_BOOLEAN = true;").
        :rtype:  str
        """
        pass

    @abstractmethod
    def _start_type(self, type_name: str) -> str:
        """
        Abstract method which must be implemented by the deriving class to generate the class'/struct's definition.

        :return: The generated class/struct definition (e.g., "export class TestConfig {").
        :rtype:  str
        """
        pass

    @abstractmethod
    def _property_in_type(self, property: Property) -> str | List[str]:
        """
        Abstract method which must be implemented by the deriving class to generate a single property string.

        :param property: Property to generate a property string from.
        :type property:  Property

        :return: A language specific property string (e.g., "public static readonly myBoolean = true;") or list of
                 strings (if the property should span several lines).
        :rtype:  str | List[str]
        """
        pass

    @abstractmethod
    def _property_comment(self, comment: str) -> str:
        """
        Abstract method which must be implemented by the deriving class to generate a comment string.

        :param comment: Comment value.
        :type comment:  str

        :return: A language specific comment string (e.g., /* This is a comment. */).
        :rtype:  str
        """
        pass

    @abstractmethod
    def _end_type(self) -> str:
        """
        Abstract method which must be implemented by the deriving class to generate the class'/struct's body end.

        :return: The generated class'/struct's body end (e.g., "}").
        :rtype:  str
        """
        pass

    @abstractmethod
    def _property_after_type(self, property: Property) -> str:
        """
        Abstract method which must be implemented by the deriving class to generate a single property string after the
        type definition. This might be useful in some cases to do some extra processing of the properties. If it's not
        required, an empty string shall be returned.

        :param property: Property to generate a property string from.
        :type property:  Property

        :return: A language specific property string which is added in after the type definition.
        :rtype:  str
        """
        pass

    @abstractmethod
    def _after_type(self) -> str:
        """
        Abstract method which must be implemented by the deriving class to generate a possible string which will
        be added after the generated class/struct. If not required, this method shall return an empty string.

        :return: String to insert after the generated class/struct.
        :rtype:  str
        """
        pass

    def _set_type_name(self, name: str) -> GeneratorBase:
        """
        Sets the type name to the specified name. If no naming convention was set, the default
        naming convention, specified by the deriving class, will be used.

        :param name: Name of the generated type. HINT: This acts more like a template than the
                     real name as some conventions must be met and therefore the default convention
                     specified by the deriving class will be used if no naming convention for the
                     type name was provided (see _default_type_naming_convention).
        :type name:  str

        :raises NoTypeNameProvidedException: Raised if no name has been provided.

        :return: The current generator instance.
        :rtype:  Self
        """
        if not name:
            raise NoTypeNameProvidedException()
        naming_convention = self._naming_conventions.type_naming_convention

        self._type_name = NameConverter.convert(
            name,

            # Evaluate type naming convention. Use default if none was provided.
            naming_convention if naming_convention else self._default_type_naming_convention()
        )
        return self
    
    def _create_properties_string(self, callout: Callable[[Property], str], properties_copy: List[Property]) -> str:
        """
        Creates a string of all properties based on the provided callout.

        :param callout:         Callout to create a string based on the provided properties.
        :type callout:          Callable[[Property], str]
        :param properties_copy: Copy of all properties (to prevent modification of original).
        :type properties_copy:  List[Property]

        :return: Newline-separated properties string.
        :rtype:  str
        """
        return '\n'.join(
            # Loop in a loop. I know, it's a little bit confusing...
            property_string for property_string in [
                # This loop forms each property into a string.
                f'{callout(property)}' for property in properties_copy
            ] if property_string  # This clause makes sure that only property strings with a value are used.
        )

    def _create_property_in_type(self, property: Property) -> str:
        """
        Creates a property string from a property.

        :param property: Property to generate a property string from.
        :type property:  Property

        :return: A language specific property string including a possible comment (e.g.,
                 "public static readonly myBoolean = true;" /* This is a comment. */).
        :rtype:  str
        """
        INDENT = ' ' * self._indent  # Indent space.

        # If provided, use specific property naming convention.
        if self._naming_conventions.properties_naming_convention:
            property.name = NameConverter.convert(
                property.name, 
                self._naming_conventions.properties_naming_convention
            )
        property_in_type = self._property_in_type(property)
        comment = self._property_comment(property.comment) if property.comment else ''

        # If the property is delivered as list, add the comment before it and indent each line.
        if isinstance(property_in_type, list):#
            comment = comment.strip()

            s = f'{INDENT}{comment}\n' if comment else ''
            s += '\n'.join([f'{INDENT}{value}' for value in property_in_type])
        else:
            s = f'{INDENT}{property_in_type}{comment if comment else ""}'
        return s
    
    def _apply_transformation(self, properties_copy: List[Property]) -> None:
        """
        Applies the user defined value transformation to each property value.

        :param properties_copy: Copy of all properties (to prevent modification of original).
        :type properties_copy:  List[Property]
        """
        if self.transform:
            NAME_KEY = 'name'
            VALUE_KEY = 'value'
            TYPE_KEY = 'type'
            PROPERTIES_KEY = 'properties'

            for i, property in enumerate(properties_copy):
                # Create dictionary for local variables. This dictionary will also be used
                # to get the modified value afterwards (https://stackoverflow.com/a/67824076).
                local_variables = {
                    NAME_KEY: property.name,
                    VALUE_KEY: property.value,
                    TYPE_KEY: property.type.value,
                    PROPERTIES_KEY: properties_copy,
                }

                # Execute user defined Python script.
                exec(self.transform, None, local_variables)
                
                # Create new property from modified value.
                properties_copy[i] = Property(
                    name=property.name,
                    value=local_variables[VALUE_KEY],
                    property_type=property.type,
                    hidden=property.hidden,
                    comment=property.comment,
                    namespace=property.namespace,
                )

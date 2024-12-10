from __future__ import annotations
import copy
import random
import re
from typing import Callable, List
from .property_type import PropertyType


class UnknownSubstitutionException(Exception):
    def __init__(self, substitution_property: str):
        super().__init__(f'Unknown substitution property {substitution_property}')


class RecursiveSubstitutionException(Exception):
    def __init__(self, substitution_property: str):
        super().__init__(f'It\'s not allowed for a property to reference itself ({substitution_property})')


class InvalidVariableNameException(Exception):
    def __init__(self, variable_name: str):
        super().__init__(f'{variable_name} is not a valid variable name')


class InvalidNamespaceException(Exception):
    def __init__(self, namespace: str):
        super().__init__(f'{namespace} is not a valid namespace')


class Property:
    _PROPERTY_SUBSTITUTION_PATTERN = r'\${((_|[a-zA-Z])(\w|-)*((\.(_|[a-zA-Z])(\w|-)*))?)}'
    _NAMING_PATTERN = r'^(_|[a-zA-Z])(\w|-)*$'  # Define a general naming pattern.
    _PROPERTY_NAME_PATTERN = _NAMING_PATTERN
    _NAMESPACE_NAME_PATTERN = _NAMING_PATTERN

    """
    Holds all the required information to create a property string.

    :raises InvalidVariableNameException: Raised if an invalid variable name has been provided.
    """

    def __init__(
        self,
        name: str,
        value: str | bool | int | float,
        property_type: PropertyType,
        hidden: bool = False,
        comment: str = None,
        namespace: str = None,
    ):
        """
        Constructor

        :param name:          Property name.
        :type name:           str
        :param value:         Property value.
        :type value:          str | bool | int | float
        :param property_type: Property type.
        :type property_type:  PropertyType
        :param hidden:        If True, the property will not be put out to the generated file, defaults to False
        :type hidden:         bool, optional
        :param comment:       Property description, defaults to None
        :type comment:        str, optional
        :param namespace:     Namespace to give the property an additional information to where it belongs, defaults to
                              None to avoid collision with other properties with the same name (e.g., imported from
                              other files).
        :type namespace:      str, optional

        :raises InvalidVariableNameException: Raised if an invalid variable name has been provided.
        :raises InvalidNamespaceException:    Raised if an invalid namespace has been provided.
        """
        # Check if the key is a valid variable name.
        if not re.match(Property._PROPERTY_NAME_PATTERN, name):
            raise InvalidVariableNameException(name)
        
        # Check if the key is a valid variable name.
        if namespace and not re.match(Property._NAMESPACE_NAME_PATTERN, namespace):
            raise InvalidNamespaceException(namespace)
        
        # Make sure that the provided value is valid even if it's a string.
        value = Property._convert_value(value, property_type)
        
        self.name = name
        self.type = property_type
        self.value = value
        self.hidden = hidden
        self.comment = comment
        self.namespace = namespace

    @staticmethod
    def substitute(property: Property, properties: List[Property]) -> None:
        """
        Substitutes the referenced property references.

        :param property:   Property which's value shall be updated.
        :type property:    Property
        :param properties: List of properties to get the substitution values from.
        :type properties:  List[Property]

        :raises UnknownSubstitutionException:   Raised if the requested substitution property does not exist.
        :raises RecursiveSubstitutionException: Raised if a property referenced itself as substitution.
        """
        # Copy properties to avoid messing around with the originals.
        properties_copy = copy.deepcopy(properties)

        def replace(match, _: Property):

            def add_namespace(property_name, namespace):
                return f'{namespace}.{property_name}'

            # Incorporate namespace into property name.
            def prepared_property_name(propertyTemp):
                return add_namespace(propertyTemp.name, propertyTemp.namespace) \
                    if propertyTemp.namespace else propertyTemp.name

            substitution_property_value = match.group(1)  # E.g. myReplaceString or ti.myReplaceString.
            property_name = prepared_property_name(property)
            
            if property.namespace:
                substitution_property_value = add_namespace(substitution_property_value, property.namespace)
            
            # Substitute property only if it's not the same property as the one which is currently being processed.
            if substitution_property_value != property_name:
                found_properties = [
                    search_property.value for search_property in properties_copy if
                    prepared_property_name(search_property) == substitution_property_value
                ]

                if not found_properties:
                    raise UnknownSubstitutionException(substitution_property_value)
                replacement = found_properties[0]
            else:
                # TODO: Handle indirect self reference.
                raise RecursiveSubstitutionException(
                    f'Property {property_name} must not reference itself!'
                )
            return f'{replacement}'
        
        original_value = property.value
        Property._replace_property_value(property, replace)

        # Check if value changed due to substitution.
        if property.value != original_value:
            # IF type is some kind of number or boolean, evaluate the new string and assign the result to the
            # current property.
            if property.type in [PropertyType.INT, PropertyType.FLOAT, PropertyType.DOUBLE, PropertyType.BOOL]:
                property.value = Property._convert_value(eval(property.value), property.type)

    @staticmethod
    def _convert_value(value: any, property_type: PropertyType) -> any:
        if isinstance(value, str):
            if property_type == PropertyType.BOOL:
                # Correct boolean property value to 'true' or 'false'.
                value = False if value.lower() in ['0', 'false', 'no', 'off'] else True
            elif property_type == PropertyType.INT:
                # If numbers can be substituted validly and produce another number, keep it as string.
                if not Property._is_valid_number_substitution(value):
                    match = re.match(r'\d+', value)
                    value = int(match.group(0)) if match else 0  # Remove everything that comes after the integer.
            elif property_type == PropertyType.FLOAT or property_type == PropertyType.DOUBLE:
                # If numbers can be substituted validly and produce another number, keep it as string.
                if not Property._is_valid_number_substitution(value):
                    match = re.match(r'\d+(\.\d+)?', value)
                    value = float(match.group(0)) if match else 0  # Remove everything that comes after the float.
        return value

    @staticmethod
    def _replace_property_value(property: Property, callout: Callable[[re.Match, Property], str]) -> None:
        """
        Replaces the substitution strings of a property value by using the provided callout function.

        :param property: Property of which the value shall be be processed.
        :type property:  Property
        :param callout:  Callout function to which the RegEx match is passed. It must return a replacement string.
        :type callout:   Callable[[re.Match], str]
        """
        def callout_wrapper(match: re.Match) -> str:
            return callout(match, property)

        if isinstance(property.value, str):
            # Replace all occurrences.
            property.value = re.sub(Property._PROPERTY_SUBSTITUTION_PATTERN, callout_wrapper, property.value)

    @staticmethod
    def _is_valid_number_substitution(value: any) -> bool:
        """
        Checks if the provided value can be converted to a number value if all referenced substitution values get
        replaced by random integers.

        :param value: Value to check.
        :type  value: any

        :return: True if substituted value would result in a number.
        :rtype:  bool
        """
        valid = False

        if isinstance(value, str):
            pseudo_property = Property('p', value, PropertyType.STRING)

            # Substitute with pseudo data.
            def replace(*_):
                random_number = random.randint(1, 10)
                return f'{random_number}'
            Property._replace_property_value(pseudo_property, replace)

            try:
                eval(pseudo_property.value)
                valid = True
            except Exception:
                # Nothing to do here, value was invalid.
                pass
        return valid

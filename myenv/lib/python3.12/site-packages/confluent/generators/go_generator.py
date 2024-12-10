from typing import List

from ..helpers.package_handling import evaluate_package

from ..base.generator_configuration import GeneratorConfiguration
from ..base.name_converter import NamingConventionType
from ..base.generator_base import GeneratorBase
from ..base.property import Property
from ..base.property_type import PropertyType


class GoGenerator(GeneratorBase):
    """
    Go specific generator. For more information about the generator methods, refer to GeneratorBase.
    """

    def __init__(
        self,
        config: GeneratorConfiguration,
        properties: List[Property] = [],
        additional_props = {}
    ):
        super().__init__(config, properties, additional_props)

        # Evaluate the config's package name.
        self.package = evaluate_package(
            r'^[a-z][a-z0-9]+$',
            'See also https://go.dev/doc/effective_go#package-names',
            **self._additional_props,
        )

    def _default_type_naming_convention(self) -> NamingConventionType:
        return NamingConventionType.PASCAL_CASE
    
    def _before_type(self) -> str:
        return f'package {self.package}\n\n'

    def _property_before_type(self, _: Property) -> str:
        return ''
    
    def _start_type(self, _: str) -> str:
        return f'var {self._type_name} = struct {{'

    def _property_in_type(self, property: Property) -> str:
        type = property.type

        if type == PropertyType.BOOL:
            type = 'bool'
        elif type == PropertyType.INT:
            type = 'int'
        elif type == PropertyType.FLOAT or type == PropertyType.DOUBLE:
            type = 'float64'
        elif type == PropertyType.STRING or type == PropertyType.REGEX:
            type = 'string'
        else:
            raise Exception('Unknown type')
            
        REMAINING_SPACE_LENGTH = self._evaluate_longest_property() - len(property.name)
        return f'{property.name}{" " * REMAINING_SPACE_LENGTH} {type}'
    
    def _property_comment(self, comment: str) -> str:
        return f' // {comment}'
    
    def _end_type(self) -> str:
        return '}{'
    
    def _property_after_type(self, property: Property) -> str:
        type = property.type

        if type == PropertyType.BOOL:
            value = 'true' if property.value else 'false'
        elif type == PropertyType.INT:
            value = property.value
        elif type == PropertyType.FLOAT or type == PropertyType.DOUBLE:
            value = property.value
        elif type == PropertyType.STRING or type == PropertyType.REGEX:
            value = property.value.replace('\\', '\\\\')  # TODO: Might need to be refined.
            value = f'"{value}"'  # Wrap in quotes.
        else:
            raise Exception('Unknown type')
            
        REMAINING_SPACE_LENGTH = self._evaluate_longest_property() - len(property.name)
        return f'{" " * self._indent}{property.name}:{" " * REMAINING_SPACE_LENGTH} {value},'

    def _after_type(self) -> str:
        return '}'
    
    def _evaluate_longest_property(self) -> int:
        """
        Evaluates the length of the name of the property with the longest name.

        :return: Longest property name length.
        :rtype:  int
        """
        longest = 0

        for property in self._properties:
            length = len(property.name)

            if length > longest:
                longest = length
        return longest

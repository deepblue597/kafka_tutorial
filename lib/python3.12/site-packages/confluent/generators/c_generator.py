from ..base.name_converter import NameConverter, NamingConventionType
from ..base.generator_base import GeneratorBase
from ..base.property import Property
from ..base.property_type import PropertyType


class CGenerator(GeneratorBase):
    """
    C specific generator. For more information about the generator methods, refer to GeneratorBase.
    """

    def _default_type_naming_convention(self) -> NamingConventionType:
        return NamingConventionType.PASCAL_CASE
    
    def _before_type(self) -> str:
        return f'#ifndef {self._guard_name()}\n#define {self._guard_name()}\n\n'

    def _property_before_type(self, _: Property) -> str:
        return ''
    
    def _start_type(self, _: str) -> str:
        return 'const struct {'

    def _property_in_type(self, property: Property) -> str:
        type = property.type

        if type == PropertyType.BOOL:
            type = 'unsigned char'
        elif type == PropertyType.INT:
            type = 'int'
        elif type == PropertyType.FLOAT:
            type = 'float'
        elif type == PropertyType.DOUBLE:
            type = 'double'
        elif type == PropertyType.STRING or type == PropertyType.REGEX:
            type = 'char*'
        else:
            raise Exception('Unknown type')

        return f'{type} {property.name};'
    
    def _property_comment(self, comment: str) -> str:
        return f' /* {comment} */'
    
    def _end_type(self) -> str:
        return f'}} {self._type_name} = {{'
    
    def _property_after_type(self, property: Property) -> str:
        type = property.type

        if type == PropertyType.BOOL:
            value = '1' if property.value else '0'
        elif type == PropertyType.INT:
            value = property.value
        elif type == PropertyType.FLOAT:
            value = f'{property.value}f'
        elif type == PropertyType.DOUBLE:
            value = property.value
        elif type == PropertyType.STRING or type == PropertyType.REGEX:
            value = property.value.replace('\\', '\\\\')  # TODO: Might need to be refined.
            value = f'"{value}"'  # Wrap in quotes.
        else:
            raise Exception('Unknown type')

        return f'{" " * self._indent}{value},'
    
    def _after_type(self) -> str:
        return f'}};\n\n#endif{self._property_comment(self._guard_name())}'
    
    def _guard_name(self):
        return f'{NameConverter.convert(self._type_name, NamingConventionType.SCREAMING_SNAKE_CASE)}_H'

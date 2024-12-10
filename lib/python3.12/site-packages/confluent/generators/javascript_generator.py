from enum import Enum
from typing import List

from ..base.generator_configuration import GeneratorConfiguration
from ..base.name_converter import NamingConventionType
from ..base.generator_base import GeneratorBase
from ..base.property import Property
from ..base.property_type import PropertyType


class ExportType(str, Enum):
    ESM = 'esm'
    COMMON_JS = 'common_js'
    NONE = 'none'


class UnknownExportTypeException(Exception):
    def __init__(self, export_type: str):
        super().__init__(f'Unknown export type {export_type}')



class JavascriptGenerator(GeneratorBase):
    """
    JavaScript specific generator. For more information about the generator methods, refer to GeneratorBase.
    """
    _ATTRIBUTE_EXPORT = 'export'

    def __init__(
        self,
        config: GeneratorConfiguration,
        properties: List[Property] = [],
        additional_props = {}
    ):
        super().__init__(config, properties, additional_props)

        # Evaluate which export type to use.
        self.export_type = self._evaluate_export_type()

    def _default_type_naming_convention(self) -> NamingConventionType:
        return NamingConventionType.PASCAL_CASE
    
    def _before_type(self) -> str:
        return ''

    def _property_before_type(self, _: Property) -> str:
        return ''
    
    def _start_type(self, type_name: str) -> str:
        # Export class only directly if ESM is used.
        export = 'export ' if self.export_type == ExportType.ESM else ''

        return f'{export}class {type_name} {{'

    def _property_in_type(self, property: Property) -> str:
        type = property.type

        if type == PropertyType.BOOL:
            value = 'true' if property.value else 'false'
        elif type == PropertyType.INT or type == PropertyType.FLOAT or type ==  PropertyType.DOUBLE:
            value = property.value
        elif type == PropertyType.STRING:
            value = property.value.replace('\\', '\\\\')  # TODO: Might need to be refined.
            value = f'\'{value}\''  # Wrap in single quotes.
        elif type == PropertyType.REGEX:
            value = f'/{property.value}/'  # Wrap in single quotes.
        else:
            raise Exception('Unknown type')
            
        return self._create_property(property.name, value)
    
    def _property_comment(self, comment: str) -> str:
        return f' // {comment}'
    
    def _end_type(self) -> str:
        return '}'
    
    def _property_after_type(self, _: Property) -> str:
        return ''
    
    def _after_type(self) -> str:
        # Add module export only if CommonJS is used.
        return f'module.exports = {self._type_name}' if self.export_type == ExportType.COMMON_JS else ''
    
    def _evaluate_export_type(self) -> ExportType:
        export_type = ExportType.ESM  # Default to ESM.

        if self._ATTRIBUTE_EXPORT in self._additional_props:
            exception_type_string = self._additional_props[self._ATTRIBUTE_EXPORT]

            if exception_type_string == ExportType.ESM:
                export_type = ExportType.ESM
            elif exception_type_string == ExportType.COMMON_JS:
                export_type = ExportType.COMMON_JS
            elif exception_type_string == ExportType.NONE:
                export_type = ExportType.NONE
            else:
                raise UnknownExportTypeException(exception_type_string)
        return export_type

    def _create_property(self, name: str, value: str):
        # Realize JavaScript constant by defining a Getter.
        return f'static get {name}() {{ return {value}; }}'

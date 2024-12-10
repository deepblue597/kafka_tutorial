from .javascript_generator import ExportType, JavascriptGenerator


class TypescriptGenerator(JavascriptGenerator):
    """
    TypeScript specific generator. For more information about the generator methods, refer to GeneratorBase.
    """

    # Override JavaScriptGenerator method.
    def _start_type(self, type_name: str) -> str:
        # Export class only directly if ESM is used.
        export = 'export ' if self.export_type == ExportType.ESM else ''

        return f'{export}const {type_name} = {{'
    
    # Override JavaScriptGenerator method.
    def _end_type(self) -> str:
        return '} as const;'

    # Override JavaScriptGenerator method.
    def _create_property(self, name: str, value: str):
        return f'{name}: {value},'

from typing import Type

from ..generators.javascript_generator import JavascriptGenerator

from ..base.generator_base import GeneratorBase
from ..base.language_config_base import LanguageConfigBase
from ..base.language_type import LanguageType


class JavascriptConfig(LanguageConfigBase):
    """
    JavaScript specific config. For more information about the config methods, refer to LanguageConfigBase.
    """

    def _language_type(self) -> LanguageType:
        return LanguageType.JAVASCRIPT

    def _file_extension(self) -> str:
        return 'js'

    def _generator_type(self) -> Type[GeneratorBase]:
        return JavascriptGenerator

    def _allowed_file_name_pattern(self) -> str:
        return r'^(\.|\w)(\.|\w|-)+$'

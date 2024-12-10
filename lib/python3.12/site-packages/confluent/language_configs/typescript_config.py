from typing import Type

from ..generators.typescript_generator import TypescriptGenerator

from ..base.generator_base import GeneratorBase
from ..base.language_config_base import LanguageConfigBase
from ..base.language_type import LanguageType


class TypescriptConfig(LanguageConfigBase):
    """
    TypeScript specific config. For more information about the config methods, refer to LanguageConfigBase.
    """

    def _language_type(self) -> LanguageType:
        return LanguageType.TYPESCRIPT

    def _file_extension(self) -> str:
        return 'ts'

    def _generator_type(self) -> Type[GeneratorBase]:
        return TypescriptGenerator

    def _allowed_file_name_pattern(self) -> str:
        return r'^(\.|\w)(\.|\w|-)+$'

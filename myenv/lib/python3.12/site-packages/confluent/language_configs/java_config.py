from typing import Type
from ..generators.java_generator import JavaGenerator

from ..base.generator_base import GeneratorBase
from ..base.language_config_base import LanguageConfigBase
from ..base.language_type import LanguageType


class JavaConfig(LanguageConfigBase):
    """
    Java specific config. For more information about the config methods, refer to LanguageConfigBase.
    """

    def _language_type(self) -> LanguageType:
        return LanguageType.JAVA

    def _file_extension(self) -> str:
        return 'java'

    def _generator_type(self) -> Type[GeneratorBase]:
        return JavaGenerator

    def _allowed_file_name_pattern(self) -> str:
        return fr'^{self.generator.get_type_name()}$'

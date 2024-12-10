from typing import Type

from ..generators.c_generator import CGenerator

from ..base.generator_base import GeneratorBase
from ..base.language_config_base import LanguageConfigBase
from ..base.language_type import LanguageType


class CConfig(LanguageConfigBase):
    """
    C specific config. For more information about the config methods, refer to LanguageConfigBase.
    """

    def _language_type(self) -> LanguageType:
        return LanguageType.C

    def _file_extension(self) -> str:
        return 'h'

    def _generator_type(self) -> Type[GeneratorBase]:
        return CGenerator

    def _allowed_file_name_pattern(self) -> str:
        return r'^\w+$'

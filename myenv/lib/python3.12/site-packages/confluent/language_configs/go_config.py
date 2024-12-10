from typing import Type

from ..generators.go_generator import GoGenerator

from ..base.generator_base import GeneratorBase
from ..base.language_config_base import LanguageConfigBase
from ..base.language_type import LanguageType


class GoConfig(LanguageConfigBase):
    """
    Go specific config. For more information about the config methods, refer to LanguageConfigBase.
    """

    def _language_type(self) -> LanguageType:
        return LanguageType.GO

    def _file_extension(self) -> str:
        return 'go'

    def _generator_type(self) -> Type[GeneratorBase]:
        return GoGenerator

    def _allowed_file_name_pattern(self) -> str:
        return r'^(\.|\w)?\w+$'

from typing import Type

from ..generators.python_generator import PythonGenerator

from ..base.generator_base import GeneratorBase
from ..base.language_config_base import LanguageConfigBase
from ..base.language_type import LanguageType


class PythonConfig(LanguageConfigBase):
    """
    Python specific config. For more information about the config methods, refer to LanguageConfigBase.
    """

    def _language_type(self) -> LanguageType:
        return LanguageType.PYTHON

    def _file_extension(self) -> str:
        return 'py'

    def _generator_type(self) -> Type[GeneratorBase]:
        return PythonGenerator

    def _allowed_file_name_pattern(self) -> str:
        return r'^\w+$'

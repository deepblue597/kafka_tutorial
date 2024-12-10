from __future__ import annotations
from typing import List, Type

from ..language_configs.java_config import JavaConfig
from ..language_configs.javascript_config import JavascriptConfig
from ..language_configs.typescript_config import TypescriptConfig
from ..language_configs.python_config import PythonConfig
from ..language_configs.c_config import CConfig
from ..language_configs.go_config import GoConfig

from .language_config_base import LanguageConfigBase
from .language_type import LanguageType


class ConfigLanguageMapping:
    """
    Container to create a link between a language name, a language type and the appropriate
    language config class.
    """
    def __init__(self, name: str, type: LanguageType, config_type: Type[LanguageConfigBase]):
        """
        Constructor

        :param name:        Language name (e.g., java).
        :type name:         str
        :param type:        Language type (e.g., LanguageType.JAVA).
        :type type:         LanguageType
        :param config_type: Language config (derivate of the LanguageConfigBase class) (e.g., JavaConfig).
        :type config_type:  Type[LanguageConfigBase]
        """
        self.name = name
        self.type = type
        self.config_type = config_type

    @staticmethod
    def get_mappings() -> List[ConfigLanguageMapping]:
        """
        Returns a list of all valid language mappings. IMPORTANT: This is where all the mappings for
        supported languages go. If it's not included here, it's not being supported by the Config class.

        :return: List of supported languages.
        :rtype:  List[ConfigLanguageMapping]
        """
        return [
            ConfigLanguageMapping('java', LanguageType.JAVA, JavaConfig),
            ConfigLanguageMapping('javascript', LanguageType.JAVASCRIPT, JavascriptConfig),
            ConfigLanguageMapping('typescript', LanguageType.TYPESCRIPT, TypescriptConfig),
            ConfigLanguageMapping('python', LanguageType.PYTHON, PythonConfig),
            ConfigLanguageMapping('c', LanguageType.C, CConfig),
            ConfigLanguageMapping('go', LanguageType.GO, GoConfig),
        ]

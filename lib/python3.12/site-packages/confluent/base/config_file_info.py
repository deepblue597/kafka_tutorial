class ConfigFileInfo:
    """
    Container for basic config file information.
    """
    def __init__(self, file_name: str, file_extension: str):
        """
        Constructor

        :param file_name:      File name (e.g., test-config).
        :type file_name:       str
        :param file_extension: File extension (e.g., java).
        :type file_extension:  str
        """
        self.file_name = file_name
        self.file_extension = file_extension.lstrip('.')
        self.file_name_full = f'{self.file_name}.{self.file_extension}'

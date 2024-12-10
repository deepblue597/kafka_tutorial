import re


class NoPackageNameException(Exception):
    def __init__(self):
        super().__init__('No package name provided')


class EmptyPackageNameException(Exception):
    def __init__(self):
        super().__init__('Package name is empty')


class InvalidPackageNameException(Exception):
    def __init__(self, package_name: str, hint: str):
        message = f'Package name {package_name} is not a valid name'

        if hint:
            message = f'{message}. HINT: {hint}'
        super().__init__(message)


def evaluate_package(package_regex: str, hint: str, **props) -> str:
    """
    Checks if props contains a value for 'package' and if the package name conforms
    to the specified RegEx.

    :param package_regex: Package name RegEx to check.
    :type package_regex:  str
    :param hint:          Hint to throw if the package name doesn't conform.
    :type hint:           str

    :raises NoPackageNameException:      Raised if no package name was found.
    :raises EmptyPackageNameException:   Raised if the package name is empty.
    :raises InvalidPackageNameException: Raised if the package name does not conform to the RegEx.

    :return: Package name string.
    :rtype:  str
    """
    ATTRIBUTE_PACKAGE = 'package'

    if ATTRIBUTE_PACKAGE not in props:
        raise NoPackageNameException()
    else:
        package = props[ATTRIBUTE_PACKAGE]
    
    if not package:
        raise EmptyPackageNameException()
    elif not re.match(package_regex, package):
        raise InvalidPackageNameException(package, hint)
    return package

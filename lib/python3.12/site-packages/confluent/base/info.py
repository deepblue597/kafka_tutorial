from warnings import warn

VERSION = '0.3.2'

def deprecation_warning():
    warn(
        'confluent is deprecated. Please use ninja-bear instead. For more information refer to https://pypi.org/project/ninja-bear',
        DeprecationWarning, stacklevel=2
    )

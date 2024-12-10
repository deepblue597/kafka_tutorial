from __future__ import annotations
from abc import ABC, abstractmethod


class NoAliasProvidedException(Exception):
    def __init__(self):
        super().__init__('No alias has been provided')


class DistributorCredential:
    """
    Class to encapsulate credentials for specific distributor types.
    """

    distribution_alias: str
    user: str
    password: str

    def __init__(self, distribution_alias: str, user: str='', password: str=''):
        """
        DistributorBase constructor.

        :param distribution_alias: Alias to identify the credentials.
        :type distribution_alias:  str
        :param user:               Credential user, defaults to ''
        :type user:                str, optional
        :param password:           Credential password, defaults to ''
        :type password:            str, optional

        :raises NoAliasProvidedException: Raised if no distribution alias has been provided.
        """
        # Make sure there's an alias for the credentials.
        if not distribution_alias:
            raise NoAliasProvidedException()

        self.distribution_alias = distribution_alias
        self.user = user
        self.password = password


class DistributorBase(ABC):
    """
    Abstract class that acts as the base for all Distributor implementations. Distributors
    are used to distribute the generated configs to different locations (based on the actual
    distributor implementation).
    """

    @abstractmethod
    def distribute(file_name: str, data: str) -> DistributorBase:
        """
        Method to distribute a generated config which must be implemented by a derivative class.

        :param file_name: Config file name.
        :type file_name:  str
        :param data:      Config file data.
        :type data:       str
        """
        pass

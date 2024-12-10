import argparse
from os import path
from typing import List

from .base.orchestrator import Orchestrator
from .base.distributor_base import DistributorCredential
from .base.info import deprecation_warning

deprecation_warning()

_CONFIG_PARAMETER = 'config'
_OUTPUT_PARAMETER = 'output'
_SECRET_PARAMETER = 'secret'
_DISTRIBUTE_PARAMETER = 'distribute'


def _parse_credentials(credential_strings: List[str]) -> List[DistributorCredential]:
    credentials = []

    for credential_string in credential_strings if credential_strings else []:
        front_and_rear_parts = list(filter(lambda part: part, credential_string.split('=')))
        rear_parts = list(filter(
            lambda part: part,
            front_and_rear_parts[1].split(':') if len(front_and_rear_parts) == 2 else [],
        ))
        alias = front_and_rear_parts[0] if len(front_and_rear_parts) > 0 else None
        user = rear_parts[0] if len(rear_parts) > 1 else None
        password = rear_parts[1] if len(rear_parts) > 1 else (
            rear_parts[0] if len(rear_parts) == 1 else None
        )

        if alias and password:
            credentials.append(DistributorCredential(alias, user, password))
    return credentials


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('-c', f'--{_CONFIG_PARAMETER}', help='Path to configuration file', required=True, type=str)
    parser.add_argument('-o', f'--{_OUTPUT_PARAMETER}', help='Output location', required=False, type=str, default='.')
    parser.add_argument('-s', f'--{_SECRET_PARAMETER}',
        help='Credential for distributions in the form of <alias>=[<username>:]<password>',
        required=False, action='append')
    parser.add_argument('-d', f'--{_DISTRIBUTE_PARAMETER}',
        help='Distribute the generated configs to the specified locations', required=False, action='store_true')

    args = parser.parse_args()

    # TODO: Might also strip backslashes.
    output_dir = f'{str(getattr(args, _OUTPUT_PARAMETER)).strip("/")}/' if hasattr(args, _OUTPUT_PARAMETER) else ''

    if output_dir and not path.isdir(output_dir):
        raise Exception(f'Output directory {output_dir} does not exist')
    
    credentials = _parse_credentials(getattr(args, _SECRET_PARAMETER) if hasattr(args, _SECRET_PARAMETER) else [])
    config = Orchestrator.read_config(getattr(args, _CONFIG_PARAMETER), credentials)
    config.write(output_dir)

    if getattr(args, _DISTRIBUTE_PARAMETER):
        config.distribute()


if __name__ == '__main__':
    main()

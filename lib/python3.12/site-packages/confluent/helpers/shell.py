import subprocess
from typing import Tuple


def execute_commands(*commands: str) -> Tuple[int, str, str]:
    commands = filter(lambda command: command, commands)  # Filter empty commands.
    chain = ' && '.join(commands)

    # https://www.squash.io/how-to-execute-a-program-or-system-command-in-python/.
    result = subprocess.run(chain, capture_output=True, text=True, shell=True)
    return result.returncode, result.stdout, result.stderr

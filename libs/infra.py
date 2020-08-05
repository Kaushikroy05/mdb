
import logging
import subprocess
import shlex

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DEFAULT_TIMEOUT = 300

def print_banner(given_string):
    banner_str = "\n\n" + '=' * 80 + "\n"
    banner_str += "|| {} ||\n".format(given_string.center(74))
    banner_str += '=' * 80 + "\n\n"
    print(banner_str)

def execute_cmd(cmd, timeout=DEFAULT_TIMEOUT):
    """
    Execute cmd on local system and return stdout, stderr and exit status
    """
    with subprocess.Popen(
        #shlex.split(cmd),
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ) as proc:

        proc.wait(timeout=timeout)
        exit_status = proc.returncode

        if proc.stdout:
            stdout_full = proc.stdout.read().decode()
        else:
            stdout_full = ''

        if proc.stderr:
            stderr_full = proc.stderr.read().decode()
        else:
            stderr_full = ''

    if exit_status != 0:
        msg = "Command {} failed!\nOutput: {}\nError: {}\n Exit Status {}\n"
        msg = msg.format(cmd, stdout_full, stderr_full, exit_status)

        logger.error(msg)
        raise Exception("command {} failed!".format(cmd))

    return stdout_full, stderr_full, exit_status

#!/usr/local/bin/python3
import infra
import paramiko
from scp import SCPClient
import baseexception as be
import logging

DEF_CONNECT_TIMEOUT = 60
DEF_EXEC_TIMEOUT = 120

log = logging.getLogger(__name__)

class Connection(object):
    """
    connection class
    """

    def __init__(self, hostname, username='root', password=''):
        self._hostname = hostname
        self._username = username
        self._password = password
        self.connect()

    @property
    def hostname(self):
        return self._hostname

    @property
    def username(self):
        return self._username

    @property
    def password(self):
        return self._password

    def connect(self):
        """
        connect to the target host
        """
        self._ssh_paramiko = paramiko.SSHClient()
        self._ssh_paramiko.set_missing_host_key_policy(
            paramiko.AutoAddPolicy()
        )

        try:
            self._ssh_paramiko.connect(
                self._hostname,
                username=self._username,
                password=self._password,
                timeout=DEF_CONNECT_TIMEOUT
            )

            self._scp = SCPClient(self._ssh_paramiko.get_transport())
        except Exception as e:
            raise Exception(
                "Exception ({}) while connecting to host: {}".format(
                    str(e), self._hostname
                )
            )

        log.info(
            "Connection successful to host {}".format(self._hostname)
        )

    def reconnect(self):
        """
        reconnect to the target host
        """
        self._ssh_paramiko.close()
        self.connect()

    def close(self):
        """
        close connection
        """
        self._ssh_paramiko.close()

    def execute(self, cmd, *args, raise_on_error=True,
                timeout=DEF_EXEC_TIMEOUT, **kwargs):
        """
        :param cmd: command to execute
        :param args: extra args to commands
        :param raise_on_error: raise if cmd exists with non-zero exit status
        :param timeout: default cmd timeout
        :param kwargs: extra (key=value) arguments to cmd
        :return: tuple of (stdout, stderr, exit_code)
        """
        if args:
            cmd += '{} {}'.format(' ', ' '.join(args))
        if kwargs:
            for k, v in kwargs.items():
                cmd += ' {}={}'.format(k, v)

        try:
            stdin, stdout, stderr = self._ssh_paramiko. \
                exec_command(cmd, timeout=timeout)

            stdout_full = stdout.read().decode('utf-8').rstrip()
            stderr_full = stderr.read().decode('utf-8').rstrip()
            exit_status = stdout.channel.recv_exit_status()

            log_msg = ("Executing cmd({}) on {} "
                       "\nstdout: {}\nstderr: {}\nexit_code: {}")
            log_msg = log_msg.format(
                cmd, self._hostname, stdout_full, stderr_full, exit_status
            )
            log.info(log_msg)

            if exit_status != 0:
                if raise_on_error:
                    log.error(
                        "Failure ({}) while executing cmd ({})".format(
                            stderr_full, cmd
                        )
                    )
                    raise be.CommandError("Command({}) failed".format(cmd))
                else:
                    log.warning(
                        "Failure ({}) while executing cmd ({})".format(
                            stderr_full, cmd
                        )
                    )
        except Exception as e:
            raise Exception(
                "Exception({}) while executing cmd({})".format(
                    str(e), cmd
                )
            )

        return (stdout_full, stderr_full, exit_status)


    def copy(self, local_file, remote_file, from_remote=True,
             raise_on_error=True,
             timeout=DEF_CONNECT_TIMEOUT):
        """

        :param local_file: path of local file
        :param remote_file: path of remote file
        :param from_remote: default - copy from remote to local
        :param timeout: timeout for copy
        :param raise_on_error: raise exception if copy fails
        :return: None
        """
        try:
            if from_remote:
                log.info(
                    "copy from remote({}) to local({})".format(
                        remote_file, local_file
                    )
                )
                self._scp.get(
                    remote_file, local_file, recursive=True,
                    preserve_rimes=True
                )
            else:
                log.info(
                    "copy to remote({}) from local({})".format(
                        remote_file, local_file
                    )
                )
                self._scp.put(
                    local_file, remote_file, recursive=True,
                    preserve_times=True
                )
        except Exception as e:
            self.reconnect()
            if raise_on_error:
                raise Exception("Exception during scp ({})".format(str(e)))
            else:
                log.warning(
                    "Exception during scp ({})".format(str(e))
                )

from connection import Connection
from .sysbench_config import SysbenchConfig
from multiprocessing import Pool

import logging

log = logging.getLogger(__name__)

DEFAULT_MAX_WAIT = 7200

def execute_on_target(
        hostname,
        cmd,
        username='kaushik.roy',
        password='dummy'
    ):
        log.info("[{}] Exec cmd: {}".format(hostname, cmd))
        conn_obj = Connection(
            hostname, username, password
        )
        inp, out, ret = conn_obj.execute(cmd)
        conn_obj.close()
        return inp, out, ret


class Sysbench(object):

    def __init__(
        self,
        yaml_config,
        job_name=None,
    ):
        self._sysbench_config = SysbenchConfig(yaml_config, job_name)
        self._pool = Pool()


    @property
    def sysbench_cli_commands(self):
        """
        get commands to run in dictionary format
        {
            'client1': [list of commands to run from client1].
            'client2': [list of commands to run from client2],
            ...
        }
        """
        return self._sysbench_config.get_cli_commands()

    def _get_command_list(self):
        """
        returns list of commands in format -
        [('host1', 'cmd1'), ('host1', 'cmd2'), ('host2', 'cmd3') ... ]
        """
        command_dict = self.sysbench_cli_commands

        all_cmd_list = list()
        for host, cmd_list in self.sysbench_cli_commands.items():
            all_cmd_list.extend([(host, cmd) for cmd in cmd_list])

        return all_cmd_list

    def start(self, a_sync=False):
        """
        start IO
        """

        self._async_return = self._pool.starmap_async(
            execute_on_target, self._get_command_list()
        )

        if not a_sync:
            self.wait_for_io_complete()
        else:
            log.info('async IO requested. Returning immediately')


    def wait_for_io_complete(self, max_wait=DEFAULT_MAX_WAIT):
        """
        """
        self._async_return.wait(timeout=max_wait)

        try:
            self._results = self._async_return.get(timeout=10)

            if -1 in self._results:
                raise Exception("IO Failed")

            log.info(self._results)

        except TimeoutError:
            log.error("IO Timeout")

        except:
            self.stop()
            raise

    def stop(self):
        try:
            self._pool.terminate()
        except:
            pass

        #######################################################
        #TODO - force kill sysbench on remote host if required
        #######################################################

from cachedproperty import cached_property
from yamlconfig import read_yaml
from .sysbench_exception import SBMultipleJobs
import logging

log = logging.getLogger(__name__)

class SysbenchConfigValidator(object):
    """
    TODO:
    validate sysbench yaml config file and throw exception if incorrect
    """
    pass


class SysbenchConfig(object):

    def __init__(
        self,
        yaml_config,    # path to yaml config file
        job_list=[],  # list of jobs to run (None - runs all jobs)
    ):
        self._yaml_config = yaml_config
        self._job_list = job_list
        self._sysbench_cfg = read_yaml(self._yaml_config)
        ###################################################################
        # TODO - validate self._sysbench_cfg with SysbenchConfigValidator
        ##################################################################

    @property
    def sysbench_config(self):
        return self._sysbench_cfg

    @property
    def job_list(self):
        """
        returns job name to run
        """
        if not self._job_list:
            all_jobs = self._get_job_config()
            self._job_list = [ job for job in all_jobs.keys() ]

        return self._job_list

    def _get_sysbench_config(self, config_name=None):
        """
        return sysbench config dictionary for 'config_name'
        else return all sysbench configs in yaml file
        """
        config_dict = self.sysbench_config['sysbench']
        if config_name:
            return config_dict[config_name]
        else:
            return config_dict

    def _get_job_config(self, job_name=None):
        """
        returns job config dictionary got 'job_name'
        else return all job configs in yaml file
        """
        job_list = self.sysbench_config['job']
        if job_name:
            log.info("Job Config Return {}".format(job_list[job_name]))
            return job_list[job_name]
        else:
            log.info("Job Config Return {}".format(job_list))
            return job_list

    def _generate_sysbenchcli_command(self, config_dict):
        """
        generate sysbench cli command from self.sysbench_config
        Keys : options, testname, commands (list)
        """
        cli_cmd = "/usr/local/bin/sysbench "

        # add options
        for option, value in config_dict.get('options', {}).items():
            cli_cmd += ' --{}={}'.format(option, value)

        # add testname
        cli_cmd += ' --test={}'.format(config_dict['testname'])

        ret_cmd = ""
        # add command. For each command create a separate entry if
        # multiple commands are provided
        if type(config_dict['commands']) is list:
            for cmd in config_dict['commands']:
                ret_cmd += "{} {};".format(cli_cmd, cmd)
        else:
            ret_cmd = '{} {}'.format(cli_cmd, config_dict['commands'])

        log_msg = "generated Sysbench cmd ( {} ) from config_dict ( {} )"
        log.info( log_msg.format(ret_cmd, config_dict))
        return ret_cmd


    def _get_sysbench_for_joblist(self):
        """
        return dictionary of commands to be run on each client
        """
        job_dict = dict()
        for job in self.job_list:
            # get job dictionary
            tmp_job_cfg = self._get_job_config(job)

            # get sysbench config name and its dictionary
            config_dict = self._get_sysbench_config(tmp_job_cfg['config'])

            # get sysbench_cli to be run on each client
            sysbench_cli = self._generate_sysbenchcli_command(config_dict)

            # generate requested number of sysbench command for each client
            for client, instances in tmp_job_cfg['clients'].items():
                add_sysbench_cmds = [sysbench_cli] * instances
                if client in job_dict:
                    job_dict[client].extend(add_sysbench_cmds)
                else:
                    job_dict[client] = add_sysbench_cmds

        return job_dict

    @cached_property
    def get_cli_commands(self):
        """
        get cli commands to be executed
        """
        return self._get_sysbench_for_joblist()

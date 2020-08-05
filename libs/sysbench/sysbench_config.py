import sys
sys.path.append("..")

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
        yaml_config,
        job_name=None,
    ):
        self._yaml_config = yaml_config
        self._job_name = job_name
        self._cli_command_dict = None
        self._sysbench_cfg = read_yaml(self._yaml_config)
        ###################################################################
        # TODO - validate self._sysbench_cfg with SysbenchConfigValidator
        ##################################################################

    @property
    def sysbench_config(self):
        return self._sysbench_cfg

    @property
    def job_name(self):
        """
        returns job name to run.
        If job_name is not passed while creating SysbenchConfig object
        then choose the only job defined in the yaml file. Else fail
        """
        if not self._job_name:
            all_jobs = self._get_job_config()
            if len(all_jobs) != 1:
                raise SBMultipleJobs(
                    "Only 1 job definition expected in config file {}".format(self._yaml_config)
                )
            self._job_name = all_jobs.popitem()[0]

        return self._job_name

    def _get_sysbench_config(self, config_name=None):
        config_dict = self.sysbench_config['sysbench']
        if config_name:
            return config_dict[config_name]
        else:
            return config_dict



    def _get_job_config(self, job_name=None):
        job_dict = self.sysbench_config['job']
        if job_name:
            log.info("Job Config Return {}".format(job_dict[job_name]))
            return job_dict[job_name]
        else:
            log.info("Job Config Return {}".format(job_dict))
            return job_dict

    def _get_job_details(self):

        job_dict = dict()
        if self._job_name:
            tmp_workload_cfg = self._get_job_config(self._job_name)
            config_name = tmp_workload_cfg['config']

            client_dict = tmp_workload_cfg['clients']
            """ contains client name and no of instances to be run"""
            config_dict = self._get_sysbench_config(config_name)
            """ Contains options,testname and commands"""
            sysbench_cli = self._generate_cli_command(config_dict)
            import pdb;pdb.set_trace()

            log.info("Generate CLi data {}".format(sysbench_cli))
            for client in tmp_workload_cfg['clients']:
                if client in job_dict:
                    job_dict[client].extend(sysbench_cli)
                else:
                    job_dict[client] = sysbench_cli
                job_dict[client] = job_dict[client] * client_dict[client]
            return job_dict
        else :
            for job in self._get_job_config():
                tmp_workload_cfg = self._get_job_config(job)
                import pdb;pdb.set_trace()
                config_name = tmp_workload_cfg['config']

                client_dict = tmp_workload_cfg['clients']
                """ contains client name and no of instances to be run"""
                config_dict = self._get_sysbench_config(config_name)
                """ Contains options,testname and commands"""
                sysbench_cli = self._generate_cli_command(config_dict)
                log.info("Generate CLi data {}".format(sysbench_cli))
                for client in tmp_workload_cfg['clients']:
                    if client in job_dict:
                        job_dict[client].extend(sysbench_cli)
                    else:
                        job_dict[client] = sysbench_cli
                    job_dict[client]= job_dict[client] * client_dict[client]
            return job_dict

    def _generate_cli_command(self, config_dict):
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

        cmds = []
        # add command. For each command create a separate entry if
        # multiple commands are provided
        import pdb;pdb.set_trace()
        if type(config_dict['commands']) is list:

            for entry in config_dict['commands']:
                cmds.append(cli_cmd+" "+entry)
            log.info("Generated sysbench commands: {}".format(cmds))
            return [";".join(cmds)]
        else:
            cli_cmd += ' {}'.format(config_dict['commands'])
            log.info("Generated sysbench commands: {}".format(cmds))
            cmds.append(cli_cmd)
            return cmds


    def get_cli_commands(self):
        """
        get cli command to be executed
        """
        if not self._cli_command_dict:
            import pdb;pdb.set_trace()
            self._cli_command_dict = self._get_job_details()
        log.info(" Sending the command dict {}".format(self._cli_command_dict))
        return self._cli_command_dict

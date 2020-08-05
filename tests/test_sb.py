import sysbench
import logging 

log = logging.getLogger(__name__)

def test_sysbench():
    #sb = sysbench.Sysbench('../libs/sysbench/yaml/sample2.yaml', 'job_1')
    #sb = sysbench.Sysbench('../libs/sysbench/yaml/sample2.yaml', 'job_2')

    sb = sysbench.Sysbench('../libs/sysbench/yaml/sample2.yaml')
    sb.start()
    log.info(sb._results)


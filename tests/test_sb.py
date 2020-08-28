from libs.sysbench.sysbenchutil import Sysbench
import logging 

def test_sysbench():
    sb = Sysbench('../libs/sysbench/yaml/sample2.yaml')
    sb.start()
    logging.info(sb._results)


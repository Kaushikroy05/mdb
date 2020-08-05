import yaml
import os
import logging

log = logging.getLogger(__name__)

def read_yaml(filepath):
    """
    read yaml file and returns data in dictionary format

    Args:
        filepath (str): yaml file path to read
    """
    try:
        with open(filepath, 'r') as rfp:
            data = yaml.safe_load(rfp)
        return data
    except IOError as ioe:
        log.error("Unable to read file: {}".format(filepath))
    except yaml.YAMLError as ye:
        log.error("Check YAML file format: {}".format(filepath))
    except Exception as e:
        log.error("Unexpected error: {}".format(str(e)))

def write_yaml(filepath, data, overwrite=False):
    """
    Write data to yaml file

    Args:
        filepath (str): write yaml data to filepath
        data (dict): yaml data
        overwrite (bool): if set to true overwrite file
    """
    if os.path.isfile(filepath) and not overwrite:
        log.warning("File '{}' exists. overwrite set to False, returning.".format(filepath))
        return

    try:
        with open(filepath, 'w') as wfp:
            yaml.dump(data, wfp, default_flow_style=False)
        return data
    except IOError as e:
        log.error("Unable to write to file: {}".format(filepath))
    except Exception as e:
        log.error("Unexpected error: {}".format(str(e)))


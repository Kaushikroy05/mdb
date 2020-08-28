import logging
import logging.handlers
import os
from pathlib import Path
import sys


class LogSetup:
    current_logger = None

    def __init__(self, log_fmt=None) -> None:
        """
        sets logging log level and log file directory
        """
        self.rcount = 10
        self.encoding = "utf8"
        self.mode = "a"
        if log_fmt is None:
            self._fmt = logging.Formatter(
                fmt="%(asctime)s - %(levelname)s [%(name)s] -  %(filename)s:%(lineno)d - %(message)s "
            )
        else:
            self._fmt = logging.Formatter(fmt=log_fmt)

    def logfile(self, filename: str, log_level: int = logging.INFO) -> None:

        if not self.current_logger:
            self.current_logger = logging.getLogger()

        self.current_logger.setLevel(log_level)

        # create module level folder
        module_logdir = os.path.dirname(filename)
        Path(module_logdir).mkdir(parents=True, exist_ok=True)

        # TODO: visit this if consumed by standalone script
        pytest_logdir = os.path.abspath(os.path.dirname(module_logdir))
        top_logdir = os.path.abspath(os.path.dirname(pytest_logdir))
        link_file = f"{os.path.abspath(top_logdir)}/latest"
        if Path(link_file).is_symlink():
            Path(link_file).unlink()
        Path(link_file).symlink_to(os.path.basename(pytest_logdir),
                                   target_is_directory=True)
        print(module_logdir, pytest_logdir, top_logdir, link_file)
        file_handler = logging.handlers.RotatingFileHandler(
            filename, mode=self.mode, encoding=self.encoding, maxBytes=10485760, backupCount=self.rcount
        )
        file_handler.setFormatter(self._fmt)
        file_handler.setLevel(log_level)
        self.current_logger.addHandler(file_handler)

        stream_handler = logging.StreamHandler(stream=sys.stdout)
        stream_handler.setFormatter(self._fmt)
        stream_handler.setLevel(log_level)
        for hdlr in self.current_logger.handlers:
            if type(hdlr) == logging.StreamHandler:
                return
        self.current_logger.addHandler(stream_handler)

    def reset_logger(self) -> None:
        """ Removes all handlers except StreamHandlers """
        if not self.current_logger:
            self.current_logger = logging.getLogger()
        for hdlr in self.current_logger.handlers:
            if type(hdlr) != logging.StreamHandler:
                self.current_logger.removeHandler(hdlr)

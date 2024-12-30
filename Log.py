import os
import logging
from logging.handlers import RotatingFileHandler


class LogHandler:
    """
    A class to manage and configure a rotating logger.
    
    Features:
      - Configurable base directory for logs.
      - Automatic rotation by file size using RotatingFileHandler.
      - Easy methods to clear the log file or rotate logs on demand.
      - Ability to set the logging level dynamically.
      - Convenience methods for adding log lines at various severity levels.
      - Safe initialization to prevent multiple handlers being attached.
    """

    def __init__(
        self,
        logger_name: str = "my_logger",
        log_filename: str = "debug.log",
        backup_filename: str = "debug.log.old",   # For demonstration if you want manual rename.
        base_dir: str = ".",
        max_bytes: int = 1_000_000,  # 1 MB
        backup_count: int = 1
    ):
        """
        :param logger_name: Name of the logger.
        :param log_filename: Primary log file name.
        :param backup_filename: Name for backup logs (optional; if you want to rename manually).
        :param base_dir: Base directory where log files should be stored.
        :param max_bytes: Max file size (in bytes) before rotating.
        :param backup_count: Number of backup files to keep.
        """
        self.logger_name = logger_name
        self.log_filename = log_filename
        self.backup_filename = backup_filename
        self.base_dir = base_dir
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self.level = logging.DEBUG
        
        # Internal references
        self._logger = None
        self._log_path = os.path.join(self.base_dir, self.log_filename)

        self._setup_logger()

    def _setup_logger(self):
        """
        Sets up the logger if it hasn't been created yet.
        """
        # Create directories if they don't exist
        os.makedirs(self.base_dir, exist_ok=True)

        self._logger = logging.getLogger(self.logger_name)
        self._logger.setLevel(self.level)

        # Prevent attaching multiple handlers if re-initialized
        if not self._logger.handlers:
            rotating_handler = RotatingFileHandler(
                filename=self._log_path,
                maxBytes=self.max_bytes,
                backupCount=self.backup_count,
            )

            formatter = logging.Formatter(
                fmt="%(asctime)s [%(levelname)-8s] %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            rotating_handler.setFormatter(formatter)

            self._logger.addHandler(rotating_handler)

    # -----------------------
    # Getter & utility methods
    # -----------------------
    def get_logger(self) -> logging.Logger:
        """
        Return the underlying logger instance.
        """
        return self._logger

    def get_log_path(self) -> str:
        """
        Returns the full path of the log file.
        """
        return self._log_path

    def set_level(self, level: int):
        """
        Dynamically set the logging level.
        
        :param level: Logging level (e.g., logging.DEBUG, logging.INFO).
        """
        self._logger.setLevel(level)

    def clear_log(self):
        """
        Clears the contents of the current log file.
        """
        with open(self._log_path, "w"):
            pass
    
    def logfile_exists(self) -> bool:
        """
        Check if the log file exists and filesize > 0
        """
        if os.path.exists(self._log_path):
            return os.path.getsize(self._log_path) > 0
        
        return False
    
    def rotate_logs(self):
        """
        Manually force the rotation of logs, if you ever need that.
        This simply copies the file to a backup and then clears the main log.
        
        By default, RotatingFileHandler only rotates automatically when 
        the file size is exceeded. If you want to rename the backup to 
        `debug.log.old`, you can do it here.
        """
        for handler in self._logger.handlers:
            if isinstance(handler, RotatingFileHandler):
                handler.close()

        main_log = self._log_path
        backup_log = os.path.join(self.base_dir, self.backup_filename)

        # Rename the current log -> backup_log
        if os.path.exists(main_log):
            if os.path.exists(backup_log):
                os.remove(backup_log)
            os.rename(main_log, backup_log)

        # Reopen/reattach the handler so new logs go to a fresh main log
        self._setup_logger()

    # -----------------------
    # Convenience log methods
    # -----------------------
    def debug(self, msg: str):
        """
        Add a debug-level log message.
        """
        self._logger.debug(msg)

    def info(self, msg: str):
        """
        Add an info-level log message.
        """
        self._logger.info(msg)

    def warning(self, msg: str):
        """
        Add a warning-level log message.
        """
        self._logger.warning(msg)

    def error(self, msg: str):
        """
        Add an error-level log message.
        """
        self._logger.error(msg)

    def critical(self, msg: str):
        """
        Add a critical-level log message.
        """
        self._logger.critical(msg)

    def log(self, level: int, msg: str):
        """
        Add a log message at a specified level.
        """
        self._logger.log(level, msg)

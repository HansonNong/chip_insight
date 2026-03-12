import logging
from logging.handlers import RotatingFileHandler
from nicegui import ui

from .config import Config

class AppLogger:
    def __init__(self, log_file=Config.LOG_FILE):
        self._logger = logging.getLogger("ChipInSight")
        self._logger.setLevel(logging.INFO)
        if self._logger.handlers:
            return

        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        fh = RotatingFileHandler(
            log_file,
            maxBytes=Config.LOG_MAX_BYTES,
            backupCount=Config.LOG_BACKUP_COUNT,
            encoding="utf-8"
        )
        fh.setFormatter(formatter)
        self._logger.addHandler(fh)

        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        self._logger.addHandler(ch)

        self.tip_label = None

    def set_tip_label(self, label: ui.label):
        self.tip_label = label

    def log(self, message: str, level: str = "info"):
        color_map = {
            "info": "text-blue-500",
            "success": "text-green-600",
            "warn": "text-amber-600",
            "error": "text-red-600"
        }
        if self.tip_label:
            self.tip_label.set_text(message)
            self.tip_label.classes(replace=f"text-sm {color_map.get(level, 'text-black')}")

        log_mapping = {
            "info": self._logger.info,
            "success": self._logger.info,
            "warn": self._logger.warning,
            "error": self._logger.error
        }
        log_func = log_mapping.get(level, self._logger.info)
        prefix = "[SUCCESS] " if level == "success" else ""
        log_func(f"{prefix}{message}")

    def info(self, msg): self.log(msg, "info")
    def success(self, msg): self.log(msg, "success")
    def warn(self, msg): self.log(msg, "warn")
    def error(self, msg): self.log(msg, "error")
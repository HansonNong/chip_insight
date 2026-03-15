import logging
from logging.handlers import RotatingFileHandler
from typing import Literal
from nicegui import ui

# Assuming .config exists in the package structure
from .config import Config

class AppLogger:
    def __init__(self, log_file: str = Config.LOG_FILE) -> None:
        """Initialize logger with both file and stream handlers."""
        self._logger = logging.getLogger("ChipInSight")
        self._logger.setLevel(logging.INFO)
        
        # Prevent duplicate handlers if singleton-like access occurs
        if self._logger.handlers:
            return

        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        # File Handler with rotation
        fh = RotatingFileHandler(
            log_file,
            maxBytes=Config.LOG_MAX_BYTES,
            backupCount=Config.LOG_BACKUP_COUNT,
            encoding="utf-8"
        )
        fh.setFormatter(formatter)
        self._logger.addHandler(fh)

        # Console Handler
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        self._logger.addHandler(ch)

        self.tip_label: ui.label | None = None

    def set_tip_label(self, label: ui.label) -> None:
        """Bind a NiceGUI label component to display real-time status updates."""
        self.tip_label = label

    def log(self, message: str, level: Literal["info", "success", "warn", "error"] = "info") -> None:
        """Log message to system and update the UI status label if available."""
        color_map = {
            "info": "text-blue-500",
            "success": "text-green-600",
            "warn": "text-amber-600",
            "error": "text-red-600"
        }

        # Update NiceGUI UI component
        if self.tip_label:
            self.tip_label.set_text(message)
            self.tip_label.classes(replace=f"text-sm {color_map.get(level, 'text-black')}")

        # Map internal levels to standard logging levels
        log_mapping = {
            "info": self._logger.info,
            "success": self._logger.info,
            "warn": self._logger.warning,
            "error": self._logger.error
        }
        
        log_func = log_mapping.get(level, self._logger.info)
        prefix = "[SUCCESS] " if level == "success" else ""
        log_func(f"{prefix}{message}")

    def info(self, msg: str) -> None:
        """Log an informational message."""
        self.log(msg, "info")

    def success(self, msg: str) -> None:
        """Log a success message."""
        self.log(msg, "success")

    def warn(self, msg: str) -> None:
        """Log a warning message."""
        self.log(msg, "warn")

    def error(self, msg: str) -> None:
        """Log an error message."""
        self.log(msg, "error")
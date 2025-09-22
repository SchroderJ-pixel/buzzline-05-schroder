"""
Logger Setup Script
File: utils/utils_logger.py

- Logs to a unique file per process (avoids Windows/OneDrive file-lock/rotation issues)
- Ensures the log directory exists
- Sanitizes log messages to strip personal paths/usernames
"""

#####################################
# Imports
#####################################
import os
import sys
import getpass
import pathlib
from typing import Mapping, Any
from loguru import logger

#####################################
# Constants / Paths
#####################################
CURRENT_SCRIPT = pathlib.Path(__file__).stem
LOG_FOLDER: pathlib.Path = pathlib.Path("logs")
# Unique file per process using PID (stable on Windows)
LOG_FILE: pathlib.Path = LOG_FOLDER / f"project_log_{os.getpid()}.log"

# Ensure the log folder exists early
try:
    LOG_FOLDER.mkdir(parents=True, exist_ok=True)
    print(f"Log folder ready at: {LOG_FOLDER}")
except Exception as e:
    print(f"Error creating log folder: {e}", file=sys.stderr)

#####################################
# Helper Functions (must be defined before logger.add)
#####################################
def sanitize_message(record: Mapping[str, Any]) -> str:
    """Remove personal/identifying information from log messages and escape braces."""
    message = record["message"]

    # Replace username with placeholder
    try:
        message = message.replace(getpass.getuser(), "USER")
    except Exception:
        pass

    # Replace home directory
    try:
        message = message.replace(str(pathlib.Path.home()), "~")
    except Exception:
        pass

    # Replace absolute CWD with PROJECT_ROOT
    try:
        message = message.replace(str(pathlib.Path.cwd()), "PROJECT_ROOT")
    except Exception:
        pass

    # Normalize slashes for readability
    message = message.replace("\\", "/")

    # Escape braces so Loguru doesn't treat them as formatting fields
    message = message.replace("{", "{{").replace("}", "}}")
    return message


def format_sanitized(record: Mapping[str, Any]) -> str:
    """Custom formatter that sanitizes messages and returns a plain string."""
    message = sanitize_message(record)
    time_str = record["time"].strftime("%Y-%m-%d %H:%M:%S")
    level_name = record["level"].name
    return f"{time_str} | {level_name} | {message}\n"

#####################################
# Logger Configuration
#####################################
try:
    logger.remove()

    # File sink: per-process file, rotation disabled to avoid rename races
    logger.add(
        LOG_FILE,
        level="INFO",
        rotation=None,      # no rotation (prevents WinError 32 on OneDrive)
        retention=None,
        compression=None,
        enqueue=True,       # safe across threads/processes
        backtrace=False,
        diagnose=False,
        format=format_sanitized,
    )

    # Console sink
    logger.add(
        sys.stderr,
        level="INFO",
        enqueue=True,
        backtrace=False,
        diagnose=False,
        format=format_sanitized,
    )

    logger.info(f"Logging to file: {LOG_FILE}")
    logger.info("Log sanitization enabled, personal info removed")
except Exception as e:
    # Minimal fallback to console if file sink fails
    try:
        logger.add(sys.stderr, level="INFO")
    except Exception:
        pass
    print(f"Error configuring logger to write to file: {e}", file=sys.stderr)

#####################################
# Public Helpers
#####################################
def get_log_file_path() -> pathlib.Path:
    """Return the concrete log file path (PID-based)."""
    return LOG_FILE


def log_example() -> None:
    """Example logging to demonstrate behavior."""
    try:
        logger.info("This is an example info message.")
        logger.info(f"Current working directory: {pathlib.Path.cwd()}")
        logger.info(f"User home directory: {pathlib.Path.home()}")
        logger.warning("This is an example warning message.")
        logger.error("This is an example error message.")
    except Exception as e:
        logger.error(f"An error occurred during logging: {e}")

#####################################
# CLI Test
#####################################
def main() -> None:
    logger.info(f"STARTING {CURRENT_SCRIPT}.py")
    log_example()
    logger.info(f"View the log output in: {LOG_FILE}")
    logger.info(f"EXITING {CURRENT_SCRIPT}.py.")

if __name__ == "__main__":
    main()

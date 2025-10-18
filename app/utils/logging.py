from loguru import logger
import sys

logger.remove()
logger.add(sys.stderr, level="INFO", enqueue=True, backtrace=True, diagnose=False)
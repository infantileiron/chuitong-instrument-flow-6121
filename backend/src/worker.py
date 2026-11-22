import logging
import time
from pathlib import Path

from src.config.settings import CACHE
from src.services.batches import StaleWorker, consume

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


if __name__ == '__main__':
    while True:
        try:
            CACHE.ping()
            try:
                run_id = consume()
                if run_id:
                    logger.info('Worker 推进了运行 %s', run_id)
            except StaleWorker:
                logger.warning('本进程纪元已过期，等待下一轮领取')
            Path('/tmp/worker-heartbeat').touch()
        except Exception:
            logger.exception('Queue iteration will retry')
        time.sleep(1)

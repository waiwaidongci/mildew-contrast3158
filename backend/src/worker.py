import logging
import time
from src.config.settings import HEARTBEAT
from src.services.execution import claim, execute, materialize_due
from src.utils.wakeup import wait

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)
heartbeat = HEARTBEAT


def main():
    while True:
        try:
            materialize_due()
            lease = claim()
            heartbeat.touch()
            if lease:
                execute(lease)
                heartbeat.touch()
            else:
                wait()
        except Exception:
            log.exception('Worker cycle failed')
            time.sleep(2)


if __name__ == '__main__':
    main()

import logging
from pathlib import Path
from threading import Event, Thread
from time import monotonic, sleep

from src.config.settings import ARTIFACTS, cache, engine
from src.services.domain import solve
from src.services.expansion import ExpansionInterrupted
from src.services.workflow import claim, finish, heartbeat, interrup

HEARTBEAT = Path(ARTIFACTS) / 'worker-heartbeat'
LEASE_REFRESH = 30


def heartbeat_file(stop):
    while not stop.is_set():
        HEARTBEAT.touch()
        stop.wait(2)


def lease(stop, run_id, token, lost):
    """Refresh the run lease so another worker does not reclaim live work."""
    next_refresh = 0
    while not stop.is_set():
        if monotonic() >= next_refresh:
            try:
                if not heartbeat(run_id, token):
                    lost.set()
                    return
            except Exception:
                logging.exception('Lease refresh failed for run %s', run_id)
            next_refresh = monotonic() + LEASE_REFRESH
        stop.wait(2)


def work():
    HEARTBEAT.unlink(missing_ok=True)
    while True:
        try:
            job = claim()
            HEARTBEAT.touch()
            if not job:
                cache.brpop('worker:wake', timeout=1)
                continue
            run_id, token, payload = job
            stop = Event()
            lost = Event()
            threads = [Thread(target=heartbeat_file, args=(stop,), daemon=True),
                       Thread(target=lease, args=(stop, run_id, token, lost), daemon=True)]
            for thread in threads:
                thread.start()
            try:
                result = solve(payload, engine, run_id=run_id,
                               should_stop=lambda: not lost.is_set() and not stop.is_set())
                finish(run_id, token, result=result)
            except ExpansionInterrupted:
                interrup(run_id, token)
            except (ValueError, KeyError, TypeError) as exc:
                finish(run_id, token, error=str(exc)[:500])
            except Exception:
                logging.exception('Domain run interrupted: %s', run_id)
                interrup(run_id, token)
            finally:
                stop.set()
                for thread in threads:
                    thread.join()
        except Exception:
            logging.exception('Worker loop failure')
            sleep(2)


if __name__ == '__main__':
    work()

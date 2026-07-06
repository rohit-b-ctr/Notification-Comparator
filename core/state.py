"""Shared mutable runtime state for live/watch/capture threads."""
import threading


class Broadcaster:
    """Append-only event log with broadcast (not consume-once) semantics.

    A plain queue.Queue is single-consumer: if a browser refresh opens a new
    SSE connection while the old one's generator is still blocked reading the
    same queue, whichever side wins a given item steals it from the other —
    so the new tab can silently miss messages and the run looks "stopped".
    Every reader here gets its own index into the shared log, so a refresh
    just starts a fresh reader over the same history instead of racing one.
    """
    def __init__(self):
        self._log = []
        self._cond = threading.Condition()

    def put(self, item):
        with self._cond:
            self._log.append(item)
            self._cond.notify_all()

    def get_from(self, idx, timeout=30):
        """Return (next_idx, item). item is None on timeout (caller should ping)."""
        with self._cond:
            if idx >= len(self._log):
                self._cond.wait(timeout)
            if idx < len(self._log):
                return idx + 1, self._log[idx]
            return idx, None


watch_state = {
    "running": False,
    "results": [],
    "log_queue": Broadcaster(),
    "thread": None,
    "mode": "full",
}

# Full Run = live compare across ALL configured flows at once (time-bounded).
full_watch_state = {
    "running": False,
    "results": [],
    "log_queue": Broadcaster(),
    "thread": None,
    "mode": "full",
    "started_at": None,
}

capture_state = {
    "running": False,
    "seen":    set(),
    "saved":   {},
    "log_queue": Broadcaster(),
    "thread":  None,
}

# Live capture of Kowl topic messages -> kowl golden (mirrors DB live capture)
kowl_capture_state = {
    "running": False,
    "saved":   {},
    "log_queue": Broadcaster(),
    "thread":  None,
}

# One-shot topic baseline capture with per-topic progress streaming
topic_capture_state = {
    "running":   False,
    "log_queue": Broadcaster(),
    "thread":    None,
}

# One-shot topic compare with per-topic progress streaming
topic_compare_state = {
    "running":   False,
    "log_queue": Broadcaster(),
    "thread":    None,
}

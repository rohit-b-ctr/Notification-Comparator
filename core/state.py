"""Shared mutable runtime state for live/watch/capture threads."""
import queue

watch_state = {
    "running": False,
    "results": [],
    "log_queue": queue.Queue(),
    "thread": None,
    "mode": "full",
}

# Full Run = live compare across ALL configured flows at once (time-bounded).
full_watch_state = {
    "running": False,
    "results": [],
    "log_queue": queue.Queue(),
    "thread": None,
    "mode": "full",
    "started_at": None,
}

capture_state = {
    "running": False,
    "seen":    set(),
    "saved":   {},
    "log_queue": queue.Queue(),
    "thread":  None,
}

# Live capture of Kowl topic messages -> kowl golden (mirrors DB live capture)
kowl_capture_state = {
    "running": False,
    "saved":   {},
    "log_queue": queue.Queue(),
    "thread":  None,
}

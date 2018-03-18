from enum import Enum


class TaskStatus(Enum):
    """
    holds tasks status values
    """
    QUEUED = 'queued'
    FINISHED = 'finished'
    FAILED = 'failed'
    STARTED = 'started'
    DEFERRED = 'deferred'
    NOT_QUEUED = 'not queued'


class WorkerStatus(Enum):
    """
    holds workers status values
    """
    WORKING = 'working'
    IDLE = 'idle'
    STOPPED = 'stopped'

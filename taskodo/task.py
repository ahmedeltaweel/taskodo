from .config.status import TaskStatus
import logging


class Task:

    def __init__(self, fn, args, depend=None, incompatible=None):
        self.fn = fn
        self.args = args
        self.id = None
        self.timeout = 600
        self.status = TaskStatus.QUEUED
        self.result = None
        self.error_message = None
        self.depend = depend
        self.incompatible = incompatible

    def set_id(self, _id):
        self.id = _id

    def set_timeout(self, _timeout):
        self.timeout = _timeout

    def is_finished(self):
        return self.status == TaskStatus.FINISHED

    def is_started(self):
        return self.status == TaskStatus.STARTED

    def is_failed(self):
        return self.status == TaskStatus.FAILED

    def get_status(self):
        return self.status

    def set_depend(self, _depend):
        self.depend = _depend

    def set_incompatible(self, _incompatible):
        self.incompatible = _incompatible

    def canRun(self, tasks):
        if self.depend is not None:
            for dependency in self.depend:
                task = tasks.get(dependency, None)
                if task is None:
                    logging.debug(
                        "Cannot run task " + str(self.id) + ". Unable to find task " + str(dependency) + " in queue.")
                    return False
                if not task.is_finished():
                    logging.debug("Cannot run task " + str(self.id) + ". Task " + str(dependency) + " is not finished")
                    return False

        if self.incompatible is not None:
            for task in tasks.values():
                if task.is_started() and str(task.fn.__name__) in self.incompatible:
                    logging.debug(
                        "Cannot run task " + str(self.id) + ". Conflicting task " + str(task.fn.__name__) + " is running.")
                    return False
        return True

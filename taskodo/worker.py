import logging
from threading import Thread

from .config.status import WorkerStatus, TaskStatus


class Worker:
    def __init__(self, _id, _queue):
        self.id = _id
        self.queue = _queue
        self.status = WorkerStatus.IDLE
        self.must_die = False
        self.task = None

    def notify(self):
        if self.status is not WorkerStatus.WORKING:
            if self.must_die:
                self.queue.remove_worker(self.id)
            else:
                task = self.queue.dequeue()
                if task != None:
                    self.task = task
                    WorkerThread(self).start()

    def run(self):
        try:
            logging.debug("Worker " + str(self.id) + " starts working...")
            self.status = WorkerStatus.WORKING
            # Execute the function
            fn = self.task.fn
            args = self.task.args
            self.task.status = TaskStatus.STARTED
            self.task.result = fn(*args)
            self.task.status = TaskStatus.FINISHED
        except Exception as ex:
            if self.task is not None:
                self.task.status = TaskStatus.FAILED
                self.task.error_message = ex.message
            else:
                logging.debug("WORKER " + str(self.id) + " WITHOUT TASK.")
        finally:
            logging.debug("Worker " + str(self.id) + " stops working...")
            self.status = WorkerStatus.IDLE
            self.task = None
            self.notify()


class WorkerThread (Thread):
    def __init__(self, worker):
        Thread.__init__(self)
        self.worker = worker

    def run(self):
        self.worker.run()

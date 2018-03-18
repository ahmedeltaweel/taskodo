import logging
from threading import RLock as threading_lock, Timer
from collections import deque

from .singelton import Singleton
from .worker import Worker
from .config.status import TaskStatus
from .task import Task


class Queue(metaclass=Singleton):
    def __init__(self):
        logging.info("CREATING NEW INSTANCE FOR Queue...")
        self.lock = threading_lock()
        self.queue = deque([])
        self.tasks = {}
        self.workers = []
        self.timer = None

    def start_worker(self, n_workers=1):
        ids = []
        worker_id = ""
        for i in range(0, n_workers):
            worker_id = "w" + self.get_random_id()
            self.workers.append(Worker(worker_id, self))
            ids.append(worker_id)
        return ids

    def stop_worker(self, worker_id=None):
        try:
            self.lock.acquire()  # LOCK CACHE
            if worker_id is None:
                for worker in self.workers:
                    if worker.must_die is not True:
                        worker_id = worker.id
                        break
                if worker_id is None:
                    logging.info("All workers will die...")

            for worker in self.workers:
                if worker.id == worker_id:
                    worker.must_die = True
                    break
        finally:
            self.lock.release()  # UNLOCK CACHE
            self.notify_workers()

    def remove_worker(self, worker_id):
        try:
            self.lock.acquire()  # LOCK CACHE
            i = 0
            for worker in self.workers:
                if worker.id == worker_id and worker.must_die is True:
                    self.workers.pop(i)
                    break
                i += 1
        finally:
            self.lock.release()  # UNLOCK CACHE
            self.notify_workers()

    def enqueue(self, fn, args, task_id="", timeout=600, depend=None, incompatible=None):
        try:
            self.lock.acquire()  # LOCK CACHE
            task = Task(fn, args, depend, incompatible)
            if task_id == "":
                task_id = self.get_random_id()
                while task_id in self.tasks:
                    task_id = self.get_random_id()
            elif task_id in self.tasks:
                raise RuntimeError("Task already at the queue (Task id : " + str(task_id) + ")")
            task.set_id(task_id)
            task.set_timeout(timeout)

            self.tasks[task_id] = task
            self.queue.appendleft(task)
            logging.debug("New task " + str(task_id) + " added to queue.")
            logging.debug("Queue length " + str(len(self.queue)))
        except Exception as ex:
                logger.error('Failed: ' + str(ex))
        finally:
            self.lock.release()  # UNLOCK CACHE
            self.notify_workers()
            return task_id

    def dequeue(self):
        try:
            self.lock.acquire()  # LOCK CACHE

            if len(self.queue) > 0:
                switch_pos = 1
                nextTask = self.queue[len(self.queue) - 1]
                runnable = nextTask.canRun(self.tasks)

                while not runnable:
                    switch_pos = switch_pos + 1
                    if switch_pos > len(self.queue):
                        # Reset queue state
                        self.queue.rotate(1)
                        switch_pos = 0
                        logging.debug("Cannot find runnable tasks, waiting for next try...")
                        if self.timer is None:
                            self.timer = Timer(10.0, self.notify_workers)
                            self.timer.start()
                        return None
                    elif len(self.queue) > 1:
                        logging.debug("Reordering tasks...")
                        task_aux = self.queue[len(self.queue) - switch_pos]
                        self.queue[len(self.queue) - switch_pos] = self.queue[len(self.queue) - 1]
                        self.queue[len(self.queue) - 1] = task_aux
                    nextTask = self.queue[len(self.queue) - 1]
                    runnable = nextTask.canRun(self.tasks)
                logging.debug("Task dequeued.")
                logging.debug("Queue length " + str(len(self.queue)))
                return self.queue.pop()
            return None
        finally:
            self.lock.release()  # UNLOCK CACHE

    def notify_workers(self):
        logging.debug("Notifying workers")
        if self.timer is not None:
            logging.debug("Cleaning timer")
            self.timer.cancel()
            self.timer = None
        for worker in self.workers:
            worker.notify()

    def check_status(self, task_id):
        task = self.tasks.get(task_id, None)
        if task:
            return task.status
        return TaskStatus.NOT_QUEUED

    def fetch_task(self, task_id):
        return self.tasks.get(task_id, None)

    def remove_task(self, task_id):
        try:
            self.lock.acquire()  # LOCK CACHE
            if task_id in self.tasks:
                self.queue.remove(self.tasks.get(task_id))
                del self.tasks[task_id]
        finally:
            self.lock.release()  # UNLOCK CACHE

    def get_result(self, task_id, remove=True):
        task = self.tasks.get(task_id, None)
        if task:
            if remove and (task.status == TaskStatus.FINISHED or task.status == TaskStatus.FAILED):
                logging.debug("Removing task " + str(task_id))
                self.tasks.pop(task_id)
            return task.result
        return TaskStatus.NOT_QUEUED

    def get_error_message(self, task_id):
        task = self.tasks.get(task_id, None)
        if task:
            return task.error_message
        return None

    def get_random_id(self):
        """
        This function returns a new random task id
        """
        import string
        import random
        taskID = ''.join(random.sample(string.ascii_letters+string.octdigits*5, 10))
        return taskID

    def enableStdoutLogging(self):
        logging.getLogger().setLevel(logging.DEBUG)

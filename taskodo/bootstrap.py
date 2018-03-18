import os.path
import sys
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))

from time import sleep
from taskodo.queue import Queue

# ************************************************************************
# Initialize queue
# ************************************************************************

N_WORKERS = 2


def foo(N, message):
    print (message + " started...")
    sleep(N)
    print (message + " finished")


def main():
    queue_instance = Queue()
    queue_instance.start_worker(N_WORKERS)

    # queue_instance.enableStdoutLogging()

    queue_instance.enqueue(
        fn=foo,
        args=(10, "this is Task 1"),
        task_id="Task 1"
    )
    queue_instance.enqueue(
        fn=foo,
        args=(11, "this is Task 3"),
        task_id="Task 3"
    )
    queue_instance.enqueue(
        fn=foo,
        args=(4, "this is Task 2"),
        task_id="Task 2"
    )

    while 1:
        print("Task 1 is " + str(queue_instance.check_status("Task 1")))
        print("Task 2 is " + str(queue_instance.check_status("Task 2")))
        print("Task 3 is " + str(queue_instance.check_status("Task 3")))
        sleep(3)


if __name__ == '__main__':
    main()

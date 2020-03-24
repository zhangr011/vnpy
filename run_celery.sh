celery -A vnpy.task.celery_app worker --max-tasks-per-child 1 -l debug  > tests/celery/worker.log 2>tests/celery/worker-error.log &

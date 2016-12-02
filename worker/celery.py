"""
@author: Keyon Genesis
"""
from __future__ import absolute_import

from celery import Celery

app = Celery('worker',
             broker='redis://localhost:6379/1',
             backend='redis://localhost:6379/1',
             include=['worker.tasks'])

# Optional configuration, see the application user guide.
app.conf.update(
    CELERY_TASK_RESULT_EXPIRES=3600,
)

if __name__ == '__main__':
    app.start()
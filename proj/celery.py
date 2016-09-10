from __future__ import absolute_import

from celery import Celery

app = Celery('proj',
             broker='amqp://guest:guest@localhost:5672//',
             #backend='amqp',
			 include=['proj.task']
             ) #

# Optional configuration, see the application user guide.
app.conf.update(
	CELERY_RESULT_BACKEND='amqp',
    CELERY_TASK_RESULT_EXPIRES=3600,	
)


if __name__ == '__main__':
    app.start()
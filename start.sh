celery -A search worker -n worker1.%h -Q searchRequestQueue -l info -c 2 &
celery -A search worker -n worker1.%h -l info -Q searchNameQueue -c 10 &
celery -A search worker -n worker1.%h -Q exportQueue -l info -c 1 &
python search_displatcher &

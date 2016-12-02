"""
@author: Weijian
"""
from worker.tasks import process_search, export
from worker.common.amazon import connect_search_queue
import ExitStrategy

import traceback

active_messages = {}


def main():
    """
    starts queue up and dispatches to worker tasks.
    on exit will return unprocessed messages to queue.
    """

    queue = connect_search_queue()

    kill_sig = ExitStrategy()

    while True:
        try:
            if kill_sig.kill_now:
                print 'shutdown initiated'
                break
            check_messages(queue)
            check_tasks()
        except Exception, e:
            print 'error! exiting.'
            print traceback.format_exc()
    
    cleanup()
    print 'done'


def check_tasks():
    for task_id in active_messages.keys():
        task_result = process_search.AsyncResult(task_id)
        if task_result.ready():
            message = active_messages.pop(task_id, None)
            print 'message.%s finished' % message.body 
            message.delete()


def check_messages(queue):
    # Process messages by printing out body and optional author name
    for message in queue.receive_messages(WaitTimeSeconds=10):
        search_request_id = message.body 
        try:
            search_request_id = int(search_request_id)
            print search_request_id
        except:
            print 'malformed search_request_id %s' % message.body
            message.delete()
        
        task = process_search.apply_async((search_request_id,), queue='searchRequestQueue')   
        active_messages[task.task_id] = message


def cleanup():    
    print 'releasing unprocessed messages'
    for message in active_messages.values():
        message.message_visibility(VisibilityTimeout=0)

main()
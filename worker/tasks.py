"""
@author: Weijian
"""
from __future__ import absolute_import

import time

from celery import group
from celery.utils.log import get_task_logger

from worker.celery import app
from worker.common.connection import get_worker_cache, get_database
from worker.common.repository import update_status
from worker.export.exportbot import do_export
from worker.search.searchbot import do_search_name

logger = get_task_logger(__name__)


@app.task()
def process_search(search_request_id):

    logger.info( 'searching %s' % search_request_id )

    # purge any recent work
    key = 'searchrequest/%s' % search_request_id

    RW = get_worker_cache()
    RW.delete(key)

    db = get_database()

    try:
        find_search_items_sql = """
         select s.id, 
                COALESCE( reg.register_code, 'All'),
                s.namelist_id, 
                si.id as search_item_id,
                si.search_name, 
                si.details 
              from core.search_request s
              left outer join core.register reg on ( reg.id = s.register_id )
              join core.searchitem si on ( si.namelist_id = s.namelist_id )
        where s.id = %s 
        """

        logger.info('searching database')
        cursor = db.cursor('searchCursor')
        cursor.itersize = 1000

        cursor.execute(find_search_items_sql, [search_request_id] )
        logger.info('start document ')

        count = 0
        subtasks = []
        for (search_request_id, source, namelist_id, search_item_id, search_name, details ) in cursor:
            subtasks.append( search_name_task.subtask( (search_request_id, source, namelist_id, search_item_id, search_name, details )).set(queue='searchNameQueue') ) 
    except Exception, e:
        update_status(search_request_id, 'F', dict(message='failed', current=0, total=0))
        return False

    finally:
        db.close()


    logger.info("created %s subtasks" % len( subtasks ))
    job = group( subtasks )
    results = job.apply_async() 
    while results.waiting():

        update_status(search_request_id, 'S', dict(message='searching',
                        current=results.completed_count(),
                        total=len(subtasks) ))

        logger.info( "progress: %s/%s" % ( results.completed_count(), len(subtasks) ))
        time.sleep(5)

    logger.info( "progress: %s/%s" % ( results.completed_count(), len(subtasks) ))        
    logger.info("subtasks finished")
    
    if results.successful():
        update_status(search_request_id, 'S', dict(message='exporting',
                        current=results.completed_count(),
                        total=len(subtasks) ))
        export.apply_async((search_request_id,), queue='exportQueue')   
    else:
        update_status(search_request_id, 'F', dict(message='failed',
                        current=results.completed_count(),
                        total=len(subtasks) ))
    return True


@app.task()
def search_name_task(search_request_id, source, namelist_id, search_item_id, search_name, details):
    logger.info( '[%s/%s] %s ' % ( search_request_id, search_item_id, search_name ) )
    num_matches = do_search_name(search_request_id, source, namelist_id, search_item_id, search_name, details)
    return num_matches


@app.task
def export(search_request_id):
    logger.info( 'exporting %s' % search_request_id )
    do_export(search_request_id)




"""
@author: Keyon Genesis
"""
import json

from worker.common.connection import get_database


def update_status(search_request_id, status_code, status):

    update_sql = """
       update core.search_request set status_code = %s, status  = %s where id = %s 
    """
    
    db = get_database()
    try:

        cursor = db.cursor()
        cursor.execute( update_sql, [status_code, json.dumps(status), search_request_id])
        cursor.close()
        db.commit()
    finally:
        db.close()
    return


def get_search_request(search_request_id):

    sql = """
    select sr.id, nl.label, nl.user_id
      from core.search_request sr
      join core.namelist nl on ( nl.id = sr.namelist_id )
      where sr.id = %s
    """

    ids = []
    db = get_database()
    try:
        cursor = db.cursor()
        cursor.execute( sql, [search_request_id] )
        ids = [ (rs[0], rs[1], rs[2]) for rs in cursor.fetchall()]
        cursor.close()
        db.commit()
    finally:
        db.close()

    return ids


def update_search_request(search_id, filepath):

    db = get_database()
    try:

        delete_sql = """
        delete from core.search_result_export where search_request_id = %s
        """

        insert_sql = """
        insert into core.search_result_export ( search_request_id, file ) values ( %s, %s )
        """

        update_sql = """
        update core.search_request set status_code = 'C' where id =  %s
        """

        cursor = db.cursor()
        cursor.execute( delete_sql, [search_id] )
        cursor.execute( insert_sql, [search_id, filepath ] )
        cursor.execute( update_sql, [search_id] )

        cursor.close()
        db.commit()
    finally:
        db.close()


def get_reference_data(ids):

    db = get_database()
    try:
        sql = """
        select i.ext_reference_id, i.id, r.register_code, i.status, i.detail, r.country_code
          from core.individual i
          join core.register r on ( r.id = i.register_id )
          where i.id in ( %s )
        """

        sql = sql % (','.join( str(i) for i in ids))

        cursor = db.cursor()
        cursor.execute(sql)
        return { rs[1]:dict(status=rs[3],ext_reference_id=rs[0],register=rs[2], details=rs[4], country=rs[5]) for rs in cursor.fetchall()}
    finally:
        db.close()
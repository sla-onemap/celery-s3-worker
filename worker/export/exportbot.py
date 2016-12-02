"""
@author: Keyon Genesis
"""
import logging as log

import msgpack

from export_one import create_xls
from worker.common.amazon import save_to_s3
from worker.common.connection import get_cache, get_worker_cache
from worker.common.repository import update_search_request, get_search_request, get_reference_data

title_ignore = ""


def do_export(search_request_id, save_amazon=True):
    """
    builds xls file from worker. creates a local copy and another in s3
    """
    log.info("seaching for requests")

    for (search_id, namelist, user_id) in get_search_request(search_request_id):

        log.info( '[%s]building data %s' % (search_id, namelist))
        results, reference_data = get_all(search_id)
        log.info( '[%s]building xls for %s' % (search_id, namelist))
        xls_filepath = create_xls(search_id, namelist, results, reference_data )
        log.info( '[%s] created %s' % (search_id, xls_filepath ))

        if save_amazon:
            save_to_s3(user_id,xls_filepath)
            update_search_request(search_request_id, xls_filepath )
        log.info( '[%s] finished ' % search_id )


def get_all(search_id):
    """
    retrieves results from Redis worker cache
    """
    key = 'searchrequest/%s' % search_id

    R = get_cache()
    RW = get_worker_cache()

    data = RW.hgetall(key)
    log.info('worker items found %s' % len(data))

    reference_ids = {}
    reference_data = {}

    results = []

    for search_id, search_result_obj in data.iteritems():

        r = msgpack.unpackb(search_result_obj)

        r['status'] = 'NO_MATCH'

        if len(r['matches']) == 1:
            r['status'] = 'EXACT_MATCH'
        elif len(r['matches']) > 1:
            r['status'] = 'MULTIPLE_MATCH'

        for m in r['matches']:

            individual_id = m['individual_id']
            #
            # we just need the ids
            #
            reference_ids[m['individual_id']] = None

            firm_id = None
            if 'firm' in m:
                firm_id = m['firm']['id']

            m['firms'] = get_firms(R, individual_id, firm_id )

        results.append(r)
    if len(reference_ids) > 0:
        reference_data = get_reference_data( reference_ids.keys() )

    return results, reference_data


def find_name_by_id(R,id):

    obj = R.hget("names",id)
    if obj:
        return msgpack.unpackb(obj)
    return None


def find_individual(R,individual_id):
    obj = R.hget("individuals", individual_id )
    if obj:
        return msgpack.unpackb(obj)
    return None    


def get_firm_by_id(R,register_code,firm_id):
    firm_obj = R.hget("firms/%s" % register_code, firm_id )
    if firm_obj:
        return msgpack.unpackb(firm_obj)
    return None


def get_firms(R, individual_id, firm_id=None):

    firms = []
    obj = R.hget("individuals", individual_id )
    if obj:
        individual = msgpack.unpackb(obj)
        register_code = individual['r']
        for f in individual['f']:
            if firm_id and int(f) != firm_id:
                continue
            firm =  get_firm_by_id(R,register_code,int(f))
            if firm:
                firms.append(dict(firm=firm, current=True))
        for f in individual['fo']:
            if firm_id and int(f) != firm_id:
                continue
            firm =  get_firm_by_id(R,register_code,int(f))
            if firm:
                firms.append(dict(firm=firm, current=False))

    return firms


def find_firm_by_id(R, firm_id, register_code):

    firm_obj = R.hget("firms/%s" % register_code, firm_id )
    if firm_obj:
        return msgpack.unpackb(firm_obj)
    return None



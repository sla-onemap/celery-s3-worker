"""
@author: Weijian
"""
import logging as log

import msgpack

from worker.common.connection import get_cache, get_worker_cache
from worker.search.fuzzy import get_fuzz
from worker.search.name_util import get_search_key, get_partials

R = get_cache()
RW = get_worker_cache()


def do_search_name(search_request_id, source, namelist_id, search_item_id, search_name, details):
    """
    executes the worker against individuals in database. all results are stored in cache.
    """
    key = 'searchrequest/%s' % search_request_id

    log.info([details])

    search_firm = ''
    if 'firm' in details:
        search_firm = details['firm'].upper()

    search_name = search_name.strip().upper()
    search_key = get_search_key(search_name)
    search_first_name, search_last_name, search_middle = get_partials(search_name, remove_punc=True)

    individuals = find_individuals(search_key)

    search = dict(search_item_id=search_item_id,
                  search_request_id=search_request_id,
                  search_name=search_name,
                  search_info=details,
                  matches=[])

    exact_match = None
    candidates = {}
    for i in individuals:

        if exact_match:
            continue

        if source != 'All' and  i['register'] != source:
            continue

        name = i['name']
        match_score = get_score(search_name, search_first_name, search_last_name, search_middle, name, True)

        if match_score > 0:
            firms = get_firms(i['id'])

            if search_firm and firms:            
                for firm in firms:
                    f = firm['name']

                    firm_fuzz = get_fuzz( f.decode('utf-8').upper() , search_firm )

                    if firm_fuzz > 90:
                        exact_match = dict(name=name, match_score=(match_score+0.5), firm=firm, individual_id=i['id'])
                        break

            candidates[i['id']] = dict(individual_id=i['id'], match_score=match_score, name=name)

    if exact_match:
        search['matches'] = [exact_match]
    elif len(candidates) > 0:
        search['matches'] = candidates.values()

    add_records_to_db( key, search )
    return len(search['matches'])


def add_records_to_db(key, record):
    """
    saves worker results into redis
    """
    RW.hset( key, record['search_item_id'], msgpack.packb(record))


def find_name_by_id(id):
    obj = R.hget("names",id)
    if obj:
        return msgpack.unpackb(obj)
    return None


def get_firms(individual_id):

    firms = []
    obj = R.hget("individuals", individual_id )
    if obj:
        individual = msgpack.unpackb(obj)
        register_code = individual['r']
        for f in individual['f']:

            firm_id = int(f)
            firm_obj = R.hget("firms/%s" % register_code, firm_id )
            if firm_obj:
                firms.append(dict(name=msgpack.unpackb(firm_obj)['n'],id=firm_id,current=True))
        for f in individual['fo']:
            firm_id = int(f)
            firm_obj = R.hget("firms/%s" % register_code, firm_id )
            if firm_obj:
                firms.append(dict(name=msgpack.unpackb(firm_obj)['n'],id=firm_id,current=True))

        return firms
    return None


def find_individuals (search_key):
    """
    finds all individuals based on worker key in the individuals database
    """
    results = []
    for name_id in R.smembers('SK/%s' % search_key):
        name_info = find_name_by_id(name_id)
        results.append(dict(id=name_info['i_id'],
                            register=name_info['r'],
                            name_id=int(name_id),
                            name=name_info['n']))

    return results


def get_score(search_name,
              search_first_name,
              search_last_name,
              search_middle,
              candidate_name,
              exact):
    """
    returns decimal percent of similarity between candidate and individual
    """

    if search_name == candidate_name:
        return 0.50

    individual_first_name, individual_last_name, individual_middle = get_partials(candidate_name, remove_punc=True)

    if search_middle is None:
        search_middle = ''
    if individual_middle is None:
        individual_middle = ''

    if exact and ( search_first_name != individual_first_name or search_last_name != individual_last_name):
        return 0
    elif not (individual_first_name.startswith(search_first_name)) or not (individual_last_name.startswith(search_last_name)):
        return 0

    match_score = 0.25

    if len(search_middle) > 0 and len(individual_middle) > 0:
        l = min( 5, len(search_middle), len(individual_middle))
        if search_middle[0:l] != individual_middle[0:l]:
            return 0
        if l == 1:
            match_score += 0.10
        else:
            match_score += 0.25
    else:
        match_score = 0.50

    return match_score

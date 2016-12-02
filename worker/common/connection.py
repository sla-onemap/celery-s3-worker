"""
@author: Weijian
"""
import psycopg2
import redis

DATA_CACHE = redis.StrictRedis(db=0)
WORK_CACHE = redis.StrictRedis(db=2)


def get_database():
    return psycopg2.connect()


def get_cache():
    return DATA_CACHE


def get_worker_cache():
    return WORK_CACHE
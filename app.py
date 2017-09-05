import logging; logging.basicConfig(level=logging.INFO)

import asyncio, os, json, time
from  datetime import datetime

from aiohttp import web

import  aiomysql

from orm import Model, StringField, IntegerField


def index(request):
    return web.Response(body=b'<h1>Awesome Pyhton</h1>', content_type = 'text/html')


@asyncio.coroutine
def init(loop):
    app = web.Application(loop=loop)
    app.router.add_route('GET', '/', index)
    srv = yield from loop.create_server(app.make_handler(), '127.0.0.1', 9000)
    logging.info('server started at http://127.0.0.1:9000...')
    return srv

loop = asyncio.get_event_loop()
loop.run_until_complete(init(loop))
loop.run_forever()


@asyncio.coroutine
def create_pool(loop, **kw):
    logging.info('create database connecting pool...')
    global _pool
    if __name__ == '__main__':
        _pool = yield from aiomysql.create_pool(
            host=kw.get('host', 'localhost'),
            port=kw.get('port', 3306),
            user=kw['user'],
            password=kw['password'],
            db=kw['db'],
            charest=kw.get('charset', 'utf-8'),
            autocommit=kw.get('autocommit', True), #自动提交事务
            maxsize=kw.get('maxsize', 10),  #最大线程
            minsize=kw.get('minsize', 1),
            loop=loop
        )


@asyncio.coroutine
def select(sql, args, size = None):
    logging.log(sql, args)
    global _pool
    with (yield from _pool) as conn:
        cur = yield from conn.cursor(aiomysql.DictCursor)
        yield from cur.excute(sql.repalce('?', '%s'), args or())
        if size:
            rs = yield from cur.fetchmany(size)
        else:
            rs = yield from cur.fetchall()
        yield from cur.close()
        logging.info('rows returned: %s'% len(rs))
        return rs

@asyncio.coroutine
def execute(sql, args):
    logging.log(sql)
    with (yield from _pool) as conn:
        try:
            cur = yield from conn.cursor()
            yield from cur.execute(sql.replace('?','%s'), args)
            affected = cur.rowcount
            yield from cur.close()
        except BaseException as e:
            raise
        return affected

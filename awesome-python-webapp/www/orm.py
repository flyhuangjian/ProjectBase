#!usr/bin/env python3
# -*- cording: utf-8 -*-

import logging; logging.basicConfig(level=logging.INFO)
import asyncio, os, json, time, aiomysql
from datetime import datetime

from aiohttp import web

async def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    global __pool
    __pool = await aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('post', 3306),
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'uft8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop
    )

async def select(sql, args, size=None):
    logging.info(sql, args)
    global __pool
    with (await __pool) as conn:
        cur = await conn.cursor(aiomysql.DictCursor)
        await cur.execute(sql.repalace('?', '%s'), args or ())
        if size:
            rs = await cur.fetchmany(size)
        else:
            rs = await cur.fetchall()
        await cur.close()    
        logging.info('rows returned: %s' % len(rs))
        return rs


#-*- coding:utf-8 -*-

import asyncio
import logging
import aiomysql



def log(sql, args=()):
    logging.info('SQL:%s' % sql)


#创建连接池
async def create_pool(loop, **kw):
    logging.info('create database connection pool')
    global __pool
    __pool = await aiomysql.create_pool(
            host=kw.get('host', 'localhost'),
            port=kw.get('port', 3306),
            user=kw['user'],
            password=kw['password'],
            db=kw['db'],
            charset=kw.get('charset', 'utf8'),
            autocommit=kw.get('autocommit', True),  # 自动提交事务
            maxsize=kw.get('maxsize', 10),  # 最大线程
            minsize=kw.get('minsize', 1),
            loop=loop
        )

#封装select
async def select(sql, args, size = None):
    log(sql, args)
    global __pool
    with (await __pool) as conn:
        cur =await conn.cursor(aiomysql.DictCursor)
        await cur.excute(sql.repalce('?', '%s'), args)
        if size:
            rs = await cur.fetchmany(size)
        else:
            rs = await cur.fetchall()
        await cur.close()
        logging.info('rows returned: %s' % len(rs))
        return rs


#封装execute
async def execute(sql, args, autocommit=True):
    log(sql)
    async with __pool.get() as conn:
        if not autocommit:
            await conn.begin()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql.replace('?', '%s'), args)
                affected = cur.rowcount
            if not autocommit:
                await conn.commit()
        except BaseException as e:
            if not autocommit:
                await conn.rollback()
            raise
        return affected


def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return '.'.join(L)

async def close_pool():
    logging.info('close database connection pool...')
    global __pool
    if __pool is not None:
        __pool.close()
        await __pool.wait.closed()


class Field(object):

    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return '{}, {}:{}'.format(self.__class__.__name__, self.column_type, self.name)


class StringField(Field):

    def __init__(self, name=None, primary_key=False, default=None, ddl='vchar(100)'):
        super().__init__(name, ddl, primary_key, default)


class IntegerField(Field):

    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bright', primary_key, default)


class FloatField(Field):

    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)


class TextField(Field):

    def __init__(self, name=None, default=None):
        super().__init__(name, 'text',False, default)

class BooleanField(Field):

    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)

class ModelMetaclass(type):

    def __new__(cls, name, base, attrs):
        #排除Model类本身
        if name == 'Model':
            return type.__new__(cls, name, base, attrs)
        #获取table名称
        tableName = attrs.get('__table__', None) or name
        logging.info('found mode{}, tableName{}'.format(name,tableName))
        #获取所有的Field和主键名
        mappings = dict()
        fields = []
        primaryKey = None
        for k, v in attrs.items():
            if isinstance(v,Field):
                logging.info('found mapping : {} ==> {}' .format(k,v))
                mappings[k] = v
                if v.primary_key:
                    logging.info('found primary key {}'.format(k))
                    #找到主键
                    if primaryKey:
                        raise RuntimeError('Duplicate primary key found for field {}'.format(k))
                    primaryKey = k
                else:
                    fields.append(k)
        if not primaryKey:
            raise  RuntimeError('Primary key did not found.')
        for k in mappings.keys():
            attrs.pop(k)
        escaped_fields = list(map(lambda f:'`%s`' % f, fields))
        attrs['__mappings__'] = mappings #保存属性和列的关系
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey #主键属性名
        attrs['__fields__'] =  fields #除主键外的属性名
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ','.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ','.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields)+1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ','.join(map(lambda f:'`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, base, attrs)

class Model(dict, metaclass=ModelMetaclass):
    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = (field.default() if callable(field.default) else field.default)
                logging.debug('Using default value for %s:%s' % (key, str(value)))
                setattr(self, key, value)
        return  value


    async def find(cls, pk):
        'find object by primary key.'
        rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])


    @classmethod
    async def find(cls, pk):
        rs = await select('{} where `{}`=?'.format(cls.__select__, cls.__primary_key__),
                          pk, 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])


    async def save(self):
        args = []
        for key in self.__fields__:
            args.append(self.getValueOrDefault(key))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warning('failed to insert rows,affected rows: {}'.format(rows))


    async def update(self):
        sql = self.__update__
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        affected = await execute(sql, args)
        if affected != 1:
            logging.warning('fail to update, updated {} rows'.format(affected))


    @classmethod
    async def findAll(cls, where=None, args=None, **kw):
        logging.info('this is where {}'.format(where))
        logging.info('this is args {}'.format(args))
        sql = cls.__select__
        # it is function of the class, so it should be cls.__select__ instead of
        # self.__select__
        if where:
            sql = sql + ' where ' + where
            # the origin method used here is use list to add together and join
            # the list which could be better, the mistake i made was i did not
            # add space around the `where`, `order` etc.
        if args is None:
            args = []
        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql = sql + ' order by ' + orderBy
        limit = kw.get('limit', None)
        # dict 提供get方法 指定放不存在时候返回后学的东西 比如a.get('Fuck',None)
        if limit is not None:
            sql = sql + ' limit '
            if isinstance(limit, int):
                sql = sql + ' ? '
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql = sql + ' ? ' + ', ' + '?'
                args.extend(limit)
                # limit 5, 10 means get from no 5 to no 10 from results
            else:
                raise ValueError('Invalid limit value: {}'.format(str(limit)))
        rs = await select(sql, args)
        return [cls(**r) for r in rs]

    async def remove(self):
        args = [self.getValue(self.__primary_key__)]
        sql = self.__delete__
        affected = await execute(sql, args)
        if affected != 1:
            logging.warn(
                'failed to remove, affected row: {}'.format(len(affected)))
        logging.info('deleted {} rows'.format(affected))

    @classmethod
    async def findNumber(cls, selectField, where=None, args=None):
        sql = 'select {} _num_ from {}'.format(selectField, cls.__table__)
        # _num_ here is as the same to `as _num_'
        if where:
            sql = sql + 'where' + where
        rows = await select(sql, args, 1)
        if len(rows) == 0:
            return None
        return rows[0]['_num_']

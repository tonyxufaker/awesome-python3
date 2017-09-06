import orm
from Model import User, Blog, Comment



def test(loop):
    yield from orm.create_pool(loop=loop, user='www-data', password='www-data', database='awesome')
    u = User(name='Test', email='test@example.com', passwd='1234567890', image='about:blank')
    yield from u.save()


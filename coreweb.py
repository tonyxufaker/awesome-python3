import asyncio


def get(path):
    '''
    Define decorator @get('/path')
    '''
    def decorator(func):


import redis
import md5
import json
import time

conn = redis.Redis(host='192.168.0.226', port=6379, db=7)

REDISKEY_HASH_TOKEN = 'login:'
REDISKEY_RECENT = 'recent:'
REDISKEY_VIEWED = 'viewed:'
REDISKEY_CART_PRE = 'cart:'

QUIT = False
LIMIT = 1

def check_token(conn, token):
    return conn.hget(REDISKEY_HASH_TOKEN, token)

def update_token(conn, token, user, item=None):
    timestamp = time.time()
    conn.hset(REDISKEY_HASH_TOKEN, token, user)
    conn.zadd(REDISKEY_RECENT, token, timestamp)
    if item:
        conn.zadd(REDISKEY_VIEWED + str(token), item , timestamp)
        conn.zremrangebyrank(REDISKEY_VIEWED + str(token), 0, -26)

def new_token(conn, user):
    token = str(md5.md5(str(user)))
    if (conn.hset(REDISKEY_HASH_TOKEN, token, user)):
        return token

def show_token_details(conn, token):
    conn.hget(REDISKEY_HASH_TOKEN, token)
    conn.zrange(REDISKEY_RECENT, 0, -1)

def clean_sessions(conn):
    while not QUIT:
        size = conn.zcard(REDISKEY_RECENT)
        if size <= LIMIT:
            time.sleep(5)
            continue
        
        end_index = min(size - LIMIT, 100)
        tokens = conn.zrange(REDISKEY_RECENT, 0, end_index - 1)

        session_keys = []
        for token in tokens:
            session_keys.append(REDISKEY_VIEWED + token)
            session_keys.append(REDISKEY_CART_PRE + token)
        
        conn.delete(*session_keys)
        conn.hdel(REDISKEY_HASH_TOKEN, *tokens)
        conn.zrem(REDISKEY_RECENT, *tokens)

def multi_new(conn):
    user = time.time()
    token = new_token(conn, user)

    item_id = 'item' + str(time.time())
    update_token(conn, token, user, item_id)

    item_id = 'item' + str(time.time())
    update_token(conn, token, user, item_id)

def add_to_cart(conn, token, item, count):
    if count <= 0:
        conn.hrem(REDISKEY_CART_PRE + token, item)
    else:
        conn.hincrby(REDISKEY_CART_PRE + token, item, count)

def cache_request(conn, request, callback):
    if not can_cache(conn, request):
        return callback(request)
    page_key = 'cache:' + hash_request(request)
    content = conn.get(page_key)

    if not content:
        content = callback(request)
        conn.setex(page_key, content, 300)
    return content

def test1(conn):
    multi_new(conn)
    time.sleep(5)
    clean_sessions(conn)

def test2(conn):
    user = time.time()
    token = new_token(conn, user)

    item1 = 'a'
    item2 = 'b'
    add_to_cart(conn, token, item1, 1)
    add_to_cart(conn, token, item1, 1)
    add_to_cart(conn, token, item1, 1)
    add_to_cart(conn, token, item2, 1)


def run(conn=conn):
    test2(conn)

run()
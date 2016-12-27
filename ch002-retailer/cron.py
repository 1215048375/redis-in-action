import time

QUIT = False

RKEY_ZSET_DELAY = 'delay:'
RKEY_ZSET_SCHEDULE = 'schedule:'

def schedule_row_cache(conn, row_id, delay):
    conn.zadd(RKEY_ZSET_DELAY, row_id, delay)
    conn.zadd(RKEY_ZSET_SCHEDULE, row_id, time.time())

def cache_rows(conn):
    while not QUIT:
        next = conn.zrange(RKEY_ZSET_SCHEDULE, 0, 0 ,withscores=True)
        now = time.time()
        if not next or next[0][1] > now:
            time.sleep(.05)
            continue
        
        row_id = next[0][0]

        delay = conn.zscore(RKEY_ZSET_DELAY, row_id)
        if delay <= 0:
            conn.zrem(RKEY_ZSET_DELAY, row_id)
            conn.zrem(RKEY_ZSET_SCHEDULE, row_id)
            conn.delete('inv:' + row_id)
            continue
        
        row = Inventory.get(row_id)
        conn.zadd(RKEY_ZSET_SCHEDULE, row_id, now + delay)
        conn.set('inv:' + row_id, json.dumps(row.to_dict()))

def rescale_viewed(conn):
    while not Quit:
        conn.zremrangebyrank('viewed:', 0, -20001)
        conn.zinterstore('voiwed:', {'viwed:': .5})
        time.sleep(300)

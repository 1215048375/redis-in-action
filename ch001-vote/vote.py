import redis
import time
import json

INIT_ARTICLE_ID = 10000
ONE_WEEK_IN_SECONDS = 86400 * 7
VOTE_SCORE = 4320000

ARTICLES_PER_PAGE = 25

REDISKEY_ARTICLE_ID = 'article_id_incr'
REDISKEY_HASH_ARTICLE_PRE = 'article:'
REDISKEY_SET_VOTED_PRE = 'voted:'
REDISKEY_ZSET_ARTICLE_TIME_ORDER = 'article:timeorder'
REDISKEY_ZSET_ARTICLE_SCORE_ORDER = 'article:scoreorder'

REDISKEY_GROUP_PRE = 'group:'

conn = redis.Redis(host='192.168.0.226', port=6379, db=8)

def run(conn):
    post_article(conn=conn, user=1, title=1, link=1)

def printPretty(jsonString):
    print(json.dumps(jsonString, indent=4, sort_keys=True))

def generate_article_id(conn):
    if (conn.get(REDISKEY_ARTICLE_ID) == None):
        conn.set(REDISKEY_ARTICLE_ID, INIT_ARTICLE_ID)
        return INIT_ARTICLE_ID
    else:
        return conn.incr(REDISKEY_ARTICLE_ID)

def article_vote(conn, user, article):
    cutoff = time.time() - ONE_WEEK_IN_SECONDS

    if conn.zscore(REDISKEY_ZSET_ARTICLE_TIME_ORDER, article) < cutoff:
        return
    
    article_id = article.partition(':')[-1]
    if conn.sadd(REDISKEY_SET_VOTED_PRE + article_id, user):
        printPretty(conn.zincrby(REDISKEY_ZSET_ARTICLE_SCORE_ORDER, article, VOTE_SCORE))
        printPretty(conn.hincrby(article, 'votes', 1))


def post_article(conn, user, title, link):
    article_id = str(generate_article_id(conn))

    voted = REDISKEY_SET_VOTED_PRE + article_id
    conn.sadd(voted, user)
    conn.expire(voted, ONE_WEEK_IN_SECONDS)

    now = time.time()
    article = REDISKEY_HASH_ARTICLE_PRE + article_id
    conn.hmset(article, {
        'title' : title,
        'link' : link,
        'poster' : user,
        'time' : now,
        'votes' : 1
    })

    conn.zadd(REDISKEY_ZSET_ARTICLE_SCORE_ORDER, article, now + VOTE_SCORE)
    conn.zadd(REDISKEY_ZSET_ARTICLE_TIME_ORDER, article, now)

    init_group_data(conn=conn, article_id=article_id)

def get_articles(conn, page, order=REDISKEY_ZSET_ARTICLE_SCORE_ORDER):
    start = (page - 1) * ARTICLES_PER_PAGE
    end = start + ARTICLES_PER_PAGE - 1

    ids = conn.zrevrange(order, start, end)
    articles = []
    for id in ids:
        article_data = conn.hgetall(id)
        article_data['id'] = id
        articles.append(article_data)
    return articles

def add_remove_groups(conn, article_id, to_add=[], to_remove=[]):
    article = REDISKEY_HASH_ARTICLE_PRE + str(article_id)
    for group in to_add:
        conn.sadd(REDISKEY_GROUP_PRE + group, article)
    for group in to_remove:
        conn.srem(REDISKEY_GROUP_PRE + group , article)

def get_group_articles(conn, group, page, order=REDISKEY_ZSET_ARTICLE_SCORE_ORDER):
    key = order + group
    if not conn.exists(key):
        conn.zinterstore(key, 
            [REDISKEY_GROUP_PRE + group, order],
            aggregate = 'max',
        )
        conn.expire(key, 60)
    return get_articles(conn, page, key)

def init_group_data(conn, article_id):
    add_remove_groups(conn = conn, article_id = article_id, to_add = ['lizhen', 'aa'])

run(conn)

# article_vote(conn=conn, user=123, article=REDISKEY_HASH_ARTICLE_PRE + '10005')
# article_vote(conn=conn, user=time.time(), article=REDISKEY_HASH_ARTICLE_PRE + '10005')

# printPretty(get_articles(conn, 1))

# printPretty(get_group_articles(conn, 'aa', 1))

# init_group_data(conn)
articles = get_articles(conn=conn, page=1, order=REDISKEY_ZSET_ARTICLE_SCORE_ORDER)
printPretty(articles)
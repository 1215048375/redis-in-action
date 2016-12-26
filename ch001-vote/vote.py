import redis
import time
import json

INIT_ARTICLE_ID = 10000
ONE_WEEK_IN_SECONDS = 86400 * 7
VOTE_SCORE = 432

ARTICLES_PER_PAGE = 25

REDISKEY_ARTICLE_ID = 'article_id_incr'
REDISKEY_HASH_ARTICLE_PRE = 'article:'
REDISKEY_SET_VOTED_PRE = 'voted:'
REDISKEY_ZSET_ARTICLE_TIME_ORDER = 'article:timeorder'
REDISKEY_ZSET_ARTICLE_SCORE_ORDER = 'article:scoreorder'

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

run(conn)

articles = get_articles(conn=conn, page=1, order=REDISKEY_ZSET_ARTICLE_SCORE_ORDER)
printPretty(articles)
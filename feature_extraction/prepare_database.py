import csv
import logging
from collections import defaultdict
from multiprocessing.pool import Pool

import numpy
from mysql.connector import MySQLConnection
from progressbar import Bar, ProgressBar
from progressbar import Percentage
from scipy.sparse import csr_matrix
from sklearn.feature_extraction.text import TfidfVectorizer

from config import get_db_config

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(asctime)s:\t %(message)s', datefmt='%H:%M:%S')

gamma_a_set = ['80']
gamma_t_set = ['10', '20', '30', '40', '50', '60', '70', '80', '90', '100']

db = MySQLConnection(**get_db_config())
cursor = db.cursor()

t = 'ss'
common = '%s_common' % (t,)
twitter = '%s_twitter' % (t,)
fsquare = '%s_fsquare' % (t,)
similarity = '%s_sim' % (t,)
metapaths = '%s_metapaths' % (t,)


def execute_sim(query):
    db2 = MySQLConnection(**get_db_config(database=similarity))
    cursor2 = db2.cursor()
    cursor2.execute(query)


def execute_path(query):
    db2 = MySQLConnection(**get_db_config(database=metapaths))
    cursor2 = db2.cursor()
    cursor2.execute(query)


def drop_index():
    cursor.execute("USE %s" % metapaths)
    for s in range(0, 6):
        for ra in gamma_a_set:
            cursor.execute("ALTER TABLE source%d_%s DROP INDEX user1_2" % (s, ra))
            cursor.execute("ALTER TABLE source%d_%s DROP INDEX user2_2" % (s, ra))
            try:
                cursor.execute("ALTER TABLE target%d_%s DROP INDEX user1_2" % (s, ra))
                cursor.execute("ALTER TABLE target%d_%s DROP INDEX user2_2" % (s, ra))
            except:
                pass
    for s in range(6, 9):
        for ra in gamma_a_set:
            try:
                cursor.execute("ALTER TABLE target%d_%s_100 DROP INDEX user1_2" % (s, ra))
                cursor.execute("ALTER TABLE target%d_%s_100 DROP INDEX user2_2" % (s, ra))
            except:
                pass


def create_database():
    create_query = "CREATE DATABASE %s CHARACTER SET 'utf8' COLLATE 'utf8_unicode_ci'"
    cursor.execute(create_query % common)
    cursor.execute(create_query % twitter)
    cursor.execute(create_query % fsquare)
    cursor.execute(create_query % similarity)
    cursor.execute(create_query % metapaths)


def create_common():
    cursor.execute("USE %s" % common)

    query = "CREATE TABLE positive ROW_FORMAT = FIXED AS " \
            "SELECT t.username FROM all_foursquare.users f " \
            "JOIN all_twitter.users t ON f.twitter = t.username " \
            "AND f.createdat > t.createdat"
    cursor.execute(query)

    query = "CREATE TABLE anchor_100 ROW_FORMAT = FIXED AS " \
            "SELECT t.username AS twitter, f.id AS foursquare " \
            "FROM all_foursquare.users f " \
            "JOIN all_twitter.users t ON f.twitter = t.username " \
            "WHERE f.createdat < t.createdat " \
            "OR f.createdat IS NULL " \
            "OR t.createdat IS NULL"
    cursor.execute(query)

    query = "CREATE TABLE negative ROW_FORMAT = FIXED AS " \
            "SELECT t.username FROM all_twitter.users t " \
            "LEFT JOIN all_foursquare.users f ON f.twitter = t.username " \
            "WHERE f.twitter IS NULL"
    cursor.execute(query)

    query = "CREATE TABLE samples ROW_FORMAT = FIXED AS " \
            "SELECT username, 1 AS label FROM positive " \
            "UNION SELECT username, -1 AS label FROM negative"
    cursor.execute(query)

    cursor.execute("ALTER TABLE positive ADD PRIMARY KEY (username)")
    cursor.execute("ALTER TABLE negative ADD PRIMARY KEY (username)")
    cursor.execute("ALTER TABLE samples ADD PRIMARY KEY (username)")

    cursor.execute("SELECT count(*) FROM anchor_100")
    limit = cursor.fetchone()[0]
    x = limit // 10
    from_table = 'anchor_100'
    for gamma in reversed(gamma_a_set):
        table = 'anchor_%s' % gamma
        query = "CREATE TABLE IF NOT EXISTS %s ROW_FORMAT = FIXED AS " \
                "SELECT * FROM %s ORDER BY rand() LIMIT %d" % (table, from_table, limit)
        cursor.execute(query)
        cursor.execute("ALTER TABLE %s ADD PRIMARY KEY (twitter)" % table)
        cursor.execute("ALTER TABLE %s ADD INDEX (foursquare)" % table)
        from_table = table
        limit -= x


def create_twitter():
    cursor.execute("USE %s" % twitter)

    query = "CREATE TABLE users ROW_FORMAT = FIXED AS " \
            "SELECT username FROM %(db)s.samples " \
            "UNION SELECT twitter as username FROM %(db)s.anchor_100" % {'db': common}
    cursor.execute(query)

    query = "CREATE TABLE following ROW_FORMAT = FIXED AS " \
            "SELECT f.* FROM users u1 " \
            "JOIN all_twitter.following f ON u1.username = f.user1 " \
            "JOIN users u2 ON f.user2 = u2.username"
    cursor.execute(query)

    query = "CREATE TABLE user_tweet ROW_FORMAT=FIXED AS " \
            "SELECT ut.* FROM all_twitter.user_tweet AS ut " \
            "JOIN users USING (username);"
    cursor.execute(query)
    cursor.execute("ALTER TABLE user_tweet ADD INDEX (username);")
    cursor.execute("ALTER TABLE user_tweet ADD INDEX (tweet_id);")

    query = "CREATE TABLE tweets ROW_FORMAT = FIXED AS " \
            "SELECT t.*, u.username FROM all_twitter.tweets t " \
            "JOIN user_tweet AS u ON t.id = u.tweet_id"
    cursor.execute(query)
    cursor.execute("DROP TABLE user_tweet")

    query = "CREATE TABLE loc_tweets ROW_FORMAT = FIXED AS " \
            "SELECT id,username,lat,lng FROM tweets " \
            "WHERE lat IS NOT NULL AND lng IS NOT NULL;"
    cursor.execute(query)

    query = "CREATE TABLE time_tweets ROW_FORMAT = FIXED AS " \
            "SELECT id,username, time(time) AS time FROM tweets " \
            "WHERE time IS NOT NULL;"
    cursor.execute(query)

    cursor.execute("ALTER TABLE following ADD INDEX (user1)")
    cursor.execute("ALTER TABLE following ADD INDEX (user2)")
    cursor.execute("ALTER TABLE loc_tweets ADD INDEX (username)")
    cursor.execute("ALTER TABLE loc_tweets ADD INDEX (lat)")
    cursor.execute("ALTER TABLE loc_tweets ADD INDEX (lng)")
    cursor.execute("ALTER TABLE loc_tweets ADD INDEX (lat,lng)")
    cursor.execute("ALTER TABLE time_tweets ADD INDEX (username)")
    cursor.execute("ALTER TABLE time_tweets ADD INDEX (time)")
    cursor.execute("ALTER TABLE tweets ADD INDEX (username)")
    cursor.execute("ALTER TABLE users ADD PRIMARY KEY (username)")


def create_fsquare():
    cursor.execute("USE %s" % fsquare)

    query = "CREATE TABLE IF NOT EXISTS users ROW_FORMAT = FIXED AS " \
            "SELECT id, twitter, createdat FROM all_foursquare.users u " \
            "LEFT JOIN %s.positive p ON u.twitter = p.username " \
            "WHERE p.username is NULL " % common
    cursor.execute(query)
    cursor.execute("ALTER TABLE users ADD PRIMARY KEY (id)")
    cursor.execute("ALTER TABLE users ADD INDEX (twitter)")

    query = "CREATE TABLE IF NOT EXISTS following ROW_FORMAT = FIXED AS " \
            "SELECT f.* FROM users u1 " \
            "JOIN all_foursquare.following f ON u1.id = f.user1 " \
            "JOIN users u2 ON f.user2 = u2.id"
    cursor.execute(query)
    cursor.execute("ALTER TABLE following ADD INDEX (user1)")
    cursor.execute("ALTER TABLE following ADD INDEX (user2)")

    query = "CREATE TABLE IF NOT EXISTS temp1 ROW_FORMAT = FIXED AS " \
            "SELECT t.* FROM all_foursquare.tips t " \
            "JOIN all_foursquare.users u " \
            "ON t.user_id = u.id"
    cursor.execute(query)
    cursor.execute("ALTER TABLE temp1 ADD INDEX (user_id)")

    tips_query = "CREATE TABLE IF NOT EXISTS tips ROW_FORMAT = FIXED AS " \
                 "SELECT t.*, c.loc_id FROM temp1 AS t " \
                 "JOIN all_foursquare.tip_loc AS c ON t.id = c.tip_id"

    cursor.execute(tips_query)
    cursor.execute("DROP TABLE temp1")
    cursor.execute("ALTER TABLE tips MODIFY time VARCHAR(20);")
    cursor.execute("UPDATE tips SET time = date_format(time, '%H:%i');")
    cursor.execute("ALTER TABLE tips MODIFY time TIME;")

    cursor.execute("SELECT count(*) FROM tips")
    limit = cursor.fetchone()[0]
    x = limit // 10
    from_table = 'tips'
    for gamma in reversed(gamma_t_set):
        table = 'tips_%s' % gamma
        query = "CREATE TABLE IF NOT EXISTS %s ROW_FORMAT = FIXED AS " \
                "SELECT * FROM %s ORDER BY rand() LIMIT %d" % (table, from_table, limit)
        cursor.execute(query)
        cursor.execute("ALTER TABLE %s ADD INDEX (user_id)" % table)
        cursor.execute("ALTER TABLE %s ADD INDEX (time)" % table)
        cursor.execute("ALTER TABLE %s ADD INDEX (loc_id)" % table)
        from_table = table
        limit -= x

    cursor.execute("SELECT count(*) FROM following")
    limit = cursor.fetchone()[0]
    x = limit // 10
    from_table = 'following'
    for gamma in reversed(gamma_t_set):
        table = 'following_%s' % gamma
        query = "CREATE TABLE IF NOT EXISTS %s ROW_FORMAT = FIXED AS " \
                "SELECT * FROM %s ORDER BY rand() LIMIT %d" % (table, from_table, limit)
        cursor.execute(query)
        cursor.execute("ALTER TABLE %s ADD INDEX (user1)" % table)
        cursor.execute("ALTER TABLE %s ADD INDEX (user2)" % table)
        from_table = table
        limit -= x


def create_similarity():
    table_queries = []
    index_queries = []

    source2 = "create table if not EXISTS source2 row_format = fixed as " \
              "select f1.user1 as user1, f2.user2 as user2, count(*) as count " \
              "from %s.following f1 join %s.following f2 " \
              "on f1.user2 = f2.user1 group by user1, user2 having user1 <> user2;" % (twitter, twitter)

    source3 = "create table if not EXISTS source3 row_format = fixed as " \
              "select f1.user1 as user1, f2.user1 as user2, count(*) as count " \
              "from %s.following f1 join %s.following f2 " \
              "on f1.user2 = f2.user2 group by user1, user2 having f1.user1 <> user2;" % (twitter, twitter)

    source4 = "create table if not EXISTS source4 row_format = fixed as " \
              "select f1.user2 as user1, f2.user2 as user2, count(*) as count " \
              "from %s.following f1 join %s.following f2 " \
              "on f1.user1 = f2.user1 group by user1, user2 having user1 <> f2.user2;" % (twitter, twitter)

    source6 = "create table if not EXISTS source6 row_format = fixed as " \
              "select t1.username as user1, t2.username as user2, count(*) as count " \
              "from %s.loc_tweets t1 join %s.loc_tweets t2 " \
              "on t1.lat = t2.lat and t1.lng = t2.lng group by user1, user2 " \
              "having user1 <> user2;" % (twitter, twitter)

    source7 = "CREATE TABLE IF NOT EXISTS source7 (" \
              "user1 VARCHAR(16), user2 VARCHAR(16), count BIGINT(21)" \
              ")  ROW_FORMAT = FIXED "
    
    source8 = "CREATE TABLE IF NOT EXISTS source8 (" \
              "user1 VARCHAR(16), user2 VARCHAR(16), count DOUBLE" \
              ")  ROW_FORMAT = FIXED "

    table_queries.append(source2)
    table_queries.append(source3)
    table_queries.append(source4)
    table_queries.append(source6)
    table_queries.append(source7)
    table_queries.append(source8)

    index_queries.append("ALTER TABLE source2 ADD INDEX (user1);")
    index_queries.append("ALTER TABLE source2 ADD INDEX (user2);")
    index_queries.append("ALTER TABLE source3 ADD INDEX (user1);")
    index_queries.append("ALTER TABLE source3 ADD INDEX (user2);")
    index_queries.append("ALTER TABLE source4 ADD INDEX (user1);")
    index_queries.append("ALTER TABLE source4 ADD INDEX (user2);")
    index_queries.append("ALTER TABLE source6 ADD INDEX (user1);")
    index_queries.append("ALTER TABLE source6 ADD INDEX (user2);")
    index_queries.append("ALTER TABLE source7 ADD INDEX (user1);")
    index_queries.append("ALTER TABLE source7 ADD INDEX (user2);")
    index_queries.append("ALTER TABLE source8 ADD INDEX (user1);")
    index_queries.append("ALTER TABLE source8 ADD INDEX (user2);")

    index_queries.append("ALTER TABLE source2 ADD PRIMARY KEY (user1,user2);")
    index_queries.append("ALTER TABLE source3 ADD PRIMARY KEY (user1,user2);")
    index_queries.append("ALTER TABLE source4 ADD PRIMARY KEY (user1,user2);")
    index_queries.append("ALTER TABLE source6 ADD PRIMARY KEY (user1,user2);")
    index_queries.append("ALTER TABLE source7 ADD PRIMARY KEY (user1,user2);")
    index_queries.append("ALTER TABLE source8 ADD PRIMARY KEY (user1,user2);")

    for gamma in gamma_t_set:
        target2 = "create table if not EXISTS target2_%(rf)s row_format = fixed as " \
                  "select f1.user1 as user1, f2.user2 as user2, count(*) as count " \
                  "from %(db)s.following_%(rf)s f1 join %(db)s.following_%(rf)s f2 " \
                  "on f1.user2 = f2.user1 group by user1, user2 having user1 <> user2;" % {'rf': gamma, 'db': fsquare}

        target3 = "create table if not EXISTS target3_%(rf)s row_format = fixed as " \
                  "select f1.user1 as user1, f2.user1 as user2, count(*) as count " \
                  "from %(db)s.following_%(rf)s f1 join %(db)s.following_%(rf)s f2 " \
                  "on f1.user2 = f2.user2 group by user1, user2 having f1.user1 <> user2;" % {'rf': gamma, 'db': fsquare}

        target4 = "create table if not EXISTS target4_%(rf)s row_format = fixed as " \
                  "select f1.user2 as user1, f2.user2 as user2, count(*) as count " \
                  "from %(db)s.following_%(rf)s f1 join %(db)s.following_%(rf)s f2 " \
                  "on f1.user1 = f2.user1 group by user1, user2 having user1 <> f2.user2;" % {'rf': gamma, 'db': fsquare}

        target6 = "create table if not EXISTS target6_%(r)s row_format = fixed as " \
                  "select t1.user_id as user1, t2.user_id as user2, count(*) as count " \
                  "from %(db)s.tips_%(r)s t1 join %(db)s.tips_%(r)s t2 " \
                  "on t1.loc_id = t2.loc_id group by user1, user2 having user1 <> user2;" % {'r': gamma, 'db': fsquare}

        target7 = "CREATE TABLE if not EXISTS target7_%s (" \
                  "user1 VARCHAR(64), user2 VARCHAR(64), count BIGINT(21)" \
                  ")  row_format = fixed " % gamma

        target8 = "CREATE TABLE if not EXISTS target8_%s (" \
                  "user1 VARCHAR(64), user2 VARCHAR(64), count double" \
                  ")  row_format = fixed " % gamma

        table_queries.append(target2)
        table_queries.append(target3)
        table_queries.append(target4)
        table_queries.append(target6)
        table_queries.append(target7)
        table_queries.append(target8)

        index_queries.append("ALTER TABLE target2_%s ADD INDEX (user1);" % gamma)
        index_queries.append("ALTER TABLE target2_%s ADD INDEX (user2);" % gamma)
        index_queries.append("ALTER TABLE target3_%s ADD INDEX (user1);" % gamma)
        index_queries.append("ALTER TABLE target3_%s ADD INDEX (user2);" % gamma)
        index_queries.append("ALTER TABLE target4_%s ADD INDEX (user1);" % gamma)
        index_queries.append("ALTER TABLE target4_%s ADD INDEX (user2);" % gamma)
        index_queries.append("ALTER TABLE target2_%s ADD PRIMARY KEY (user1,user2);" % gamma)
        index_queries.append("ALTER TABLE target3_%s ADD PRIMARY KEY (user1,user2);" % gamma)
        index_queries.append("ALTER TABLE target4_%s ADD PRIMARY KEY (user1,user2);" % gamma)
        index_queries.append("alter table target6_%s add index (user1);" % gamma)
        index_queries.append("alter table target6_%s add index (user2);" % gamma)
        index_queries.append("alter table target6_%s add primary key (user1,user2);" % gamma)

    with Pool(8) as p:
        p.map(execute_sim, table_queries)
        p.map(execute_sim, index_queries)

    time_similarity(twitter, 'time_tweets', 'username', 'source7')

    for gamma in gamma_t_set:
        time_similarity(fsquare, 'tips_%s' % gamma, 'user_id', 'target7_%s' % gamma)

    for gamma in gamma_t_set:
        post_similarity(fsquare, 'tips_%s' % gamma, 'user_id', 'target8_%s' % gamma)


def time_similarity(database, table, field, target_table):
    db2 = MySQLConnection(**get_db_config(database=database))
    cursor2 = db2.cursor()

    query = "SELECT GROUP_CONCAT( DISTINCT %s ) FROM %s GROUP BY time" % (field, table)
    cursor2.execute(query)
    results = cursor2.fetchall()

    time_sim = defaultdict(dict)
    for row in results:
        reader = csv.reader([row[0]])
        values = reader.__next__()
        for user1 in values:
            for user2 in values:
                if user1 != user2:
                    if (user1, user2) in time_sim:
                        time_sim[user1, user2] += 1
                    else:
                        time_sim[user1, user2] = 1

	query = 'INSERT INTO ' + target_table + '(user1, user2, count) VALUES '
	for user, count in time_sim.items():
		query += ','.join(['(%s,%s,%d)' % (user[0], user[1], count) for user, count in time_sim.items()])
	
	query += ';'
    # with open('timesim_%s_%s' % (database, table), mode='w') as f:
        # for user, count in time_sim.items():
            # f.write('%s,%s,%d\n' % (user[0], user[1], count))
	execute_sim(query)
    logging.info('Time similarity done. Database: %s, Table: %s' % (database, table))


def post_similarity(database, table, field, target_table):
    logging.info('Post similarity for database: %s, table: %s' % (database, table))
    db2 = MySQLConnection(**get_db_config(database=database))
    cursor2 = db2.cursor()

    logging.info('Connected to Database. Executing query...')

    query = "SELECT %s, text FROM %s" % (field, table)
    cursor2.execute(query)

    logging.info('Query Executed. Processing Posts...')

    users = []
    documents = []

    while True:
        try:
            row = cursor2.fetchone()
            if row is None:
                break
            username = row[0]
            text = row[1]
            post = text.replace('_', ' ')
            users.append(username)
            documents.append(post)
        except:
            pass

    logging.info('Documents are ready. Running TF/IDF analysis...')

    vect = TfidfVectorizer()
    tfidf = vect.fit_transform(documents)

    logging.info('TF/IDF ready. Initializing...')

    # savemat('tf-idf_' + database, {'tf_idf': tfidf, 'users': users})
    #
    # logging.info('Initializing...')

    widget = [Bar('=', '[', ']'), ' ', Percentage()]
    bar = ProgressBar(maxval=len(users), widgets=widget)

    rows, cols = tfidf.shape
    simdic = {}
    x = csr_matrix((1, cols), dtype=numpy.double)

    bar.start()
    index = 1
    for u in users:
        simdic[u] = x
        bar.update(index)
        index += 1

    bar.finish()
    logging.info('Summing results for each user...')

    bar = ProgressBar(maxval=rows, widgets=widget)
    bar.start()
    for i in range(rows):
        r = tfidf.getrow(i)
        u = users[i]
        simdic[u] = simdic[u] + r
        bar.update(i + 1)

    bar.finish()

    # logging.info('Saving partial results...')
    # try:
    #     numpy.savez_compressed('user-post-tfidf_%s_%s' % (database, table), simdic=simdic)
    # except:
    #     pass

    logging.info('Calculating similarities...')

	query = 'INSERT INTO ' + target_table + '(user1, user2, count) VALUES '
    bar = ProgressBar(maxval=len(simdic) ** 2, widgets=widget)
    bar.start()
    index = 1
    # with open('postsim_%s_%s' % (database, table), mode='w') as f:
	items = simdic.items()
	for u, x in items:
		for v, y in items:
			if u != v:
				sim = x * y.T
				if sim > 0:
					query += '(%s,%s,%f),' % (u, v, sim[0, 0])
			bar.update(index)
			index += 1

    bar.finish()
	if query[-1] == ',':
		query = query[:-1]+';'
	execute_sim(query)
    logging.info('Post similarity done. Database: %s, Table: %s' % (database, table))


def fix_similarity():
    cursor.execute("USE %s" % similarity)

    for gamma in gamma_t_set:
        cursor.execute("UPDATE target8_%s SET count = round(count)" % gamma)
        cursor.execute("ALTER TABLE target8_%s MODIFY count INTEGER;" % gamma)
        cursor.execute("DELETE FROM target8_%s WHERE count = 0;" % gamma)
        cursor.execute("alter table target7_%s add index (user1);" % gamma)
        cursor.execute("alter table target7_%s add index (user2);" % gamma)
        cursor.execute("alter table target8_%s add index (user1);" % gamma)
        cursor.execute("alter table target8_%s add index (user2);" % gamma)
        cursor.execute("alter table target7_%s add primary key (user1,user2);" % gamma)
        cursor.execute("alter table target8_%s add primary key (user1,user2);" % gamma)


def create_metapaths():
    table_queries = []
    index_queries = []

    for gamma_a in gamma_a_set:
        source0 = "CREATE TABLE IF NOT EXISTS source0_%(r)s ROW_FORMAT = FIXED AS " \
                  "SELECT u.username as user1, a.twitter as user2, 1 AS count " \
                  "FROM %(t)s_common.samples AS u " \
                  "JOIN %(t)s_twitter.following AS f ON u.username = f.user1 " \
                  "JOIN %(t)s_common.anchor_%(r)s AS a ON f.user2 = a.twitter;" % {'t': t, 'r': gamma_a}
        table_queries.append(source0)
        index_queries.append("ALTER TABLE source0_%(r)s ADD INDEX (user1);" % {'r': gamma_a})
        index_queries.append("ALTER TABLE source0_%(r)s ADD INDEX (user2);" % {'r': gamma_a})
        
        source1 = "CREATE TABLE IF NOT EXISTS source1_%(r)s ROW_FORMAT = FIXED AS " \
                  "SELECT u.username as user1, a.twitter as user2, 1 AS count " \
                  "FROM %(t)s_common.samples AS u " \
                  "JOIN %(t)s_twitter.following AS f ON u.username = f.user2 " \
                  "JOIN %(t)s_common.anchor_%(r)s AS a ON f.user1 = a.twitter;" % {'t': t, 'r': gamma_a}
        table_queries.append(source1)
        index_queries.append("ALTER TABLE source1_%(r)s ADD INDEX (user1);" % {'r': gamma_a})
        index_queries.append("ALTER TABLE source1_%(r)s ADD INDEX (user2);" % {'r': gamma_a})
        
        source5 = "CREATE TABLE IF NOT EXISTS source5_%(r)s ROW_FORMAT = FIXED AS " \
                  "SELECT u.username as user1, a.twitter as user2, f.count as count " \
                  "FROM %(t)s_common.samples AS u " \
                  "JOIN %(t)s_sim.source2 AS f ON u.username = f.user2 " \
                  "JOIN %(t)s_common.anchor_%(r)s AS a ON f.user1 = a.twitter;" % {'t': t, 'r': gamma_a}
        table_queries.append(source5)
        index_queries.append("ALTER TABLE source5_%(r)s ADD INDEX (user1);" % {'r': gamma_a})
        index_queries.append("ALTER TABLE source5_%(r)s ADD INDEX (user2);" % {'r': gamma_a})
        
        for s in [2, 3, 4, 6, 7]:
            source_s = "CREATE TABLE IF NOT EXISTS source%(s)s_%(r)s ROW_FORMAT = FIXED AS " \
                       "SELECT u.username as user1, a.twitter as user2 , f.count as count " \
                       "FROM %(t)s_common.samples AS u " \
                       "JOIN %(t)s_sim.source%(s)s AS f ON u.username = f.user1 " \
                       "JOIN %(t)s_common.anchor_%(r)s AS a ON f.user2 = a.twitter;" % {'s': s, 't': t, 'r': gamma_a}
            table_queries.append(source_s)
            index_queries.append("ALTER TABLE source%(s)s_%(r)s ADD INDEX (user1);" % {'r': gamma_a, 's': s})
            index_queries.append("ALTER TABLE source%(s)s_%(r)s ADD INDEX (user2);" % {'r': gamma_a, 's': s})

        gamma_set = gamma_t_set if gamma_a == '80' else ['100']
        # gamma_t_set = ['100']
        for gamma_t in gamma_set:
            target0 = "CREATE TABLE IF NOT EXISTS target0_%(r)s_%(f)s ROW_FORMAT = FIXED AS " \
                      "SELECT a1.twitter as user1, a2.twitter as user2, 1 AS count " \
                      "FROM %(t)s_common.anchor_%(r)s AS a1 " \
                      "JOIN %(t)s_fsquare.following_%(f)s AS f ON a1.foursquare = f.user1 " \
                      "JOIN %(t)s_common.anchor_%(r)s AS a2 ON f.user2 = a2.foursquare;" % {'t': t, 'r': gamma_a,
                                                                                            'f': gamma_t}
            table_queries.append(target0)
            index_queries.append("ALTER TABLE target0_%(r)s_%(f)s ADD INDEX (user1);" % {'r': gamma_a, 'f': gamma_t})
            index_queries.append("ALTER TABLE target0_%(r)s_%(f)s ADD INDEX (user2);" % {'r': gamma_a, 'f': gamma_t})

            target1 = "CREATE TABLE IF NOT EXISTS target1_%(r)s_%(f)s ROW_FORMAT = FIXED AS " \
                      "SELECT a1.twitter as user1, a2.twitter as user2, 1 AS count " \
                      "FROM %(t)s_common.anchor_%(r)s AS a1 " \
                      "JOIN %(t)s_fsquare.following_%(f)s AS f ON a1.foursquare = f.user2 " \
                      "JOIN %(t)s_common.anchor_%(r)s AS a2 ON f.user1 = a2.foursquare;" % {'t': t, 'r': gamma_a,
                                                                                            'f': gamma_t}
            table_queries.append(target1)
            index_queries.append("ALTER TABLE target1_%(r)s_%(f)s ADD INDEX (user1);" % {'r': gamma_a, 'f': gamma_t})
            index_queries.append("ALTER TABLE target1_%(r)s_%(f)s ADD INDEX (user2);" % {'r': gamma_a, 'f': gamma_t})

            target5 = "CREATE TABLE IF NOT EXISTS target5_%(r)s_%(f)s ROW_FORMAT = FIXED AS " \
                      "SELECT a1.twitter as user1, a2.twitter as user2, f.count as count " \
                      "FROM %(t)s_common.anchor_%(r)s AS a1 " \
                      "JOIN %(t)s_sim.target2_%(f)s AS f ON a1.foursquare = f.user2 " \
                      "JOIN %(t)s_common.anchor_%(r)s AS a2 ON f.user1 = a2.foursquare;" % {'t': t, 'r': gamma_a,
                                                                                            'f': gamma_t}
            table_queries.append(target5)
            index_queries.append("ALTER TABLE target5_%(r)s_%(f)s ADD INDEX (user1);" % {'r': gamma_a, 'f': gamma_t})
            index_queries.append("ALTER TABLE target5_%(r)s_%(f)s ADD INDEX (user2);" % {'r': gamma_a, 'f': gamma_t})

            for s in [2, 3, 4, 6, 7, 8]:
                target_f_s = "CREATE TABLE IF NOT EXISTS target%(s)s_%(r)s_%(f)s ROW_FORMAT = FIXED AS " \
                             "SELECT a1.twitter as user1, a2.twitter as user2, f.count as count " \
                             "FROM %(t)s_common.anchor_%(r)s AS a1 " \
                             "JOIN %(t)s_sim.target%(s)s_%(f)s AS f ON a1.foursquare = f.user1 " \
                             "JOIN %(t)s_common.anchor_%(r)s AS a2 ON f.user2 = a2.foursquare;" % {'s': s, 't': t,
                                                                                                   'r': gamma_a,
                                                                                                   'f': gamma_t}
                table_queries.append(target_f_s)
                index_queries.append(
                    "ALTER TABLE target%(s)s_%(r)s_%(f)s ADD INDEX (user1);" % {'r': gamma_a, 's': s, 'f': gamma_t})
                index_queries.append(
                    "ALTER TABLE target%(s)s_%(r)s_%(f)s ADD INDEX (user2);" % {'r': gamma_a, 's': s, 'f': gamma_t})

    with Pool(8) as p:
        p.map(execute_path, table_queries)
        p.map(execute_path, index_queries)


def main():
    create_database()
    create_common()
    create_twitter()
    create_fsquare()
    create_similarity()
    fix_similarity()
    create_metapaths()
    pass


if __name__ == '__main__':
    main()

import logging
import threading

import numpy
from mysql.connector import MySQLConnection
from progressbar import *
from scipy.io import savemat

from config import get_db_config

tsplit = 'ss'
gamma_a = '100'
gamma_t = '100'

SOURCE = 8
TARGET = 9
TOTAL = SOURCE + SOURCE * TARGET * SOURCE

features = []
samples = []

db_metapath = '%s_metapaths' % tsplit
db_common = '%s_common' % tsplit


class CounterThread(threading.Thread):
    def __init__(self, findex, begin, end):
        threading.Thread.__init__(self)
        self.begin = begin
        self.end = end
        self.feature_index = findex
        self.samples = samples[begin:end]
        self.db = MySQLConnection(**get_db_config(database=db_metapath))

    def run(self):
        self.connector_path_count()
        self.recursive_path_count()

    def connector_path_count(self):
        cursor = self.db.cursor()
        for f in range(self.feature_index, SOURCE):
            query = "SELECT SUM(count) FROM source%d_%s WHERE user1 = ?" % (f, gamma_a)
            cursor.execute('PREPARE stmt FROM %s', (query,))

            user_index = self.begin
            for username in self.samples:
                cursor.execute('SET @username = %s', (username,))
                cursor.execute('EXECUTE stmt USING @username')
                count = cursor.fetchone()[0]
                if count is None:
                    count = 0
                features[user_index, self.feature_index] = count
                user_index += 1

            cursor.execute('DEALLOCATE PREPARE stmt')
            self.feature_index += 1

    @staticmethod
    def get_index(f):
        fn = f - SOURCE
        k = fn % TARGET
        j = (fn // TARGET) % SOURCE
        i = (fn // (TARGET * SOURCE))
        return i, j, k

    def recursive_path_count(self):
        cursor = self.db.cursor()

        for f in range(self.feature_index, TOTAL):
            i, j, k = self.get_index(f)

            # if k < 6:
            #     target = 'target%d_%s' % (k, gamma_a)
            # else:
            target = 'target%d_%s_%s' % (k, gamma_a, gamma_t)

            # query = "SELECT SUM(x.count * y.count * z.count) FROM source%d_%s AS x " \
            #         "JOIN %s AS y ON x.user2 = y.user1 AND x.user1 = ? " \
            #         "JOIN source%d_%s AS z ON y.user2 = z.user2 AND z.user1 = ?;" % (i, gamma_a, target, j, gamma_a)
            #
            subquery1 = "SELECT user2, count FROM source%d_%s  WHERE user1 = ?" % (i, gamma_a)
            subquery2 = "SELECT user2, count FROM source%d_%s  WHERE user1 = ?" % (j, gamma_a)
            query = "SELECT SUM(x.count * y.count * z.count) FROM (%s) x " \
                    "JOIN %s y ON x.user2 = y.user1 " \
                    "JOIN (%s) z ON y.user2 = z.user2" % (subquery1, target, subquery2)

            cursor.execute('PREPARE stmt FROM %s', (query,))

            user_index = self.begin
            for username in self.samples:
                cursor.execute('SET @username = %s', (username,))
                cursor.execute('EXECUTE stmt USING @username, @username')
                count = cursor.fetchone()[0]
                if count is None:
                    count = 0
                features[user_index, self.feature_index] = count
                user_index += 1

            cursor.execute('DEALLOCATE PREPARE stmt')
            self.feature_index += 1


def check_progress(findex, threads):
    widget = [Bar('=', '[', ']'), ' ', Percentage()]
    bar = ProgressBar(maxval=TOTAL, widgets=widget)
    bar.start()
    for i in range(findex + 1, TOTAL + 1):
        for t in threads:
            while t.feature_index < i:
                time.sleep(1)
        s1, s2, t = CounterThread.get_index(i - 1)
        logging.info('Feature #%d Done. Path: %d-%d-%d, gamma_a: %s, gamma_t: %s' % (i, s1, t, s2, gamma_a, gamma_t))
        bar.update(i)
        # savemat('dataset_partial', {'X': features, 'fc': i})


def main():
    global samples
    global features
    global gamma_a
    global gamma_t

    logging.basicConfig(level=logging.INFO, format='[%(asctime)s: %(message)s', datefmt='%H:%M:%S')

    start = time.time()

    db = MySQLConnection(**get_db_config(database=db_common))
    cursor = db.cursor()
    cursor.execute('SELECT username,label FROM samples')
    result = cursor.fetchall()

    labels = []
    for row in result:
        username = row[0]
        label = int(row[1])
        samples.append(username)
        labels.append(label)

    n = len(samples)
    m = 8  # number of threads

    # try:
    #     saved = loadmat('dataset_partial')
    #     findex = saved['fc']
    #     features = saved['X']
    #     logging.info('Last saved results loaded. continue from %d' % (findex+1,))
    # except FileNotFoundError:
    #     features = numpy.zeros([n, TOTAL])
    #     findex = 0
    #     logging.info('Starting from scratch...')

    # gamma_a_set = range(100, 9, -10)
    gamma_a_set = ['80']
    gamma_t_set = range(90, 9, -10)
    for gamma_a in gamma_a_set:
        # gamma_t_set = rho_set if gamma_a == '100' else ['100']
        # gamma_t_set = ['100']
        for gamma_t in gamma_t_set:
            features = numpy.zeros([n, TOTAL])
            findex = 0
            logging.info('Extracting Features for gamma_a=%s , gamma_t=%s' % (gamma_a, gamma_t))

            threads = []

            for i in range(0, n, n // m):
                threads.append(CounterThread(findex, i, i + n // m))

            for t in threads:
                t.start()

            check_progress(findex, threads)

            for t in threads:
                t.join()

            logging.info('Features Extraction Done. Saving Final Results...')
            savemat('dataset_%s_%s_%s' % (tsplit, gamma_a, gamma_t), {'X': features, 'Y': labels})
            end = time.time()
            logging.info('All Done in %d seconds.' % (end - start,))


if __name__ == '__main__':
    main()

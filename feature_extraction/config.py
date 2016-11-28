from configparser import ConfigParser


def get_db_config(filename='config.ini', section='mysql', database=None):
    parser = ConfigParser()
    parser.read(filename)

    db = {}
    items = parser.items(section)
    for item in items:
        db[item[0]] = item[1]

    if database is not None:
        db['database'] = database

    return db
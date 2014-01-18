from wikicities.DB import LoadBalancer
import numpy


yml = '/usr/wikia/conf/current/DB.yml'


def get_config():
    return yml


def get_local_db_by_name(name, global_db):
    """ Allows us to load in the local DB name from one or more options
    :param options: the 0th result of OptionParser.parse_args()
    """
    cursor = global_db.cursor()
    sql = 'SELECT city_id, city_dbname FROM city_list WHERE city_dbname = "%s"' % name
    results = cursor.execute(sql)
    result = cursor.fetchone()
    if not result:
        raise ValueError("No wiki found")
    return get_local_db_from_wiki_id(result)


def get_global_db(master=False):
    lb = LoadBalancer(get_config())
    return lb.get_db_by_name('wikicities', master=master)


def get_local_db_from_wiki_id(global_db, wiki_id, master=False):
    cursor = get_global_db().cursor()
    sql = "SELECT city_id, city_dbname FROM city_list WHERE city_id = %s" % str(wiki_id)
    results = cursor.execute(sql)
    result = cursor.fetchone()
    if not result:
        raise ValueError("No wiki found")

    return LoadBalancer(get_config()).get_db_by_name(result[1], master=master)


class DriverMySQL:

    def __init__(self, dbname):
        self.db = get_local_db_by_name(dbname, get_global_db())

    def is_author_anonymous(self, author_id):
        """
        Got an ID, don't it?!
        """
        return author_id != 0

    def shortest_page_paths_for_author(self, author_id):
        pass

    def all_shortest_page_paths_by_author(self):
        pass

    def words_deleted(self, revision_id_a, revision_id_b):
        pass

    def words_added(self, revision_id_a, revision_id_b):
        pass

    def words_moved(self, revision_id_a, revision_id_b):
        pass

    def revision_children(self, revision_id):
        pass

    def author_for_revision(self, revision_id):
        pass

    def page_revisions_for_author(self, page_id, author_id):
        pass

    def page_authors(self, page_id):
        pass

    def revisions_for_page(self, page_id):
        pass

    def shortest_paths_for_page(self, page_id):
        pass

    def all_shortest_page_paths(self):
        pass
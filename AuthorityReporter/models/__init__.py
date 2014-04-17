from wikia_dstk.authority import get_db_and_cursor
from nlp_services.authority import WikiAuthorityService
from nlp_services.authority import WikiAuthorTopicAuthorityService, WikiAuthorsToIdsService
from nlp_services.authority import WikiTopicsToAuthorityService
from collections import defaultdict, OrderedDict
from nlp_services.caching import use_caching
from multiprocessing import Pool
import requests
import xlwt
from collections import defaultdict
from nlp_services.caching import use_caching
from nlp_services.pooling import set_global_num_processes
from nlp_services.authority import WikiAuthorityService, WikiTopicsToAuthorityService, WikiAuthorTopicAuthorityService
from nlp_services.authority import WikiAuthorsToIdsService


class BaseModel():
    """
    Base class for models
    """

    def __init__(self, args):
        """
        Initializes db and cursor
        :param args: a namespace object with db connection data
        :type args:class:`argparse.Namespace`
        """
        self.db, self.cursor = get_db_and_cursor(args)


def get_page_response(tup):
    current_url, ids = tup
    response = requests.get(u'%s/api/v1/Articles/Details' % current_url, params=dict(ids=u','.join(ids)))
    return current_url, dict(response.json().get(u'items', {}))


class TopicModel(BaseModel):

    """
    Provides logic for interacting with a given topic
    """

    def __init__(self, topic, args):
        """
        Init method
        :param topic: the topic
        :type topic: str
        :param args: the argparse namespace w/ db info
        :type args:class:`argparse.Namespace`
        """
        self.topic = topic
        BaseModel.__init__(self, args)

    def get_pages(self, limit=10):
        """
        Gets most authoritative pages for a topic using Authority DB and Wikia API data
        :param limit: Number of results we want
        :type limit: int
        :return: a list of objects reflecting page results
        :rtype: list
        """

        self.cursor.execute(u"""
    SELECT wikis.url, wikis.title, wikis.wiki_id, articles.article_id, articles.global_authority  AS auth
    FROM topics INNER JOIN articles_topics ON topics.name = '%s' AND topics.topic_id = articles_topics.topic_id
    INNER JOIN articles ON articles.article_id = articles_topics.article_id
                           AND articles.wiki_id = articles_topics.wiki_id
    INNER JOIN wikis ON wikis.wiki_id = articles.wiki_id
    ORDER BY auth DESC
    LIMIT %d
    """ % (self.db.escape_string(self.topic), limit))
        ordered_db_results = [(y[0], y[1], unicode(y[2]), unicode(y[3]), y[4]) for y in self.cursor.fetchall()]
        url_to_ids = defaultdict(list)
        map(lambda x: url_to_ids[x[0]].append(x[3]), ordered_db_results)

        url_to_articles = dict(Pool(processes=8).map_async(get_page_response, list(url_to_ids.items())).get())

        ordered_page_results = []
        for url, wiki_name, wiki_id, page_id, authority in ordered_db_results:
            result = dict(base_url=url, **url_to_articles[url].get(unicode(page_id), {}))
            result[u'full_url'] = (result.get(u'base_url', '').strip(u'/') + result.get(u'url', ''))
            result[u'wiki'] = wiki_name
            result[u'authority'] = authority
            result[u'wiki_id'] = wiki_id
            result[u'page_id'] = page_id
            ordered_page_results.append(result)

        return ordered_page_results

    def get_wikis(self, limit=10):
        """
        Gets wikis for the current topic
        :param limit: the number of wikis we want
        :type limit: int
        :return: a dict with keys for wikis (objects) and wiki ids (ints) for ordering
        :rtype: dict
        """
        self.cursor.execute(u"""
SELECT wikis.wiki_id, SUM(articles.global_authority) AS total_auth
FROM topics
  INNER JOIN articles_topics ON topics.name = '%s' AND topics.topic_id = articles_topics.topic_id
  INNER JOIN articles ON articles.article_id = articles_topics.article_id AND articles.wiki_id = articles_topics.wiki_id
  INNER JOIN wikis ON articles.wiki_id = wikis.wiki_id
  GROUP BY articles.wiki_id ORDER BY total_auth DESC LIMIT %d
    -- selects the best wikis for a given topic
                        """ % (self.db.escape_string(self.topic), limit))

        wids_to_auth = OrderedDict([(row[0], row[1]) for row in self.cursor.fetchall()])
        wiki_ids = map(str, wids_to_auth.keys())

        result = requests.get(u'http://www.wikia.com/api/v1/Wikis/Details',
                              params=dict(ids=u','.join(wiki_ids)))

        wikis = result.json().get(u'items', {})
        for wid, auth in wids_to_auth.items():
            wikis[unicode(wid)][u'authority'] = auth

        return dict(wikis=wikis, wiki_ids=wiki_ids)

    def get_users(self, limit=10, with_api=True):
        """
        Gets users for a given topic
        :param limit: the number of users we want
        :type limit: int
        :return: a list of objects related to authors
        :rtype: list
        """

        self.cursor.execute(u"""
SELECT users.user_id, users.user_name, SUM(articles_users.contribs * articles.global_authority) AS auth
FROM topics
  INNER JOIN articles_topics ON topics.name = '%s' AND topics.topic_id = articles_topics.topic_id
  INNER JOIN articles_users ON articles_topics.article_id = articles_users.article_id
                               AND articles_topics.wiki_id = articles_users.wiki_id
  INNER JOIN articles ON articles.article_id = articles_users.article_id AND articles.wiki_id = articles_users.wiki_id
  INNER JOIN users ON articles_users.user_id = users.user_id
GROUP BY users.user_id
ORDER BY auth DESC
LIMIT %d
-- selects the most influential authors for a given topic
    """ % (self.db.escape_string(self.topic), limit))

        user_data = self.cursor.fetchall()

        user_api_data = []

        if with_api:
            for i in range(0, limit, 25):
                response = requests.get(u'http://www.wikia.com/api/v1/User/Details',
                                        params={u'ids': u','.join([str(x[0]) for x in user_data[i:i+25]])})

                user_api_data += response.json()[u'items']

        id_to_auth = OrderedDict([(x[0], {u'id': x[0], u'user_name': x[1], u'total_authority': x[2]})
                                  for x in user_data])
        author_objects = []
        if with_api:
            for obj in user_api_data:
                obj[u'total_authority'] = id_to_auth[obj[u'user_id']][u'total_authority']
                author_objects.append(obj)
        else:
            author_objects = id_to_auth.values()

        return author_objects


class WikiModel(BaseModel):
    """
    Logic for a given wiki
    """
    def __init__(self, wiki_id, args):
        self.wiki_id = wiki_id
        self.args = args  # stupid di
        self._api_data = None
        BaseModel.__init__(self, args)

    @property
    def api_data(self):
        """
        Memoized lazy-loaded property access
        :return: dict of api data
        :rtype: dict
        """
        if not self._api_data:
            self._api_data = requests.get(u'http://www.wikia.com/api/v1/Wikis/Details',
                                          params=dict(ids=self.wiki_id)).json()[u'items'][self.wiki_id]
        return self._api_data

    def get_topics(self, limit=10):
        """
        Get topics for this wiki
        :param limit: number of topics to get
        :type limit: int
        :return: a list of dicts
        :rtype: list
        """

        sql = u"""
        SELECT topic.name, SUM(articles.local_authority) AS authority
        FROM articles_topics art INNER JOIN topics ON art.wiki_id = %d AND topics.topic_id = art.topic__id
        INNER JOIN articles ON art.wiki_id = articles.wiki_id AND articles.article_id = art.article_id
        GROUP BY topics.topic_id
        ORDER BY authority
        """

        if limit:
            sql += u" LIMIT %d" % limit

        self.cursor.execute(sql)

        results = [dict(topic=x[0], authority=x[1]) for x in self.cursor.fetchall()]
        if limit:
            for result in results:
                result[u'authors'] = TopicModel(result[u'topic'], self.args).get_users(limit=5)

        return results

    def get_all_authors(self):
        """
        Optimized to get all authors
        :return: an OrderedDict with author dicts
        :rtype:class:`collections.OrderedDict`
        """
        self.cursor.execute(u"""
        SELECT users.user_id, users.user_name, SUM(articles.local_authority) AS total_authority
        FROM users INNER JOIN articles_users ON articles_users.wiki_id = %d AND users.user_id = articles_users.user_id
                   INNER JOIN articles on articles.article_id = articles_users.article_id AND articles.wiki_id = %d
        GROUP BY users.user_id
        ORDER BY total_authority DESC""" % (self.wiki_id, self.wiki_id))

        return OrderedDict([(x[0], dict(id=x[0], name=x[1], total_authority=x[2])) for x in self.cursor.fetchall()])

    def get_authors(self, limit=10):
        """
        Provides the top authors for a wiki
        :param limit: number of authors you want
        :type limit: int
        :return: list of author dicts
        :rtype: list
        """

        sql = u"""
        SELECT users.user_id, users.user_name, SUM(articles.local_authority * au.contribs) AS total_authority
        FROM users INNER JOIN articles_users au ON au.wiki_id = %d AND au.user_id = users.user_id
                   INNER JOIN articles ON articles.article_id = au.article_id AND articles.wiki_id = %d
        GROUP BY users.user_id
        ORDER BY total_authority DESC
        """

        if limit:
            sql += u" LIMIT %d" % limit

        authors_dict = OrderedDict([(x[0], dict(id=x[0], name=x[1], total_authority=x[2]))
                                    for x in self.cursor.fetchall(sql)])

        user_api_data = requests.get(self.api_data[u'url']+u'/api/v1/User/Details',
                                     params={u'ids': u",".join(map(str, authors_dict.keys())),
                                             u'format': u'json'}).json()[u'items']

        if limit:
            for user_data in user_api_data:
                authors_dict[user_data[u'name']].update(user_data)
                authors_dict[user_data[u'name']][u'url'] = authors_dict[user_data[u'name']][u'url'][1:]

        return authors_dict.values()

    def get_pages(self, limit=10):
        """
        Gets most authoritative pages for this wiki
        :param limit: the number of pages you want
        :type limit: int
        :return: a list of page objects
        :rtype: list
        """
        self.cursor.execute(u"""SELECT article_id, local_authority FROM articles
                           WHERE wiki_id = %s ORDER BY local_authority DESC LIMIT %d""" % (self.wiki_id, limit))
        id_to_authority = [(row[0], row[1]) for row in self.cursor.fetchall()]

        response = requests.get(self.api_data[u'url']+u'api/v1/Articles/Details',
                                params=dict(ids=u','.join([str(a[0]) for a in id_to_authority])))

        page_data = dict(response.json().get(u'items', {}))
        pages = []
        for pageid, authority in id_to_authority:
            pages.append(dict(authority=authority, pageid=pageid, **page_data.get(str(pageid), {})))

        return pages

    def get_all_titles(self, apfrom=None, aplimit=500):
        """
        Returns all titles for this wiki
        :param apfrom: starting string
        :type apfrom: unicode
        :param aplimit: number of titles
        :type aplimit: int
        :return: list of pages
        :rtype: list
        """
        params = {u'action': u'query', u'list': u'allpages', u'aplimit': aplimit,
                  u'apfilterredir': u'nonredirects', u'format': u'json'}
        if apfrom is not None:
            params[u'apfrom'] = apfrom
        resp = requests.get(self.api_data[u'url'], params=params)
        response = resp.json()
        resp.close()
        allpages = response.get(u'query', {}).get(u'allpages', [])
        if u'query-continue' in response:
            return allpages + self.get_all_titles(apfrom=response[u'query-continue'][u'allpages'][u'apfrom'],
                                                  aplimit=aplimit)
        return allpages

    def get_all_pages(self):
        """
        Optimized for all pages
        :return: dict of pages
        :rtype: dict
        """
        page_ids_to_title = {}
        for obj in self.get_all_titles():
            page_ids_to_title[obj[u'pageid']] = obj[u'title']

        self.cursor.execute(u"""
            SELECT article_id, local_authority FROM articles WHERE wiki_id = %s ORDER BY local_authority DESC
            """ % self.wiki_id)

        return [(r[0], r[1]) for r in self.cursor.fetchall()]

    @staticmethod
    def all_wikis(args):
        """
        Accesses all wikis from database
        :return: dict keying wiki name to ids
        :rtype: dict
        """
        db, cursor = get_db_and_cursor(args)
        cursor.execute(u"""SELECT wiki_id, title FROM wikis""")
        return dict([(row[1], row[0]) for row in cursor.fetchall()])

    def get_workbook(self, num_processes=8):
        use_caching()
        set_global_num_processes(num_processes)

        workbook = xlwt.Workbook()
        pages_sheet = workbook.add_sheet(u"Pages by Authority")
        pages_sheet.write(0, 0, u"Page")
        pages_sheet.write(0, 1, u"Authority")

        page_authority = self.get_all_pages()

        pages, authorities = zip(*page_authority)
        scaler = MinMaxScaler(authorities, enforced_min=0, enforced_max=100)
        for i, page in enumerate(pages):
            if i > 65000:
                break
            pages_sheet.write(i+1, 0, page)
            pages_sheet.write(i+1, 1, scaler.scale(authorities[i]))

        author_authority = self.get_all_authors()

        for author in author_authority:
            author[u'topics'] = [topic.topic for topic in
                                 UserModel(author, self.args).get_topics_for_wiki(self.wiki_id, limit=5)]

        topic_authority = self.get_topics(limit=None)
        for topic in topic_authority:
            topic[u'authors'] = TopicModel(topic[u'topic'], self.args).get_users(5, with_api=False)

        authors_sheet = workbook.add_sheet(u"Authors by Authority")
        authors_sheet.write(0, 0, u"Author")
        authors_sheet.write(0, 1, u"Authority")

        authors_topics_sheet = workbook.add_sheet(u"Topics for Best Authors")
        authors_topics_sheet.write(0, 0, u"Author")
        authors_topics_sheet.write(0, 1, u"Topic")
        authors_topics_sheet.write(0, 2, u"Rank")
        authors_topics_sheet.write(0, 3, u"Score")

        # why is total_authority not there?
        all_total_authorities = [author.get(u'total_authority', 0) for author in author_authority]
        scaler = MinMaxScaler(all_total_authorities, enforced_min=0, enforced_max=100)
        pivot_counter = 1
        for i, author in enumerate(author_authority):
            authors_sheet.write(i+1, 0, author[u'user_name'])
            authors_sheet.write(i+1, 1, scaler.scale(author[u'total_authority']))
            for rank, topic in enumerate(author[u'topics'][:10]):
                if pivot_counter > 65000:
                    break
                authors_topics_sheet.write(pivot_counter, 0, author[u'name'])
                authors_topics_sheet.write(pivot_counter, 1, topic[0])
                authors_topics_sheet.write(pivot_counter, 2, rank+1)
                authors_topics_sheet.write(pivot_counter, 3, topic[1])
                pivot_counter += 1
            if i > 65000:
                break

        print u"Writing Topic Data"
        topics_sheet = workbook.add_sheet(u"Topics by Authority")
        topics_sheet.write(0, 0, u"Topic")
        topics_sheet.write(0, 1, u"Authority")

        topics_authors_sheet = workbook.add_sheet(u"Authors for Best Topics")
        topics_authors_sheet.write(0, 0, u"Topic")
        topics_authors_sheet.write(0, 1, u"Author")
        topics_authors_sheet.write(0, 2, u"Rank")
        topics_authors_sheet.write(0, 3, u"Authority")

        scaler = MinMaxScaler([x[1].get(u'authority', 0) for x in topic_authority], enforced_min=0, enforced_max=100)
        pivot_counter = 1
        for i, topic in enumerate(topic_authority):
            topics_sheet.write(i+1, 0, topic[0])
            topics_sheet.write(i+1, 1, scaler.scale(topic[1][u'authority']))
            authors = topic[1][u'authors']
            for rank, author in enumerate(authors[:10]):
                if pivot_counter > 65000:
                    break
                topics_authors_sheet.write(pivot_counter, 0, topic[0])
                topics_authors_sheet.write(pivot_counter, 1, author[u'author'])
                topics_authors_sheet.write(pivot_counter, 2, rank+1)
                topics_authors_sheet.write(pivot_counter, 3, author[u'topic_authority'])
                pivot_counter += 1

            if i > 65000:
                break

        return workbook


class PageModel(BaseModel):
    """
    Logic for a given page
    """

    def __init__(self, wiki_id, page_id, args):
        """
        Init method
        :param wiki_id: the wiki id
        :type wiki_id: int
        :param page_id: the id of the page
        :type page_id: int
        :param args: namespace with db info
        :type args:class:`arparse.Namespace`
        """
        BaseModel.__init__(self, args)
        self.page_id = page_id
        self.wiki_id = wiki_id
        self.wiki = WikiModel(wiki_id, args)
        self._api_data = None

    @property
    def api_data(self):
        """
        Memoized lazy-loaded property access
        :return: dict of api data
        :rtype: data
        """
        if not self._api_data:
            self._api_data = requests.get(u'%sapi/v1/Articles/Details' % self.wiki.api_data[u'url'],
                                          params=dict(ids=self.page_id)).json()[u'items'][self.page_id]
        return self._api_data

    def get_users(self, limit=10):
        """
        Get the most authoritative users for this page
        :param limit: the number of users you want
        :type limit: int
        :return: a list of of user dicts in order of authority
        :rtype: list
        """

        self.cursor.execute(u"""SELECT users.user_id, users.user_name, contribs FROM articles_users
                       INNER JOIN users ON wiki_id = %s AND article_id = %s AND users.user_id = articles_users.user_id
                       ORDER BY contribs desc LIMIT %d""" % (self.wiki_id, self.page_id, limit))

        users_dict = OrderedDict([(a[0], {u'id': a[0], u'name': a[1], u'contribs': a[2]})
                                  for a in self.cursor.fetchall()])

        user_api_data = requests.get(self.wiki.api_data[u'url']+u'/api/v1/User/Details',
                                     params={u'ids': u','.join(map(lambda x: str(x), users_dict.keys())),
                                             u'format': u'json'}).json()[u'items']

        map(lambda x: users_dict[x[u'user_id']].update(x), user_api_data)

        return users_dict.values()

    def get_topics(self, limit=10):
        """
        Get the topics for the current page
        :param limit: how much you want fool
        :type limit: int
        :return: a list of dicts
        :rtype: list
        """

        self.cursor.execute(u"""SELECT topics.topic_id, topics.name
                       FROM topics INNER JOIN articles_topics ON wiki_id = %s AND article_id = %s
                                              AND topics.topic_id = articles_topics.topic_id
                       ORDER BY topics.total_authority DESC LIMIT %d""" % (self.wiki_id, self.page_id, limit))

        return [{u'id': row[0], u'name': row[1]} for row in self.cursor.fetchall()]


class UserModel(BaseModel):
    """
    Data model for user
    """

    def __init__(self, user_name, args):
        """
        init method
        :param user_name: the username we care about
        :type user_name: str
        :param args: namespace
        :type args:class:`argparse.Namespace`
        """
        BaseModel.__init__(self, args)
        self.user_name = user_name

    def get_pages(self, limit=10):
        """
        Gets top pages for this author
        calculated by contribs times global authority
        :param limit: how many you want
        :type limit: int
        :return: a list of dicts
        :rtype: list
        """
        self.cursor.execute(u"""
    SELECT wikis.url, articles.article_id
    FROM users
      INNER JOIN articles_users ON users.user_name = '%s' AND articles_users.user_id = users.user_id
      INNER JOIN wikis on wikis.wiki_id = articles_users.wiki_id
      INNER JOIN articles ON articles.article_id = articles_users.article_id
                          AND articles.wiki_id = articles_users.wiki_id
    ORDER BY articles_users.contribs * articles.global_authority DESC LIMIT %d;
    -- selects the most important pages a user has contributed to the most to
    """ % (self.db.escape_string(self.user_name), limit))
        url_to_ids = defaultdict(list)
        ordered_db_results = [(y[0], str(y[1])) for y in self.cursor.fetchall()]
        map(lambda x: url_to_ids[x[0]].append(x[1]), ordered_db_results)
        url_to_articles = dict()
        for url, ids in url_to_ids.items():
            response = requests.get(u'%s/api/v1/Articles/Details' % url, params=dict(ids=u','.join(ids)))
            url_to_articles[url] = dict(response.json().get(u'items', {}))

        ordered_page_results = []
        for url, page_id in ordered_db_results:
            result = dict(base_url=url, **url_to_articles[url].get(page_id, {}))
            result[u'full_url'] = (result.get(u'base_url', '').strip(u'/') + result.get(u'url', ''))
            ordered_page_results.append(result)

        return ordered_page_results

    def get_wikis(self, limit=10):
        """
        Most important wikis for this user
        Calculated by sum of contribs times global authority
        :param limit: the limit
        :type limit: int
        :return: an ordereddict of wiki ids to wiki dicts
        :rtype:class:`collections.OrderedDict`
        """
        self.cursor.execute(u"""
SELECT wikis.wiki_id
FROM users
  INNER JOIN articles_users ON users.user_name = '%s' AND articles_users.user_id = users.user_id
  INNER JOIN wikis on wikis.wiki_id = articles_users.wiki_id
  INNER JOIN articles ON articles.article_id = articles_users.article_id AND articles.wiki_id = articles_users.wiki_id
GROUP BY wikis.wiki_id ORDER BY SUM(articles_users.contribs * articles.global_authority) DESC LIMIT %d;
-- selects the most important wiki a user has contributed the most to
    """ % (self.db.escape_string(self.user_name), limit))

        wiki_ids = [str(x[0]) for x in self.cursor.fetchall()]

        result = requests.get(u'http://www.wikia.com/api/v1/Wikis/Details',
                              params=dict(ids=u','.join(wiki_ids)))

        wikis = result.json().get(u'items', {})

        return OrderedDict([(wid, wikis.get(wid)) for wid in wiki_ids])

    def get_topics(self, limit=10):
        """
        Gets most important topics for this user
        :param limit: limit
        :type limit: int
        :return: ordered dict of topic name to auth
        :rtype:class:`collections.OrderedDict`
        """
        sql = u"""
        SELECT topics.name, SUM(au.contribs * articles.local_authority) AS topic_authority
        FROM users INNER JOIN articles_users au ON users.user_name = "%s" AND users.user_id = au.user_id
                   INNER JOIN JOIN articles_topics art ON art.article_id = au.article_id AND art.wiki_id = au.wiki_id
                   INNER JOIN articles ON articles.article_id = au.article_id AND articles.wiki_id = au.wiki_id
                   INNER JOIN topics ON topics.topic_id = art.topic_id
        GROUP BY topics.topic_id
        ORDER BY topic_authority""" % self.user_name

        if limit:
            sql += u" LIMIT %d" % limit

        self.cursor.execute(sql)
        return OrderedDict([(x[0], dict(name=x[0], authority=x[1]) for x in self.cursor.fetchall())])

    def get_topics_for_wiki(self, wiki_id, limit=10):
        """
        Gets most important topics for this user on this wiki
        :param limit: the wiki id
        :type limit: str
        :param limit: limit
        :type limit: int
        :return: ordered dict of topic name to auth
        :rtype:class:`collections.OrderedDict`
        """
        sql = u"""
        SELECT topics.name, SUM(au.contribs * articles.local_authority) AS topic_authority
        FROM users INNER JOIN articles_users au ON au.wiki_id = %d
                                                AND users.user_name = "%s" AND users.user_id = au.user_id
                   INNER JOIN JOIN articles_topics art ON art.article_id = au.article_id AND art.wiki_id = au.wiki_id
                   INNER JOIN articles ON articles.article_id = au.article_id AND articles.wiki_id = au.wiki_id
                   INNER JOIN topics ON topics.topic_id = art.topic_id
        GROUP BY topics.topic_id
        ORDER BY topic_authority""" % (wiki_id, self.user_name)

        if limit:
            sql += u" LIMIT %d" % limit

        self.cursor.execute(sql)
        return OrderedDict([(x[0], dict(name=x[0], authority=x[1]) for x in self.cursor.fetchall())])


class MinMaxScaler:
    """
    Scales values from 0 to 1 by default
    """

    def __init__(self, vals, enforced_min=0, enforced_max=1):
        self.min = min(vals)
        self.max = max(vals)
        self.enforced_min = enforced_min
        self.enforced_max = enforced_max

    def scale(self, val):
        return (((self.enforced_max - self.enforced_min) * (val - self.min))
                / (self.max - self.min)) + self.enforced_min
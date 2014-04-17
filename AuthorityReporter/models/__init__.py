from wikia_dstk.authority import get_db_and_cursor
from nlp_services.authority import WikiAuthorityService
from nlp_services.authority import WikiAuthorTopicAuthorityService, WikiAuthorsToIdsService
from nlp_services.authority import WikiTopicsToAuthorityService
from collections import defaultdict, OrderedDict
from nlp_services.caching import use_caching
from multiprocessing import Pool
import requests


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
        ordered_db_results = [(y[0], y[1], str(y[2]), str(y[3]), y[4]) for y in self.cursor.fetchall()]
        url_to_ids = defaultdict(list)
        map(lambda x: url_to_ids[x[0]].append(x[2]), ordered_db_results)

        results = Pool(processes=8).map_async(get_page_response, list(url_to_ids.items())).get()
        print results
        url_to_articles = dict(results)

        ordered_page_results = []
        for url, wiki_name, wiki_id, page_id, authority in ordered_db_results:
            result = dict(base_url=url, **url_to_articles[url].get(page_id, {}))
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
            wikis[wid][u'authority'] = auth

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
                obj[u'total_authority'] = id_to_auth[obj[u'user_id'][u'total_authority']]
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
        self.authority_data = WikiAuthorityService().get_value(wiki_id)
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
        Uses WikiTopicsToAuthorityService to get top topics for this wiki
        (Should be cached!)
        TODO: should we use MySQL for this now?
        :param limit: number of topics to get
        :type limit: int
        :return: a list of dicts
        :rtype: list
        """
        use_caching()
        wiki_topics = WikiTopicsToAuthorityService().get_value(self.wiki_id)
        top_topics = [dict(topic=topic, authority=data[u'authority'], authors=data[u'authors'])
                      for topic, data in sorted(wiki_topics, key=lambda x: x[1][u'authority'], reverse=True)[:limit]]
        return top_topics

    def get_authors(self, limit=10):
        """
        Provides the top authors for a wiki
        :param limit: number of authors you want
        :type limit: int
        :return: list of author dicts
        :rtype: list
        """
        topic_authority_data = WikiAuthorTopicAuthorityService().get_value(self.wiki_id)

        authors_to_topics = sorted(topic_authority_data[u'weighted'].items(),
                                   key=lambda y: sum(y[1].values()),
                                   reverse=True)[:10]

        a2ids = WikiAuthorsToIdsService().get_value(self.wiki_id)

        authors_dict = dict([(x[0], dict(name=x[0],
                                         total_authority=sum(x[1].values()),
                                         topics=sorted(x[1].items(), key=lambda z: z[1], reverse=True)[:limit]))
                             for x in authors_to_topics])

        joined_ids = u','.join([str(a2ids[user]) for user, contribs in authors_to_topics])
        user_api_data = requests.get(self.api_data[u'url']+u'/api/v1/User/Details',
                                     params={u'ids': joined_ids,
                                             u'format': u'json'}).json()[u'items']

        for user_data in user_api_data:
            authors_dict[user_data[u'name']].update(user_data)
            authors_dict[user_data[u'name']][u'url'] = authors_dict[user_data[u'name']][u'url'][1:]

        author_objects = sorted(authors_dict.values(), key=lambda z: z[u'total_authority'], reverse=True)
        return author_objects

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
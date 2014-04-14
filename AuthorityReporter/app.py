from flask import Flask, render_template, Response
import requests
import argparse
import json
from collections import OrderedDict
from wikia_dstk.authority import add_db_arguments, get_db_and_cursor
from nlp_services.authority import WikiAuthorityService, PageAuthorityService
from nlp_services.authority import WikiAuthorTopicAuthorityService, WikiAuthorsToIdsService
from nlp_services.authority import WikiTopicsToAuthorityService
from nlp_services.discourse.entities import CombinedWikiPageEntitiesService
from nlp_services.caching import use_caching
from multiprocessing import Pool
from collections import defaultdict

use_caching()


def update_top_page(arg_tuple):
    wiki_id, top_page = arg_tuple
    no_image_url = (u"http://slot1.images.wikia.nocookie.net/__cb62407/"
                    + u"common/extensions/wikia/Search/images/wiki_image_placeholder.png")
    if top_page.get(u'thumbnail') is None:
        top_page[u'thumbnail'] = no_image_url
    top_page[u'entities'] = CombinedWikiPageEntitiesService().get_value(wiki_id+'_'+str(top_page['id']))
    top_page[u'authorities'] = PageAuthorityService().get_value(wiki_id+'_'+str(top_page['id']))
    for author in top_page[u'authorities']:
        author[u'contrib_pct'] = u"%.2f%%" % (author[u'contrib_pct'] * 100.0)
    top_page[u'authority'] = u"%.2f" % (100.0 * top_page[u'authority'])

    return top_page


app = Flask(__name__)
args = None

POOL = Pool(processes=20)
WIKI_ID = None
WIKI_AUTHORITY_DATA = None
WIKI_API_DATA = None


def configure_wiki_id(wiki_id):
    global WIKI_ID, WIKI_API_DATA, WIKI_AUTHORITY_DATA
    if WIKI_ID != wiki_id:
        WIKI_AUTHORITY_DATA = WikiAuthorityService().get_value(wiki_id)
        WIKI_ID = wiki_id
        WIKI_API_DATA = requests.get(u'http://www.wikia.com/api/v1/Wikis/Details',
                                     params=dict(ids=WIKI_ID)).json()['items'][wiki_id]


@app.route(u'/wiki/<wiki_id>/topics/')
def topics(wiki_id):
    global WIKI_ID, WIKI_API_DATA, WIKI_AUTHORITY_DATA, POOL
    configure_wiki_id(wiki_id)

    wiki_topics = WikiTopicsToAuthorityService().get_value(wiki_id)
    top_topics = [dict(topic=topic, authority=data[u'authority'], authors=data[u'authors'])
                  for topic, data in sorted(wiki_topics, key=lambda x: x[1][u'authority'], reverse=True)[:10]]

    return render_template(u'topics.html', topics=top_topics, wiki_api_data=WIKI_API_DATA)


@app.route(u'/wiki/<wiki_id>/users/')
def authors(wiki_id):
    global WIKI_ID, WIKI_API_DATA, WIKI_AUTHORITY_DATA, POOL
    configure_wiki_id(wiki_id)

    topic_authority_data = WikiAuthorTopicAuthorityService().get_value(wiki_id)

    authors_to_topics = sorted(topic_authority_data[u'weighted'].items(),
                               key=lambda y: sum(y[1].values()),
                               reverse=True)[:10]

    a2ids = WikiAuthorsToIdsService().get_value(wiki_id)

    authors_dict = dict([(x[0], dict(name=x[0],
                                     total_authority=sum(x[1].values()),
                                     topics=sorted(x[1].items(), key=lambda z: z[1], reverse=True)[:10]))
                         for x in authors_to_topics])

    user_api_data = requests.get(WIKI_API_DATA[u'url']+u'/api/v1/User/Details',
                                 params={u'ids': u','.join([str(a2ids[user]) for user, contribs in authors_to_topics]),
                                         u'format': u'json'}).json()[u'items']

    for user_data in user_api_data:
        authors_dict[user_data[u'name']].update(user_data)
        authors_dict[user_data[u'name']][u'url'] = authors_dict[user_data[u'name']][u'url'][1:]

    author_objects = sorted(authors_dict.values(), key=lambda z: z[u'total_authority'], reverse=True)

    return render_template(u'authors.html', authors=author_objects, wiki_api_data=WIKI_API_DATA)


@app.route(u'/wiki_autocomplete.js')
def wiki_autocomplete():
    global args
    db, cursor = get_db_and_cursor(args)
    cursor.execute(u"""SELECT wiki_id, title FROM wikis""")
    wikis = dict([(row[1], row[0]) for row in cursor.fetchall()])
    return Response(u"var wikis = %s;" % json.dumps(wikis),
                    mimetype=u"application/javascript",
                    content_type=u"application/javascript")


@app.route(u'/wiki/<wiki_id>/pages/')
def wiki_articles(wiki_id):
    global args
    db, cursor = get_db_and_cursor(args)
    cursor.execute(u"""SELECT article_id, local_authority FROM articles
                       WHERE wiki_id = %s ORDER BY local_authority DESC LIMIT 10""" % wiki_id)
    id_to_authority = [(row[0], row[1]) for row in cursor.fetchall()]
    cursor.execute(u"""SELECT title, url FROM wikis WHERE wiki_id = %s""" % wiki_id)
    (wiki_title, wiki_url,) = cursor.fetchone()
    response = requests.get(wiki_url+u'api/v1/Articles/Details',
                            params=dict(ids=u','.join([str(a[0]) for a in id_to_authority])))

    page_data = dict(response.json().get(u'items', {}))
    pages = []
    for pageid, authority in id_to_authority:
        pages.append(dict(authority=authority, pageid=pageid, **page_data.get(str(pageid), {})))
    return render_template(u'v2_wiki_articles.html',
                           pages=pages, wiki_url=wiki_url, wiki_title=wiki_title, wiki_id=wiki_id)


@app.route(u'/wiki/<wiki_id>/page/<page_id>/')
def page_index(wiki_id, page_id):
    global WIKI_ID, WIKI_API_DATA, WIKI_AUTHORITY_DATA, POOL, args
    configure_wiki_id(wiki_id)

    db, cursor = get_db_and_cursor(args)
    cursor.execute(u"""SELECT users.user_id, users.user_name, contribs FROM articles_users
                       INNER JOIN users ON wiki_id = %s AND article_id = %s AND users.user_id = articles_users.user_id
                       ORDER BY contribs desc LIMIT 10""" % (wiki_id, page_id))

    users_dict = OrderedDict([(a[0], {u'id': a[0], u'name': a[1], u'contribs': a[2]}) for a in cursor.fetchall()])

    user_api_data = requests.get(WIKI_API_DATA[u'url']+u'/api/v1/User/Details',
                                 params={u'ids': u','.join(map(lambda x: str(x), users_dict.keys())),
                                         u'format': u'json'}).json()[u'items']

    for user_data in user_api_data:
        users_dict[user_data[u'user_id']].update(user_data)

    cursor.execute(u"""SELECT topics.topic_id, topics.name
                       FROM topics INNER JOIN articles_topics ON wiki_id = %s AND article_id = %s
                                              AND topics.topic_id = articles_topics.topic_id
                       ORDER BY topics.total_authority DESC LIMIT 25""" % (wiki_id, page_id))

    page_topics = [{u'id': row[0], u'name': row[1]} for row in cursor.fetchall()]

    page_title = u"TODO: GET PAGE TITLE FROM API"

    return render_template(u'page.html', users=users_dict.values(), topics=page_topics,
                           wiki_api_data=WIKI_API_DATA, page_title=page_title)


@app.route(u'/topic/<topic>/wikis/')
def wikis_for_topic(topic):
    global args
    db, cursor = get_db_and_cursor(args)
    cursor.execute(u"""
SELECT wikis.wiki_id, SUM(articles.global_authority) AS total_auth
FROM topics
  INNER JOIN articles_topics ON topics.name = '%s' AND topics.topic_id = articles_topics.topic_id
  INNER JOIN articles ON articles.article_id = articles_topics.article_id AND articles.wiki_id = articles_topics.wiki_id
  INNER JOIN wikis ON articles.wiki_id = wikis.wiki_id GROUP BY articles.wiki_id ORDER BY total_auth DESC LIMIT 10
-- selects the best wikis for a given topic
                    """ % db.escape_string(topic))

    wiki_ids = [str(x[0]) for x in cursor.fetchall()]

    result = requests.get(u'http://www.wikia.com/api/v1/Wikis/Details',
                          params=dict(ids=u','.join(wiki_ids)))

    wikis = result.json().get(u'items', {})

    return render_template(u'wiki.html', wikis=wikis, wiki_ids=wiki_ids)


@app.route(u'/topic/<topic>/pages/')
def pages_for_topic(topic):
    global args
    db, cursor = get_db_and_cursor(args)
    cursor.execute(u"""
    SELECT wikis.url, wikis.title, articles.article_id
    FROM topics INNER JOIN articles_topics ON topics.name = '%s' AND topics.topic_id = articles_topics.topic_id
    INNER JOIN articles ON articles.article_id = articles_topics.article_id
                           AND articles.wiki_id = articles_topics.wiki_id
    INNER JOIN wikis ON wikis.wiki_id = articles.wiki_id
    ORDER BY articles.global_authority DESC
    """ % db.escape_string(topic))

    ordered_db_results = [(y[0], y[1], str(y[2])) for y in cursor.fetchall()]
    url_to_ids = defaultdict(list)
    url_to_articles = {}
    map(lambda x: url_to_ids[x[0]].append(x[2]), ordered_db_results)
    for url, ids in url_to_ids.items():
        response = requests.get(u'%s/api/v1/Articles/Details' % url, params=dict(ids=u','.join(ids)))
        url_to_articles[url] = dict(response.json().get(u'items', {}))

    ordered_page_results = []
    for url, wiki_name, page_id in ordered_db_results:
        result = dict(base_url=url, **url_to_articles[url].get(page_id, {}))
        result[u'full_url'] = (result.get(u'base_url', '').strip(u'/') + result.get(u'url', ''))
        result[u'wiki'] = wiki_name
        ordered_page_results.append(result)

    return render_template(u'topic_pages.html', topic=topic, pages=ordered_page_results)


@app.route(u'/user/<user_name>/pages/')
def pages_for_user(user_name):
    global args
    db, cursor = get_db_and_cursor(args)
    cursor.execute(u"""
SELECT wikis.url, articles.article_id
FROM users
  INNER JOIN articles_users ON users.user_name = '%s' AND articles_users.user_id = users.user_id
  INNER JOIN wikis on wikis.wiki_id = articles_users.wiki_id
  INNER JOIN articles ON articles.article_id = articles_users.article_id AND articles.wiki_id = articles_users.wiki_id
ORDER BY articles_users.contribs * articles.global_authority DESC LIMIT 10;
-- selects the most important pages a user has contributed to the most to
""" % db.escape_string(user_name))
    url_to_ids = defaultdict(list)
    ordered_db_results = [(y[0], str(y[1])) for y in cursor.fetchall()]
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

    return render_template(u'user_pages.html', user_name=user_name, pages=ordered_page_results)


@app.route(u'/user/<user_name>/wikis/')
def wikis_for_user(user_name):
    global args
    db, cursor = get_db_and_cursor(args)
    cursor.execute(u"""
SELECT wikis.wiki_id
FROM users
  INNER JOIN articles_users ON users.user_name = '%s' AND articles_users.user_id = users.user_id
  INNER JOIN wikis on wikis.wiki_id = articles_users.wiki_id
  INNER JOIN articles ON articles.article_id = articles_users.article_id AND articles.wiki_id = articles_users.wiki_id
GROUP BY wikis.wiki_id ORDER BY SUM(articles_users.contribs * articles.global_authority) DESC LIMIT 10;
-- selects the most important wiki a user has contributed the most to
    """ % db.escape_string(user_name))

    wiki_ids = [str(x[0]) for x in cursor.fetchall()]

    result = requests.get(u'http://www.wikia.com/api/v1/Wikis/Details',
                          params=dict(ids=u','.join(wiki_ids)))

    wikis = result.json().get(u'items', {})

    return render_template(u'wiki.html', wikis=wikis, wiki_ids=wiki_ids,
                           topic=u"Top Wikis for User <i>%s</i>" % user_name)


@app.route(u'/topic/<topic>/users/')
def topic_users(topic):
    global args
    db, cursor = get_db_and_cursor(args)
    cursor.execute(u"""
SELECT users.user_id, SUM(articles_users.contribs * articles.global_authority) AS auth
FROM topics
  INNER JOIN articles_topics ON topics.name = '%s' AND topics.topic_id = articles_topics.topic_id
  INNER JOIN articles_users ON articles_topics.article_id = articles_users.article_id
                               AND articles_topics.wiki_id = articles_users.wiki_id
  INNER JOIN articles ON articles.article_id = articles_users.article_id AND articles.wiki_id = articles_users.wiki_id
  INNER JOIN users ON articles_users.user_id = users.user_id
GROUP BY users.user_id
ORDER BY auth DESC
LIMIT 10
-- selects the most influential authors for a given topic
    """ % db.escape_string(topic))

    user_data = cursor.fetchall()

    response = requests.get(u'http://www.wikia.com/api/v1/User/Details',
                            params={u'ids': u','.join([str(x[0]) for x in user_data])})

    user_api_data = response.json()[u'items']

    id_to_auth = dict(user_data)
    author_objects = []
    for obj in user_api_data:
        obj[u'total_authority'] = id_to_auth[obj[u'user_id']]
        author_objects.append(obj)

    fake_wiki_api_data = {u'title': u'GlobalAuthors for %s' % topic, u'url': u'http://www.wikia.com/'}

    return render_template(u'authors.html', authors=author_objects, wiki_api_data=fake_wiki_api_data)


@app.route(u'/')
def index():
    return render_template(u'v2_index.html')


def main():
    global app, args
    parser = add_db_arguments(argparse.ArgumentParser(description=u'Authority Flask App'))
    parser.add_argument(u'--app-host', dest=u'app_host', action=u'store', default=u'0.0.0.0',
                        help=u"App host")
    parser.add_argument(u'--app-port', dest=u'app_port', action=u'store', default=5000, type=int,
                        help=u"App port")
    args = parser.parse_args()

    app.debug = True
    app.run(host=args.app_host, port=args.app_port)


if __name__ == u'__main__':
    main()

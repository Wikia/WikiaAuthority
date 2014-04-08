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


@app.route(u'/wiki/<wiki_id>/articles/')
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

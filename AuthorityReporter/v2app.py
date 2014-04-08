from argparse import ArgumentParser
from wikia_dstk.authority import add_db_arguments, get_db_and_cursor
import json
from flask import Flask, render_template, Response
import json
import requests


app = Flask(__name__)
args = None


def get_args():
    ap = add_db_arguments(ArgumentParser())
    ap.add_argument(u'--app-host', dest=u'app_host', default=u'0.0.0.0')
    ap.add_argument(u'--app-port', dest=u'app_port', default=5000, type=int)
    return ap.parse_args()


def main():
    global app, args
    args = get_args()
    app.debug = True
    app.run(host=args.app_host, port=args.app_port)



@app.route(u'/wiki/<wiki_id>/topics/')
def wiki_topics(wiki_id):
    global args
    db, cursor = get_db_and_cursor(args)
    cursor.execute(u"""SELECT topics.topic_id, topics.name, SUM(articles.local_authority) AS topic_authority
                       FROM articles
                       INNER JOIN articles_topics
                            ON articles.wiki_id = %s AND articles_topics.wiki_id = %s
                            AND articles.article_id = articles_topics.article_id
                       INNER JOIN topics ON articles_topics.topic_id = topics.topic_id
                       GROUP BY topics.topic_id
                       ORDER BY topic_authority DESC
                       """ % (wiki_id, wiki_id))
    id_to_authority = [(row[0], row[1], row[2]) for row in cursor.fetchall()]
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


@app.route(u'/')
def index():
    return render_template(u'v2_index.html')


if __name__ == u'__main__':
    main()
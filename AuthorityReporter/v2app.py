from argparse import ArgumentParser
from wikia_dstk.authority import add_db_arguments, get_db_and_cursor
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


@app.route(u'/wiki_autocomplete.js')
def wiki_autocomplete():
    global args
    db, cursor = get_db_and_cursor(args)
    cursor.execute(u"""SELECT wiki_id, title FROM wikis""")
    wikis = dict([(row[1], row[0]) for row in cursor.fetchall()])
    return Response(u"var wikis = %s;" % json.dumps(wikis),
                    mimetype=u"application/javascript",
                    content_type=u"application/javascript")


@app.route(u'/wiki/<wiki_id>/')
def wiki(wiki_id):
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
    return render_template(u'v2_wiki.html', pages=pages, wiki_url=wiki_url, wiki_title=wiki_title, wiki_id=wiki_id)


@app.route(u'/')
def index():
    return render_template(u'v2_index.html')


if __name__ == u'__main__':
    main()
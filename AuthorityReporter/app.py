from flask import Flask, render_template
import requests
import argparse
from nlp_services.authority import WikiAuthorityService, PageAuthorityService
from nlp_services.authority import WikiAuthorTopicAuthorityService, WikiAuthorsToIdsService
from nlp_services.authority import WikiTopicsToAuthorityService
from nlp_services.discourse.entities import CombinedWikiPageEntitiesService
from nlp_services.caching import use_caching
from multiprocessing import Pool

use_caching()


def update_top_page(args):
    wiki_id, top_page = args
    no_image_url = ("http://slot1.images.wikia.nocookie.net/__cb62407/"
                    + "common/extensions/wikia/Search/images/wiki_image_placeholder.png")
    if top_page.get('thumbnail') is None:
        top_page['thumbnail'] = no_image_url
    top_page['entities'] = CombinedWikiPageEntitiesService().get_value(wiki_id+'_'+str(top_page['id']))
    top_page['authorities'] = PageAuthorityService().get_value(wiki_id+'_'+str(top_page['id']))
    for author in top_page['authorities']:
        author['contrib_pct'] = "%.2f%%" % (author['contrib_pct'] * 100.0)
    top_page['authority'] = "%.2f" % (100.0 * top_page['authority'])

    return top_page


app = Flask(__name__)

POOL = Pool(processes=20)
SOLR_URL = 'http://search-s10:8983/main/'
WIKI_ID = None
WIKI_AUTHORITY_DATA = None
WIKI_API_DATA = None


def configure_wiki_id(wiki_id):
    global WIKI_ID, WIKI_API_DATA, WIKI_AUTHORITY_DATA
    if WIKI_ID != wiki_id:
        WIKI_AUTHORITY_DATA = WikiAuthorityService().get_value(wiki_id)
        WIKI_ID = wiki_id
        WIKI_API_DATA = requests.get('http://www.wikia.com/api/v1/Wikis/Details',
                                     params=dict(ids=WIKI_ID)).json()['items'][wiki_id]


@app.route('/<wiki_id>/topics/')
def topics(wiki_id):
    global WIKI_ID, WIKI_API_DATA, WIKI_AUTHORITY_DATA, POOL
    configure_wiki_id(wiki_id)

    topics = WikiTopicsToAuthorityService().get_value(wiki_id)
    top_topics = [dict(topic=topic, authority=data['authority'], authors=data['authors'])
                  for topic, data in sorted(topics, key=lambda x: x[1]['authority'], reverse=True)[:10]]

    return render_template('topics.html', topics=top_topics, wiki_api_data=WIKI_API_DATA)


@app.route('/<wiki_id>/authors/')
def authors(wiki_id):
    global WIKI_ID, WIKI_API_DATA, WIKI_AUTHORITY_DATA, POOL
    configure_wiki_id(wiki_id)

    topic_authority_data = WikiAuthorTopicAuthorityService().get_value(wiki_id)

    authors_to_topics = sorted(topic_authority_data['weighted'].items(),
                               key=lambda y: sum(y[1].values()),
                               reverse=True)[:10]

    a2ids = WikiAuthorsToIdsService().get_value(wiki_id)

    authors_dict = dict([(x[0], dict(name=x[0],
                                     total_authority=sum(x[1].values()),
                                     topics=sorted(x[1].items(), key=lambda z: z[1], reverse=True)[:10]))
                         for x in authors_to_topics])

    user_api_data = requests.get(WIKI_API_DATA['url']+'/api/v1/User/Details',
                                 params={'ids': ','.join([str(a2ids[user]) for user, contribs in authors_to_topics]),
                                 'format': 'json'}).json()['items']

    for user_data in user_api_data:
        authors_dict[user_data['name']].update(user_data)
        authors_dict[user_data['name']]['url'] = authors_dict[user_data['name']]['url'][1:]

    author_objects = sorted(authors_dict.values(), key=lambda z: z['total_authority'], reverse=True)

    return render_template('authors.html', authors=author_objects, wiki_api_data=WIKI_API_DATA)


@app.route('/<wiki_id>/<page>/')
def index(wiki_id, page):
    page = int(page)
    global WIKI_ID, WIKI_API_DATA, WIKI_AUTHORITY_DATA, POOL
    configure_wiki_id(wiki_id)

    top_docs = sorted(WIKI_AUTHORITY_DATA.items(), key=lambda z: z[1], reverse=True)
    top_page_tups = [(tup[0].split('_')[-1], tup[1]) for tup in top_docs[(page-1)*10:page*10]]

    page_api_data = requests.get(WIKI_API_DATA['url'].split('/wiki')[0]+'/api/v1/Articles/Details',
                                 params={'ids': ','.join([x[0] for x in top_page_tups])}).json()['items']

    top_pages = []
    for tup in top_page_tups:
        if tup[0] in page_api_data:
            d = dict(id=tup[0], authority=tup[1])
            d.update(page_api_data[tup[0]])
            top_pages.append(d)

    r = POOL.map_async(update_top_page, [(str(wiki_id), page) for page in top_pages])
    r.wait()

    if WIKI_API_DATA['url'].endswith('/'):
        WIKI_API_DATA['url'] = WIKI_API_DATA['url'][:-1]
    return render_template('index.html', docs=r.get(), wiki_api_data=WIKI_API_DATA)


def main():
    global app
    parser = argparse.ArgumentParser(description='Authority Flask App')
    parser.add_argument('--host', dest='host', action='store', default='0.0.0.0',
                        help="App host")
    parser.add_argument('--port', dest='port', action='store', default=5000, type=int,
                        help="App port")
    options = parser.parse_args()

    app.debug = True
    app.run(host=options.host, port=options.port)


if __name__ == '__main__':
    main()

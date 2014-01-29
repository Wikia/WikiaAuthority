from flask import Flask, request, render_template
import requests
import sys
import re
import random
import json
import time
import os
import boto
import argparse
from nlp_services.authority import WikiAuthorityService, PageAuthorityService
from nlp_services.discourse.entities import CombinedWikiPageEntitiesService
from multiprocessing import Pool


def update_top_page(args):
    wiki_id, top_page = args
    NO_IMAGE_URL = "http://slot1.images.wikia.nocookie.net/__cb62407/common/extensions/wikia/Search/images/wiki_image_placeholder.png"
    if top_page.get('thumbnail') is None:
        top_page['thumbnail'] = NO_IMAGE_URL
    top_page['entities'] = CombinedWikiPageEntitiesService().get_value(wiki_id+'_'+str(top_page['id']))
    print PageAuthorityService().get(wiki_id+'_'+wiki_id+'_'+str(top_page['id']))
    top_page['authorities'] = PageAuthorityService().get_value(wiki_id+'_'+str(top_page['id']))
    return top_page


app = Flask(__name__)

POOL = Pool(processes=20)
SOLR_URL = 'http://search-s10:8983/main/'
WIKI_ID = None
WIKI_AUTHORITY_DATA = None
WIKI_API_DATA = None
WIKI_URL = None


@app.route('/<wiki_id>/')
def index(wiki_id):
    global WIKI_ID, WIKI_API_DATA, WIKI_URL, WIKI_AUTHORITY_DATA, POOL
    if WIKI_ID != wiki_id:
        WIKI_AUTHORITY_DATA = WikiAuthorityService().get_value(wiki_id)
        WIKI_ID = wiki_id
        WIKI_API_DATA = requests.get('http://www.wikia.com/api/v1/Wikis/Details',
                                     params=dict(ids=WIKI_ID)).json()['items'][wiki_id]

    top_docs = sorted(WIKI_AUTHORITY_DATA.items(), key=lambda x: x[1], reverse=True)
    top_page_tups = [(tup[0].split('_')[-1], tup[1]) for tup in top_docs[:10]]

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

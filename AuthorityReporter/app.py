from flask import Flask, render_template, Response
import argparse
import json
from wikia_dstk.authority import add_db_arguments
from nlp_services.caching import use_caching
from models import TopicModel, WikiModel, PageModel, UserModel

use_caching()

app = Flask(__name__)
args = None


@app.route(u'/wiki/<wiki_id>/topics/')
def topics_for_wiki(wiki_id):
    global args
    model = WikiModel(wiki_id, args)
    return render_template(u'topics.html', topics=model.get_topics(), wiki_api_data=model.api_data)


@app.route(u'/wiki/<wiki_id>/users/')
def users_for_wiki(wiki_id):
    """
    Shows the top 10 users for a wiki
    """
    global args
    model = WikiModel(wiki_id, args)
    return render_template(u'authors.html', authors=model.get_authors(), wiki_api_data=model.api_data)


@app.route(u'/wiki/<wiki_id>/pages/')
def pages_for_wiki(wiki_id):
    """
    Shows the top 10 pages for a wiki by authority
    """
    global args
    model = WikiModel(wiki_id, args)
    return render_template(u'v2_wiki_articles.html', pages=model.get_pages(),
                           wiki_url=model.api_data[u'url'], wiki_title=model.api_data[u'title'], wiki_id=wiki_id)


@app.route(u'/wiki_autocomplete.js')
def wiki_autocomplete():
    """
    This allows JS typeahead for wikis on the homepage
    """
    global args
    wikis = WikiModel.all_wikis(args)
    return Response(u"var wikis = %s;" % json.dumps(wikis),
                    mimetype=u"application/javascript",
                    content_type=u"application/javascript")


@app.route(u'/wiki/<wiki_id>/page/<page_id>/')
def page_index(wiki_id, page_id):
    """
    Shows the top users and topics for a given page
    """
    global args
    model = PageModel(wiki_id, page_id, args)
    return render_template(u'page.html', users=model.get_users(), topics=model.get_topics(),
                           wiki_api_data=model.wiki.api_data, page_title=model.api_data[u'title'])


@app.route(u'/topic/<topic>/wikis/')
def wikis_for_topic(topic):
    """
    Shows the top wikis for a topic
    """
    global args
    return render_template(u'wiki.html', topic=topic, **TopicModel(topic, args).get_wikis())


@app.route(u'/topic/<topic>/pages/')
def pages_for_topic(topic):
    """
    Shows the top pages for a topic
    """
    global args
    return render_template(u'topic_pages.html', topic=topic, pages=TopicModel(topic, args).get_pages())


@app.route(u'/topic/<topic>/users/')
def users_for_topic(topic):
    """
    Shows the top 10 users for a topic
    """
    global args
    return render_template(u'authors.html',
                           authors=TopicModel(topic, args).get_users(),
                           wiki_api_data={u'title': u'Global Authors for %s' % topic, u'url': u'http://www.wikia.com/'})


@app.route(u'/user/<user_name>/pages/')
def pages_for_user(user_name):
    """
    Shows the top 10 pages for a user
    """
    global args
    return render_template(u'user_pages.html', user_name=user_name, pages=UserModel(user_name, args).get_pages())


@app.route(u'/user/<user_name>/wikis/')
def wikis_for_user(user_name):
    """
    Shows the top 10 wikis for a user
    """
    global args
    data = UserModel(user_name, args).get_wikis()
    return render_template(u'wiki.html', wikis=data, wiki_ids=data.keys(),
                           topic=u"Top Wikis for User <i>%s</i>" % user_name)


@app.route(u'/')
def index():
    """
    Index page
    """
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

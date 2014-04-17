import argparse
import json
import xlwt
import mimetypes
import StringIO
from flask import Flask, render_template, Response
from werkzeug.datastructures import Headers
from wikia_dstk.authority import add_db_arguments
from nlp_services.caching import use_caching
from models import TopicModel, WikiModel, PageModel, UserModel

use_caching()

app = Flask(__name__)
args = None


def excel_response(spreadsheet, filename=u'export.xls'):
    """
    Prepares an excel spreadsheet for response in Flask
    :param spreadsheet: the spreadsheet
    :type spreadsheet:class:`xlwt.Workbook`
    :param filename: the name of the file when downloaded
    :type filename: unicode
    :return: the flask response
    :rtype:class:`flask.Response`
    """
    response = Response()
    response.status_code = 200
    output = StringIO.StringIO()
    spreadsheet.save(output)
    response.data = output.getvalue()
    mimetype_tuple = mimetypes.guess_type(filename)

    #HTTP headers for forcing file download
    response_headers = Headers({
        u'Pragma': u"public",  # required,
        u'Expires': u'0',
        u'Cache-Control': [u'must-revalidate, post-check=0, pre-check=0', u'private'],
        u'Content-Type': mimetype_tuple[0],
        u'Content-Disposition': u'attachment; filename=\"%s\";' % filename,
        u'Content-Transfer-Encoding': u'binary',
        u'Content-Length': len(response.data)
    })

    if not mimetype_tuple[1] is None:
        response_headers.update({u'Content-Encoding': mimetype_tuple[1]})

    response.headers = response_headers
    response.set_cookie(u'fileDownload', u'true', path=u'/')
    return response


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


@app.route(u'/topic/<topic>/wikis/xls/')
def wikis_for_topic_xls(topic):
    global args
    wkbk = xlwt.Workbook()
    wksht = wkbk.add_sheet(topic)
    titles = [u"Wiki ID", u"Wiki Name", u"Wiki URL", u"Authority"]
    wikis = TopicModel(topic, args).get_wikis(limit=200)[u'wikis']
    print wikis
    keys = [u'id', u'title', u'url', u'authority']
    map(lambda (cell, title): wksht.write(0, cell, title), enumerate(titles))
    map(lambda (row, wiki): map(lambda (cell, key): wksht.write(row+1, cell, wiki[key]),
                                enumerate(keys)),
        enumerate(wikis))

    return excel_response(wkbk, filename=u"%s-wikis.xls" % topic)


@app.route(u'/topic/<topic>/pages/')
def pages_for_topic(topic):
    """
    Shows the top pages for a topic
    """
    global args
    return render_template(u'topic_pages.html', topic=topic, pages=TopicModel(topic, args).get_pages())


@app.route(u'/topic/<topic>/pages/xls/')
def pages_for_topic_xls(topic):
    """
    Gets the excel download of the best pages for a topic
    """
    global args
    workbook = xlwt.Workbook()
    worksheet = workbook.add_sheet(topic)
    titles = [u"Wiki ID", u"Page ID", u"Wiki Name", u"Page URL", u"Page Title", u"Authority"]
    keys = [u'wiki_id', u'page_id', u'wiki', u'full_url', u'title', u'authority']
    pages = TopicModel(topic, args).get_pages(1000)
    map(lambda (cell, title): worksheet.write(0, cell, title), enumerate(titles))
    map(lambda (row, page): map(lambda (cell, key): worksheet.write(row+1, cell, page.get(key, u'?')),
                                enumerate(keys)),
        enumerate(pages))
    return excel_response(workbook, filename=u'%s-pages.xls' % topic)


@app.route(u'/topic/<topic>/users/')
def users_for_topic(topic):
    """
    Shows the top 10 users for a topic
    """
    global args
    return render_template(u'authors.html',
                           authors=TopicModel(topic, args).get_users(),
                           wiki_api_data={u'title': u'Global Authors for %s' % topic, u'url': u'http://www.wikia.com/'})


@app.route(u'/topic/<topic>/users/xls/')
def users_for_topic_xls(topic):
    """
    Spreadsheet of users for a topic
    """
    global args
    workbook = xlwt.Workbook()
    worksheet = workbook.add_sheet(topic)
    titles = [u"Name", u"Authority"]
    keys = [u"user_name", u"total_authority"]
    map(lambda (cell, title): worksheet.write(0, cell, title), enumerate(titles))
    users = TopicModel(topic, args).get_users(limit=1000, with_api=False)
    map(lambda (row, user): map(lambda (cell, key): worksheet.write(row+1, cell, user[key]),
                                enumerate(keys)),
        enumerate(users))
    return excel_response(workbook, filename=u'%s-users.xls' % topic)


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

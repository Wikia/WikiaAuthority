import sys
import inspect
from flask.ext.restful import reqparse
from flask.ext import restful
from .. import models

app_args = None


def register_args(args):
    """
    Registers app args into this module
    :param args: args the args parsed from the command line when running the app
    :type args: argparse.Namespace
    """
    global app_args
    app_args = args


def register_resources(api):
    """
    Dynamically registers all restful resources in this module with the API
    :param api: the restful API object paired to the flask app
    :type api: restful.Api
    """
    for name, obj in inspect.getmembers(sys.modules[__name__]):
        if inspect.isclass(obj):
            api.add_resource(obj, *obj.urls)


def get_request_parser():
    parser = reqparse.RequestParser()
    parser.add_argument(u'limit', type=int, help=u'Limit', default=10)
    parser.add_argument(u'offset', type=int, help=u'Offset', default=0)
    parser.add_argument(u'for_api', type=bool, help=u"For Api: don't touch for now!", default=True)
    return parser


class WikiTopics(restful.Resource):

    urls = [u"/api/wiki/<int:wiki_id>/topics", u"/api/wiki/<int:wiki_id>/topics/"]

    def get(self, wiki_id):
        """
        Access a JSON response for the top topics for the given wiki
        :param wiki_id: the ID of the wiki
        :type wiki_id: int
        :return: the response dict
        :rtype: dict

        .. http:get:: /api/wiki/(int:wiki_id)/topics

           Top topics for this wiki, sorted by authority

           **Example request**:

           .. sourcecode:: http

              GET /wiki/123/topics HTTP/1.1
              Host: authority_api_server.example.com
              Accept: application/json, text/javascript

           **Example response**:

           .. sourcecode:: http

              HTTP/1.1 200 OK
              Vary: Accept
              Content-Type: text/javascript


              {
                  wiki_id: 123,
                  limit: 10,
                  offset: 0,
                  topics: [
                      {
                        topic: "foo",
                        authority: 1.02325,
                      },
                      ...
                  ]
              }

           :query offset: offset number. default is 0
           :query limit: limit number. default is 10
           :resheader Content-Type: application/json
           :statuscode 200: no error
        """
        request_args = get_request_parser().parse_args()
        return {
            u'wiki_id': wiki_id,
            u'offset': request_args[u'offset'],
            u'limit': request_args[u'limit'],
            u'topics': models.WikiModel(wiki_id, app_args).get_topics(**request_args)
        }


class WikiAuthors(restful.Resource):

    urls = [u"/api/wiki/<int:wiki_id>/authors", u"/api/wiki/<int:wiki_id>/authors/"]

    def get(self, wiki_id):
        """
        Access a JSON response for the top authors for the given wiki
        :param wiki_id: the ID of the wiki
        :type wiki_id: int
        :return: the response dict
        :rtype: dict

        .. http:get:: /api/wiki/(int:wiki_id)/authors

           Top authors for this wiki, sorted by authority

           **Example request**:

           .. sourcecode:: http

              GET /wiki/123/authors HTTP/1.1
              Host: authority_api_server.example.com
              Accept: application/json, text/javascript

           **Example response**:

           .. sourcecode:: http

              HTTP/1.1 200 OK
              Vary: Accept
              Content-Type: text/javascript


              {
                  wiki_id: 123,
                  limit: 10,
                  offset: 0,
                  authors: [
                      {
                        id: 1234,
                        name: "Foo_barson",
                        authority: 1.234,
                      },
                      ...
                  ]
              }

           :query offset: offset number. default is 0
           :query limit: limit number. default is 10
           :resheader Content-Type: application/json
           :statuscode 200: no error
        """
        request_args = get_request_parser().parse_args()
        return {
            u'wiki_id': wiki_id,
            u'offset': request_args[u'offset'],
            u'limit': request_args[u'limit'],
            u'authors': models.WikiModel(wiki_id, app_args).get_authors(**request_args)
        }


class WikiPages(restful.Resource):

    urls = [u"/api/wiki/<int:wiki_id>/pages", u"/api/wiki/<int:wiki_id>/pages/"]

    def get(self, wiki_id):
        """
        Access a JSON response for the top pages for the given wiki
        :param wiki_id: the ID of the wiki
        :type wiki_id: int
        :return: the response dict
        :rtype: dict

        .. http:get:: /api/wiki/(int:wiki_id)/pages

           Top pages for this wiki, sorted by authority

           **Example request**:

           .. sourcecode:: http

              GET /wiki/123/pages HTTP/1.1
              Host: authority_api_server.example.com
              Accept: application/json, text/javascript

           **Example response**:

           .. sourcecode:: http

              HTTP/1.1 200 OK
              Vary: Accept
              Content-Type: text/javascript


              {
                  wiki_id: 123,
                  limit: 10,
                  offset: 0,
                  pages: [
                      {
                        id: 1234,
                        authority: 1.234,
                      },
                      ...
                  ]
              }

           :query offset: offset number. default is 0
           :query limit: limit number. default is 10
           :resheader Content-Type: application/json
           :statuscode 200: no error
        """
        request_args = get_request_parser().parse_args()
        return {
            u"wiki_id": wiki_id,
            u'offset': request_args[u'offset'],
            u'limit': request_args[u'limit'],
            u'pages': models.WikiModel(wiki_id, app_args).get_pages(**request_args)
        }


class Wiki(restful.Resource):

    urls = [u"/api/wiki/<int:wiki_id>", u"/api/wiki/<int:wiki_id>/"]

    def get(self, wiki_id):
        """
        Access a JSON response representing data for the wiki, including authority
        :param wiki_id: the ID of the wiki
        :type wiki_id: int
        :return: the response dict
        :rtype: dict

        .. http:get:: /api/wiki/(int:wiki_id)

           Authority data for this wiki

           **Example request**:

           .. sourcecode:: http

              GET /wiki/123/ HTTP/1.1
              Host: authority_api_server.example.com
              Accept: application/json, text/javascript

           **Example response**:

           .. sourcecode:: http

              HTTP/1.1 200 OK
              Vary: Accept
              Content-Type: text/javascript


              {
                  wiki_id: 123,
                  wam_score: 82.2345,
                  title: "foo bar wiki",
                  url: "http://foobar.wikia.com/",
                  authority: 5.234825
              }

           :resheader Content-Type: application/json
           :statuscode 200: no error
        """
        return models.WikiModel(wiki_id, app_args).get_row()


class TopicPages(restful.Resource):

    urls = [u"/api/topic/<string:topic>/pages/", u"/api/topic/<string:topic>/pages"]

    def get(self, topic):
        """
        Access a JSON response for the top pages for the given topic
        :param topic: the topic in question
        :type topic: str
        :return: the response dict
        :rtype: dict

        .. http:get:: /api/topic/(str:topic)/pages

           Top pages for this topic, sorted by global authority

           **Example request**:

           .. sourcecode:: http

              GET /topic/batman/pages HTTP/1.1
              Host: authority_api_server.example.com
              Accept: application/json, text/javascript

           **Example response**:

           .. sourcecode:: http

              HTTP/1.1 200 OK
              Vary: Accept
              Content-Type: text/javascript


              {
                  topic: "batman",
                  limit: 10,
                  offset: 0,
                  pages: [
                      {
                        wiki_url: 'http://batman.wikia.com/',
                        wiki_id: 1234,
                        article_id: 2345,
                        authority: 2.245642
                      },
                      ...
                  ]
              }

           :query offset: offset number. default is 0
           :query limit: limit number. default is 10
           :resheader Content-Type: application/json
           :statuscode 200: no error
        """
        request_args = get_request_parser().parse_args()
        return {
            u'topic': topic,
            u'limit': request_args[u'limit'],
            u'offset': request_args[u'offset'],
            u'pages': models.TopicModel(topic, app_args).get_pages(**request_args)
        }


class TopicWikis(restful.Resource):

    urls = [u"/api/topic/<string:topic>/wikis/", u"/api/topic/<string:topic>/wikis"]

    def get(self, topic):
        """
        Access a JSON response for the top wikis for the given topic
        :param topic: the topic in question
        :type topic: str
        :return: the response dict
        :rtype: dict

        .. http:get:: /api/topic/(str:topic)/wikis

           Top wikis for this topic, sorted by total topic authority

           **Example request**:

           .. sourcecode:: http

              GET /topic/batman/wikis HTTP/1.1
              Host: authority_api_server.example.com
              Accept: application/json, text/javascript

           **Example response**:

           .. sourcecode:: http

              HTTP/1.1 200 OK
              Vary: Accept
              Content-Type: text/javascript


              {
                  topic: "batman",
                  limit: 10,
                  offset: 0,
                  wikis: [
                      {
                        wiki_url: 'http://batman.wikia.com/',
                        wiki_id: 1234,
                        total_topic_authority: 2.245642
                      },
                      ...
                  ]
              }

           :query offset: offset number. default is 0
           :query limit: limit number. default is 10
           :resheader Content-Type: application/json
           :statuscode 200: no error
        """
        request_args = get_request_parser().parse_args()
        return {
            u'topic': topic,
            u'limit': request_args[u'limit'],
            u'offset': request_args[u'offset'],
            u'wikis': models.TopicModel(topic, app_args).get_wikis(**request_args)
        }


class TopicAuthors(restful.Resource):

    urls = [u"/api/topic/<string:topic>/authors", u"/api/topic/<string:topic>/authors/"]

    def get(self, topic):
        """
        Access a JSON response for the top authors for the given topic
        :param topic: the topic in question
        :type topic: str
        :return: the response dict
        :rtype: dict

        .. http:get:: /api/topic/(str:topic)/authors

           Top wikis for this topic, sorted by total topic authority

           **Example request**:

           .. sourcecode:: http

              GET /topic/batman/authors HTTP/1.1
              Host: authority_api_server.example.com
              Accept: application/json, text/javascript

           **Example response**:

           .. sourcecode:: http

              HTTP/1.1 200 OK
              Vary: Accept
              Content-Type: text/javascript


              {
                  topic: "batman",
                  limit: 10,
                  offset: 0,
                  authors: [
                      {
                        id: 12356
                        user_name: "Foo_barson",
                        total_authority: 2.245642
                      },
                      ...
                  ]
              }

           :query offset: offset number. default is 0
           :query limit: limit number. default is 10
           :resheader Content-Type: application/json
           :statuscode 200: no error
        """
        request_args = get_request_parser().parse_args()
        return {
            u'topic': topic,
            u'limit': request_args[u'limit'],
            u'offset': request_args[u'offset'],
            u'authors': models.TopicModel(topic, app_args).get_users(**request_args)
        }


class Topic(restful.Resource):

    urls = [u"/api/topic/<string:topic>", u"/api/topic/<string:topic>/"]

    def get(self, topic):
        """
        Access a JSON response representing data for the topic, including authority
        :param topic: the string value of the topic
        :type topic: str
        :return: the response dict
        :rtype: dict

        .. http:get:: /api/topic/(str:topic)

           Authority data for this wiki

           **Example request**:

           .. sourcecode:: http

              GET /topic/batman/ HTTP/1.1
              Host: authority_api_server.example.com
              Accept: application/json, text/javascript

           **Example response**:

           .. sourcecode:: http

              HTTP/1.1 200 OK
              Vary: Accept
              Content-Type: text/javascript


              {
                  topic_id: 123,
                  topic: "batman",
                  total_authority: 5.234825
              }

           :resheader Content-Type: application/json
           :statuscode 200: no error
        """
        return models.TopicModel(topic, app_args).get_row()


class AuthorWikis(restful.Resource):

    urls = [u"/api/author/<string:user_name>/wikis/", u"/api/author/<string:user_name>/wikis"]

    def get(self, user_name):
        """
        Access a JSON response for the top wikis for the given user
        :param user_name: the name of the user in question
        :type user_name: str
        :return: the response dict
        :rtype: dict

        .. http:get:: /api/user/(str:user_name)/wikis

           Top wikis for this user, sorted by total authority

           **Example request**:

           .. sourcecode:: http

              GET /user/sactage/wikis HTTP/1.1
              Host: authority_api_server.example.com
              Accept: application/json, text/javascript

           **Example response**:

           .. sourcecode:: http

              HTTP/1.1 200 OK
              Vary: Accept
              Content-Type: text/javascript


              {
                  user: "sactage",
                  limit: 10,
                  offset: 0,
                  wikis: [
                      {
                        wiki_url: 'http://batman.wikia.com/',
                        wiki_id: 1234,
                        total_topic_authority: 2.245642
                      },
                      ...
                  ]
              }

           :query offset: offset number. default is 0
           :query limit: limit number. default is 10
           :resheader Content-Type: application/json
           :statuscode 200: no error
        """
        request_args = get_request_parser().parse_args()
        return {
            u'user': user_name,
            u'limit': request_args[u'limit'],
            u'offset': request_args[u'offset'],
            u'wikis': models.UserModel(user_name, app_args).get_wikis(**request_args)
        }


class AuthorTopics(restful.Resource):

    urls = [u"/api/author/<string:user_name>/topics/", u"/api/author/<string:user_name>/topics"]

    def get(self, user_name):
        """
        Access a JSON response for the top topics for the given user
        :param user_name: the name of the user in question
        :type user_name: str
        :return: the response dict
        :rtype: dict

        .. http:get:: /api/user/(str:user_name)/wikis

           Top topics for this user, sorted by total authority

           **Example request**:

           .. sourcecode:: http

              GET /user/sactage/topics HTTP/1.1
              Host: authority_api_server.example.com
              Accept: application/json, text/javascript

           **Example response**:

           .. sourcecode:: http

              HTTP/1.1 200 OK
              Vary: Accept
              Content-Type: text/javascript


              {
                  user: "sactage",
                  limit: 10,
                  offset: 0,
                  topics: [
                      {
                        topic: "foo",
                        authority: 1.02325,
                      },
                      ...
                  ]
              }

           :query offset: offset number. default is 0
           :query limit: limit number. default is 10
           :resheader Content-Type: application/json
           :statuscode 200: no error
        """
        request_args = get_request_parser().parse_args()
        return {
            u'user': user_name,
            u'limit': request_args[u'limit'],
            u'offset': request_args[u'offset'],
            u'topics': models.UserModel(user_name, app_args).get_topics(**request_args).values()
        }


class AuthorPages(restful.Resource):

    urls = [u"/api/author/<string:user_name>/pages/", u"/api/author/<string:user_name>/pages"]

    def get(self, user_name):
        """
        Access a JSON response for the top pages for the given user
        :param user_name: the name of the user in question
        :type user_name: str
        :return: the response dict
        :rtype: dict

        .. http:get:: /api/user/(str:user_name)/pages

           Top pages for this user, sorted by total authority

           **Example request**:

           .. sourcecode:: http

              GET /user/sactage/pages HTTP/1.1
              Host: authority_api_server.example.com
              Accept: application/json, text/javascript

           **Example response**:

           .. sourcecode:: http

              HTTP/1.1 200 OK
              Vary: Accept
              Content-Type: text/javascript


              {
                  user: "sactage",
                  limit: 10,
                  offset: 0,
                  pages: [
                      {
                        wiki_url: 'http://batman.wikia.com/',
                        wiki_id: 1234,
                        article_id: 2345,
                        authority: 2.245642
                      },
                      ...
                  ]
              }

           :query offset: offset number. default is 0
           :query limit: limit number. default is 10
           :resheader Content-Type: application/json
           :statuscode 200: no error
        """
        request_args = get_request_parser().parse_args()
        return {
            u'user': user_name,
            u'limit': request_args[u'limit'],
            u'offset': request_args[u'offset'],
            u'pages': models.UserModel(user_name, app_args).get_pages(**request_args)
        }


class AuthorWikiTopics(restful.Resource):

    urls = [
        u"/api/author/<string:user_name>/topics/wiki/<int:wiki_id>",
        u"/api/author/<string:user_name>/topics/wiki/<int:wiki_id>/",
        u"/api/wiki/<int:wiki_id>/topics/author/<string:user_name>/",
        u"/api/wiki/<int:wiki_id>/topics/author/<string:user_name>",
    ]

    def get(self, user_name, wiki_id):
        """
        Access a JSON response for the top topics for the given user and wiki
        :param user_name: the name of the user in question
        :type user_name: str
        :param wiki_id: the id of the wiki
        :type wiki_id: int
        :return: the response dict
        :rtype: dict

        .. http:get:: /api/user/(str:user_name)/topics/wiki/(int:wiki_id)/

           Drill down the top topics for this user by a given wiki ID

           **Example request**:

           .. sourcecode:: http

              GET /user/sactage/topics/wiki/3125 HTTP/1.1
              Host: authority_api_server.example.com
              Accept: application/json, text/javascript

           **Example response**:

           .. sourcecode:: http

              HTTP/1.1 200 OK
              Vary: Accept
              Content-Type: text/javascript


              {
                  user: "sactage",
                  limit: 10,
                  offset: 0,
                  topics: [
                      {
                        topic: "foo",
                        authority: 1.02325,
                      },
                      ...
                  ]
              }

           :query offset: offset number. default is 0
           :query limit: limit number. default is 10
           :resheader Content-Type: application/json
           :statuscode 200: no error
        """
        request_args = get_request_parser().parse_args()
        return {
            u'user': user_name,
            u'limit': request_args[u'limit'],
            u'offset': request_args[u'offset'],
            u'topics': models.UserModel(user_name, app_args).get_topics_for_wiki(wiki_id, **request_args).values()
        }
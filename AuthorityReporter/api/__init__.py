import json
from flask.ext.restful import reqparse
from flask import request
from flask.ext import restful
from ..app import args as app_args
from .. import models


def get_request_parser():
    parser = reqparse.RequestParser()
    parser.add_argument(u'limit', type=int, help=u'Limit', default=10)
    parser.add_argument(u'offset', type=int, help=u'Offset', default=0)
    return parser


class WikiTopics(restful.Resource):

    def get(self, wiki_id):
        """
        Access a JSON response for the top topics for the given wiki

        .. http:get:: /wiki/(int:wiki_id)/topics

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
                        authors: [
                            {id: 123, user_name: "foobar", total_authority: 0.047},
                             ...
                        ]
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

    def get(self, wiki_id):
        """
        Access a JSON response for the top authors for the given wiki

        .. http:get:: /wiki/(int:wiki_id)/authors

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
            u'authors': models.WikiModel(wiki_id, app_args).get_topics(for_api=True, **request_args)
        }


class WikiPages(restful.Resource):

    def get(self, wiki_id):
        """
        Access a JSON response for the top pages for the given wiki

        .. http:get:: /wiki/(int:wiki_id)/pages

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
            u'pages': models.WikiModel(wiki_id, app_args).get_pages(for_api=True, **request_args)
        }
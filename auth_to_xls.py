import argparse
import requests
import xlwt
from wikia_authority import MinMaxScaler
from nlp_services.caching import use_caching
from nlp_services.authority import WikiAuthorityService, WikiTopicsToAuthorityService, WikiAuthorTopicAuthorityService
from nlp_services.authority import WikiAuthorsToIdsService


def get_args():
    ap = argparse.ArgumentParser()
    ap.add_argument('--wiki-id', dest="wiki_id", required=True,
                    help="The ID of the wiki ")
    ap.add_argument('--num_processes', dest="num_processes", default=96,
                    help="Number of processes to run to compute this shiz")
    return ap.parse_args()


def load_title_list_for_wiki(wiki_url, limit=5000, offset=0, max_pages=None):
    results = requests.get("%sapi/v1/Articles/List" % wiki_url,
                           params={'limit': limit, 'offset': offset, 'namespaces': 0}).json()
    items = results.get('items', [])
    if len(items) < limit or limit+offset > max_pages:
        return items
    else:
        return items + load_title_list_for_wiki(wiki_url, limit=limit, offset=offset+limit)


def get_api_data(wiki_id):
    return requests.get('http://www.wikia.com/api/v1/Wikis/Details',
                        params=dict(ids=wiki_id)).json()['items'][wiki_id]


def get_page_authority(api_data):
    num_pages = api_data['stats']['articles']
    print "Getting title data for %d pages" % num_pages
    page_ids_to_title = {}
    for obj in load_title_list_for_wiki(api_data['url'], max_pages=num_pages):
        page_ids_to_title[obj['id']] = obj['title']

    print "Getting authority data"
    authority_data = WikiAuthorityService().get_value(str(api_data['id']))
    return sorted([(page_ids_to_title[int(z[0].split('_')[-1])], z[1])
                   for z in authority_data.items() if int(z[0].split('_')[-1]) in page_ids_to_title],
                  key=lambda y: y[1], reverse=True)


def get_author_authority(api_data):
    topic_authority_data = WikiAuthorTopicAuthorityService().get_value(str(api_data['id']))

    authors_to_topics = sorted(topic_authority_data['weighted'].items(),
                               key=lambda y: sum(y[1].values()),
                               reverse=True)

    a2ids = WikiAuthorsToIdsService().get_value(str(api_data['id']))

    authors_dict = dict([(x[0], dict(name=x[0],
                                     total_authority=sum(x[1].values()),
                                     topics=sorted(x[1].items(), key=lambda z: z[1], reverse=True)))
                         for x in authors_to_topics])

    user_api_data = requests.get(api_data['url']+'/api/v1/User/Details',
                                 params={'ids': ','.join([str(a2ids[user]) for user, contribs in authors_to_topics]),
                                 'format': 'json'}).json()['items']

    for user_data in user_api_data:
        authors_dict[user_data['name']].update(user_data)
        authors_dict[user_data['name']]['url'] = authors_dict[user_data['name']]['url'][1:]

    author_objects = sorted(authors_dict.values(), key=lambda z: z['total_authority'], reverse=True)
    return author_objects


def main():
    use_caching()
    args = get_args()
    api_data = get_api_data(args.wiki_id)
    sorted(WikiTopicsToAuthorityService().get_value(str(api_data['id'])), key=lambda x: x[1]['authority'])

    workbook = xlwt.Workbook()
    pages_sheet = workbook.add_sheet("Pages by Authority")
    pages_sheet.write(0, 0, "Page")
    pages_sheet.write(0, 1, "Authority")

    for page in get_page_authority(api_data):

    authors_sheet = workbook.add_sheet("Authors by Authority")
    authors_sheet = workbook.add_sheet("Topics for Best Authors")
    topics_sheet = workbook.add_sheet("Topics by Authority")
    topics_authors_sheet = workbook.add_sheet("Authors for Best Topics")





if __name__ == '__main__':
    main()
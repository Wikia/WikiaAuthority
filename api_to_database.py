from lxml import html
from lxml.etree import ParserError
from pygraph.classes.digraph import digraph
from pygraph.algorithms.pagerank import pagerank
import requests
import sys
import multiprocessing
import argparse

try:
    default_cpus = multiprocessing.cpu_count()
except NotImplementedError:
    default_cpus = 2   # arbitrary default

parser = argparse.ArgumentParser(description='Get authoritativeness data for a given wiki.')
parser.add_argument('--wikid-id', dest='wiki_id', action='store', required=True,
                    help='The ID of the wiki you want to operate on')
parser.add_argument('--processes', dest='processes', action='store', type=int, default=default_cpus
                    help='Number of processes you want to run at once')
parser.add_argument('--test-run', dest='test_run', action='store_true', default=False,
                    help='Test run (fewer computations)')
(options, args) = parser.parse_args()

edit_distance_memoization_cache = {}
test_run = options.test_run


class MinMaxScaler:
    """
    Scales values from 0 to 1 by default
    """

    def __init__(self, vals, enforced_min=0, enforced_max=1):
        self.min = min(vals)
        self.max = max(vals)
        self.enforced_min = enforced_min
        self.enforced_max = enforced_max

    def scale(self, val):
        return (((self.enforced_max - self.enforced_min) * (val - self.min))
                / (self.max - self.min)) + self.enforced_min


def get_all_titles(apfrom=None, aplimit=500):
    global api_url, test_run
    params = {'action': 'query', 'list': 'allpages', 'aplimit': aplimit,
              'apfilterredir': 'nonredirects', 'format': 'json'}
    if apfrom is not None:
        params['apfrom'] = apfrom
    response = requests.get(api_url, params=params).json()
    allpages = response.get('query', {}).get('allpages', [])
    if 'query-continue' in response and not test_run:
        return allpages + get_all_titles(apfrom=response['query-continue']['allpages']['apfrom'], aplimit=aplimit)
    return allpages


def get_all_revisions(title_object, rvstartid=None):
    global api_url
    title_string = title_object['title']
    params = {'action': 'query',
              'prop': 'revisions',
              'titles': title_string,
              'rvprop': 'ids|user|userid',
              'rvlimit': 'max',
              'rvdir': 'newer',
              'format': 'json'}
    if rvstartid is not None:
        params['rvstartid'] = rvstartid
    response = requests.get(api_url, params=params).json()
    revisions = response.get('query', {}).get('pages', {0: {}}).values()[0].get('revisions', [])
    if 'query-continue' in response:
        return (title_string, (revisions
                + get_all_revisions(title_object, rvstartid=response['query-continue']['revisions']['rvstartid'])[1]))
    return [title_string, revisions]


def edit_distance(title_object, earlier_revision, later_revision):
    global api_url, edit_distance_memoization_cache
    if (later_revision, earlier_revision) in edit_distance_memoization_cache:
        return edit_distance_memoization_cache[(earlier_revision, later_revision)]
    params = {'action': 'query',
              'prop': 'revisions',
              'rvprop': 'ids|user|userid',
              'rvlimit': '1',
              'format': 'json',
              'rvstartid': earlier_revision,
              'rvdiffto': later_revision,
              'titles': title_object['title']}

    resp = requests.get(api_url, params=params)
    response = resp.json()

    revision = (response.get('query', {})
                        .get('pages', {0: {}})
                        .get(unicode(title_object['pageid']), {})
                        .get('revisions', [{}])[0])
    revision['adds'], revision['deletes'], revision['moves'] = 0, 0, 0
    if ('diff' in revision and '*' in revision['diff']
       and revision['diff']['*'] != '' and revision['diff']['*'] is not False and revision['diff']['*'] is not None):
        try:
            diff_dom = html.fromstring(revision['diff']['*'])
        except TypeError, ParserError:
            return 0
        deleted = [word for span in diff_dom.cssselect('td.diff-deletedline span.diffchange-inline')
                   for word in span.text_content().split(' ')]
        added = [word for span in diff_dom.cssselect('td.diff-addedline span.diffchange-inline')
                 for word in span.text_content().split(' ')]
        adds = sum([1 for word in added if word not in deleted])
        deletes = sum([1 for word in deleted if word not in added])
        moves = sum([1 for word in added if word in deleted])
        changes = revision['adds']+revision['deletes']+revision['moves']  # bad approx. of % of document
        if changes > 0:
            moves /= changes
        distance = max([adds, deletes]) - 0.5 * min([adds, deletes]) + moves
        edit_distance_memoization_cache[(earlier_revision, later_revision)] = distance
        return distance
    return 0


def edit_quality(title_object, revision_i, revision_j):
    numerator = (edit_distance(title_object, revision_i['parentid'], revision_j['revid'])
                 - edit_distance(title_object, revision_i['revid'], revision_j['revid']))

    denominator = edit_distance(title_object, revision_i['parentid'], revision_i['revid'])

    val = numerator if denominator == 0 or numerator == 0 else numerator / denominator

    return -1 if val < 0 else 1  # must be one of[-1, 1]


def get_contributing_authors(arg_tuple):
    global minimum_authors, minimum_contribution_pct
    title_object, title_revs = arg_tuple
    top_authors = []
    try:
        for i in range(1, len(title_revs)):
            prev_rev = title_revs[i-1]
            curr_rev = title_revs[i]
            if 'revid' not in curr_rev or 'revid' not in prev_rev:
                continue

            otherrevs = [title_revs[j] for j in range(i+1, len(title_revs[i+1:i+11]))]
            non_author_revs = filter(lambda x: x.get('user', '') != curr_rev.get('user', ''), otherrevs)
            avg_edit_qty = (sum([edit_quality(title_object, curr_rev, otherrev) for otherrev in non_author_revs])
                            / max(1, len(set([non_author_rev.get('user', '') for non_author_rev in non_author_revs]))))
            curr_rev['edit_longevity'] = (avg_edit_qty
                                          * edit_distance(title_object, prev_rev['revid'], curr_rev['revid']))

        authors = filter(lambda x: x['userid'] != 0 and x['user'] != '',
                         dict([(title_rev.get('userid', 0),
                               {'userid': title_rev.get('userid', 0), 'user': title_rev.get('user', '')}
                                ) for title_rev in title_revs]).values()
                         )

        for author in authors:
            author['contribs'] = sum([title_rev['edit_longevity'] for title_rev in title_revs
                                      if title_rev.get('userid', 0) == author.get('userid', 0)
                                      and 'edit_longevity' in title_rev and title_rev['edit_longevity'] > 0])

        authors = filter(lambda x: x.get('contribs', 0) > 0, authors)

        all_contribs_sum = sum([a['contribs'] for a in authors])

        for author in authors:
            author['contrib_pct'] = author['contribs']/all_contribs_sum

        for author in sorted(authors, key=lambda x: x['contrib_pct'], reverse=True):
            if 'user' not in author:
                continue
            if author['contrib_pct'] < minimum_contribution_pct and len(top_authors) >= minimum_authors:
                break
            top_authors += [author]

    except IndexError:
        print title, sys.exc_info()

    return title_object['title'], top_authors


def links_for_page(title_object, plcontinue=None):
    global api_url
    title_string = title_object['title']
    params = {'action': 'query', 'titles': title_string, 'plnamespace': 0,
              'prop': 'links', 'pllimit': 500, 'format': 'json'}
    if plcontinue is not None:
        params['plcontinue'] = plcontinue
    response = requests.get(api_url, params=params).json()
    links = [link['title'] for link in response.get('query', {}).get('pages', {0: {}}).values()[0].get('links', [])]
    query_continue = response.get('query-continue', {}).get('links', {}).get('plcontinue')
    if query_continue is not None:
        return title_string, links + links_for_page(title_object, plcontinue=response['query-continue'])[1]
    return title_string, links


def get_pagerank(titles):
    global options
    pool = multiprocessing.Pool(processes=options.processes)
    all_links = pool.map(links_for_page, titles)
    all_title_strings = list(set([to_string for response in all_links for to_string in response[1]]
                                 + [obj['title'] for obj in all_titles]))

    wiki_graph = digraph()
    wiki_graph.add_nodes(all_title_strings)  # to prevent missing node_neighbors table
    map(wiki_graph.add_edge,
        [(title_object['title'], target) for title_object in all_titles for target in links_for_page(title_object)[1]])

    return pagerank(wiki_graph)


def author_centrality(titles_to_authors):
    author_graph = digraph()
    author_graph.add_nodes(map(lambda x: "title_%s" % x, titles_to_authors.keys()))
    author_graph.add_nodes(list(set(['author_%s' % author['user']
                                     for title in titles_to_authors
                                     for author in titles_to_authors[title]])))
    map(author_graph.add_edge,
        [('title_%s' % title, 'author_%s' % author['user'])
         for title in titles_to_authors
         for author in titles_to_authors[title]])

    return dict([('_'.join(item[0].split('_')[1:]), item[1])
                 for item in pagerank(author_graph).items() if item[0].startswith('author_')])


wiki_id = sys.argv[1]
test_run = len(sys.argv) >= 3
minimum_authors = 5
minimum_contribution_pct = 0.01

aplimit = 500 if not test_run else 10

# get wiki info
wiki_data = requests.get('http://www.wikia.com/api/v1/Wikis/Details', params={'ids': wiki_id}).json()['items'][wiki_id]
api_url = '%sapi.php' % wiki_data['url']

# can't be parallelized since it's an enum
all_titles = get_all_titles(aplimit=aplimit)
print "Got %d titles" % len(all_titles)

pool = multiprocessing.Pool(processes=options.processes)

all_revisions = []
r = pool.map_async(get_all_revisions, all_titles, callback=all_revisions.extend)
r.wait()
print "%d Revisions" % sum([len(revs) for title, revs in all_revisions])
all_revisions = dict(all_revisions)

title_top_authors = {}

r = pool.map_async(get_contributing_authors,
                   [(title_obj, all_revisions[title_obj['title']]) for title_obj in all_titles],
                   callback=title_top_authors.update)
r.wait()

centralities = author_centrality(title_top_authors)

centrality_scaler = MinMaxScaler(centralities.values())
contribs_scaler = MinMaxScaler([author['contribs']
                                for title in title_top_authors
                                for author in title_top_authors[title]])

# this com_qscore_pr, the best metric per Qin and Cunningham
print [(title,
        sum([contribs_scaler.scale(author['contribs']) * centrality_scaler.scale(centralities[author['user']])
             for author in title_top_authors[title]]))
       for title in title_top_authors]

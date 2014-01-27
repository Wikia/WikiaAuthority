from boto import connect_s3
from lxml import html
from lxml.etree import ParserError
from pygraph.classes.digraph import digraph
from pygraph.algorithms.pagerank import pagerank
import json
import requests
import sys
import multiprocessing
import argparse
import time

try:
    default_cpus = multiprocessing.cpu_count()
except NotImplementedError:
    default_cpus = 2   # arbitrary default

parser = argparse.ArgumentParser(description='Get authoritativeness data for a given wiki.')
parser.add_argument('--wiki-id', dest='wiki_id', action='store', required=True,
                    help='The ID of the wiki you want to operate on')
parser.add_argument('--processes', dest='processes', action='store', type=int, default=default_cpus,
                    help='Number of processes you want to run at once')
parser.add_argument('--test-run', dest='test_run', action='store_true', default=False,
                    help='Test run (fewer computations)')
options = parser.parse_args()

edit_distance_memoization_cache = {}
test_run = options.test_run

smoothing = 0.001


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
    global api_url
    params = {'action': 'query', 'list': 'allpages', 'aplimit': aplimit,
              'apfilterredir': 'nonredirects', 'format': 'json'}
    if apfrom is not None:
        params['apfrom'] = apfrom
    resp = requests.get(api_url, params=params)
    response = resp.json()
    resp.close()
    allpages = response.get('query', {}).get('allpages', [])
    if 'query-continue' in response:
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
    resp = requests.get(api_url, params=params)
    response = resp.json()
    resp.close()
    revisions = response.get('query', {}).get('pages', {0: {}}).values()[0].get('revisions', [])
    if 'query-continue' in response:
        return (title_string, (revisions
                + get_all_revisions(title_object, rvstartid=response['query-continue']['revisions']['rvstartid'])[1]))
    return [title_string, revisions]


def edit_distance(title_object, earlier_revision, later_revision, already_retried=False):
    global api_url, edit_distance_memoization_cache
    if (earlier_revision, later_revision) in edit_distance_memoization_cache:
        return edit_distance_memoization_cache[(earlier_revision, later_revision)]
    params = {'action': 'query',
              'prop': 'revisions',
              'rvprop': 'ids|user|userid',
              'rvlimit': '1',
              'format': 'json',
              'rvstartid': earlier_revision,
              'rvdiffto': later_revision,
              'titles': title_object['title']}

    try:
        resp = requests.get(api_url, params=params)
    except requests.exceptions.ConnectionError as e:
        if already_retried:
            print "Gave up on some socket shit"
            return 0
        print "Fucking sockets"
        time.sleep(240) # wait four minutes for your wimpy ass sockets to get their shit together
        return edit_distance(title_object, earlier_revision, later_revision, already_retried=True)

    response = resp.json()
    resp.close()
    time.sleep(0.025)  # prophylactic throttling
    revision = (response.get('query', {})
                        .get('pages', {0: {}})
                        .get(unicode(title_object['pageid']), {})
                        .get('revisions', [{}])[0])
    revision['adds'], revision['deletes'], revision['moves'] = 0, 0, 0
    if ('diff' in revision and '*' in revision['diff']
       and revision['diff']['*'] != '' and revision['diff']['*'] is not False and revision['diff']['*'] is not None):
        try:
            diff_dom = html.fromstring(revision['diff']['*'])
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
        except (TypeError, ParserError, UnicodeEncodeError):
            return 0
    return 0


def edit_quality(title_object, revision_i, revision_j):

    numerator = (edit_distance(title_object, revision_i['parentid'], revision_j['revid'])
                 - edit_distance(title_object, revision_i['revid'], revision_j['revid']))

    denominator = edit_distance(title_object, revision_i['parentid'], revision_i['revid'])

    val = numerator if denominator == 0 or numerator == 0 else numerator / denominator
    return -1 if val < 0 else 1  # must be one of[-1, 1]


def get_contributing_authors_safe(arg_tuple):
    global wiki_id
    try:
        res = get_contributing_authors(arg_tuple)
    except Exception as e:
        print arg_tuple, e
        return str(wiki_id) + '_' + str(arg_tuple[0]['pageid']), []
    return res


def get_contributing_authors(arg_tuple):
    global minimum_authors, minimum_contribution_pct, smoothing, wiki_id

    #  within scope of map_async subprocess
    requests.Session().mount('http://', requests.adapters.HTTPAdapter(pool_connections=1, pool_maxsize=1, pool_block=True))

    title_object, title_revs = arg_tuple
    doc_id = "%s_%s" % (str(wiki_id), title_object['pageid'])
    top_authors = []
    if len(title_revs) == 1 and 'user' in title_revs[0]:
        title_revs[0]['contrib_pct'] = 1
        title_revs[0]['contribs'] = 1
        return doc_id, title_revs

    for i in range(0, len(title_revs)):
        curr_rev = title_revs[i]
        if i == 0:
            edit_dist = 1
        else:
            prev_rev = title_revs[i-1]
            if 'revid' not in curr_rev or 'revid' not in prev_rev:
                continue
            edit_dist = edit_distance(title_object, prev_rev['revid'], curr_rev['revid'])

        non_author_revs_comps = [(title_revs[j-1], title_revs[j]) for j in range(i+1, len(title_revs[i+1:i+11]))
                                 if title_revs[j].get('user', '') != curr_rev.get('user')]
        
        avg_edit_qty = (sum(map(lambda x: edit_quality(title_object, x[0], x[1]), non_author_revs_comps))
                        / max(1, len(set([non_author_rev_cmp[1].get('user', '') for non_author_rev_cmp in non_author_revs_comps]))))
        if avg_edit_qty == 0:
            avg_edit_qty = smoothing
        curr_rev['edit_longevity'] = avg_edit_qty * edit_dist

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
    return doc_id, top_authors


def links_for_page(title_object, plcontinue=None):
    global api_url
    title_string = title_object['title']
    params = {'action': 'query', 'titles': title_string, 'plnamespace': 0,
              'prop': 'links', 'pllimit': 500, 'format': 'json'}
    if plcontinue is not None:
        params['plcontinue'] = plcontinue
    resp = requests.get(api_url, params=params)
    response = resp.json()
    resp.close()
    links = [link['title'] for link in response.get('query', {}).get('pages', {0: {}}).values()[0].get('links', [])]
    query_continue = response.get('query-continue', {}).get('links', {}).get('plcontinue')
    if query_continue is not None:
        return title_string, links + links_for_page(title_object, plcontinue=response['query-continue'])[1]
    return title_string, links


def get_pagerank(titles):
    global options
    pool = multiprocessing.Pool(processes=options.processes)
    r = pool.map_async(links_for_page, titles)
    r.wait()
    all_links = r.get()
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
                                     for authors in titles_to_authors.values()
                                     for author in authors])))
    map(author_graph.add_edge,
        [('title_%s' % title, 'author_%s' % author['user'])
         for title in titles_to_authors
         for author in titles_to_authors[title]])

    centralities = dict([('_'.join(item[0].split('_')[1:]), item[1])
                         for item in pagerank(author_graph).items() if item[0].startswith('author_')])

    centrality_scaler = MinMaxScaler(centralities.values())

    return dict([(cent_author, centrality_scaler.scale(cent_val))
                  for cent_author, cent_val in centralities.items()])


def get_title_top_authors(all_titles, all_revisions):
    global options
    pool = multiprocessing.Pool(processes=options.processes)
    title_top_authors = {}
    r = pool.map_async(get_contributing_authors_safe,
                       [(title_obj, all_revisions[title_obj['title']]) for title_obj in all_titles],
                       callback=title_top_authors.update)
    r.wait()
    if len(title_top_authors) == 0:
        print r.get()
        sys.exit()
    
    contribs_scaler = MinMaxScaler([author['contribs']
                                for title in title_top_authors
                                for author in title_top_authors[title]])
    scaled_title_top_authors = {}
    for title, authors in title_top_authors.items():
        new_authors = []
        for author in authors:
            author['contribs'] = contribs_scaler.scale(author['contribs'])
            new_authors.append(author)
        scaled_title_top_authors[title] = new_authors
    return scaled_title_top_authors


start = time.time()

wiki_id = options.wiki_id
print "wiki id is", wiki_id

test_run = len(sys.argv) >= 3
minimum_authors = 5
minimum_contribution_pct = 0.01

# get wiki info
resp = requests.get('http://www.wikia.com/api/v1/Wikis/Details', params={'ids': wiki_id})
wiki_data = resp.json()['items'][wiki_id]
resp.close()
print wiki_data['title']
api_url = '%sapi.php' % wiki_data['url']

# can't be parallelized since it's an enum
all_titles = get_all_titles()
print "Got %d titles" % len(all_titles)

pool = multiprocessing.Pool(processes=options.processes)

all_revisions = []
r = pool.map_async(get_all_revisions, all_titles, callback=all_revisions.extend)
r.wait()
print "%d Revisions" % sum([len(revs) for title, revs in all_revisions])
all_revisions = dict(all_revisions)

title_top_authors = get_title_top_authors(all_titles, all_revisions)

print time.time() - start

centralities = author_centrality(title_top_authors)

# this com_qscore_pr, the best metric per Qin and Cunningham
comqscore_authority = dict([('%s_%s' % (str(wiki_id), str(pageid)),
                             sum([author['contribs'] * centralities[author['user']]
                                  for author in authors])
                             ) for pageid, authors in title_top_authors.items()])

print "Got comsqscore"
title_to_pageid = dict([(title_object['title'], title_object['pageid']) for title_object in all_titles])
pr = dict([('%s_%s' % (str(wiki_id), title_to_pageid[title]), pagerank)
           for title, pagerank in get_pagerank(all_titles).items() if title in title_to_pageid])

print "Got PR"
print "Finished getting all data, now storing it..."
print time.time() - start

bucket = connect_s3().get_bucket('nlp-data')
key = bucket.new_key(key_name='service_responses/%s/WikiAuthorCentralityService.get' % wiki_id)
key.set_contents_from_string(json.dumps(centralities, ensure_ascii=False))

key = bucket.new_key(key_name='service_responses/%s/WikiAuthorityService.get' % wiki_id)
key.set_contents_from_string(json.dumps(comqscore_authority, ensure_ascii=False))

for doc_id, dct in title_top_authors.items():
    key = bucket.new_key(key_name='service_responses/%s/PageAuthorityService.get' % (doc_id.replace('_', '/')))
    key.set_contents_from_string(json.dumps(dct, ensure_ascii=False))

key = bucket.new_key(key_name='service_responses/%s/WikiPageRankService.get')
key.set_contents_from_string(json.dumps(pr, ensure_ascii=False))

print wiki_id, "finished in", time.time() - start, "seconds"

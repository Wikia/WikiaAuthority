from lxml import html
from lxml.etree import ParserError
from pygraph.classes.digraph import digraph
from pygraph.algorithms.pagerank import pagerank
import requests
import sys
import multiprocessing
import json


edit_distance_memoization_cache = {}
test_run = False


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
    return title_string, revisions


def edit_distance(title, earlier_revision, later_revision):
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
              'titles': title}
    resp = requests.get(api_url, params=params)
    response = resp.json()
    revision = response.get('query', {}).get('pages', {0: {}}).values()[0].get('revisions', [])[0]
    revision['adds'], revision['deletes'], revision['moves'] = 0, 0, 0
    if ('diff' in revision
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
        # normalization
        distance = distance if distance >= -1 else -1
        distance = distance if distance <= 1 else 1
        return distance
    return 0


def edit_quality(title_string, revision_i, revision_j):
    return ((edit_distance(title_string, revision_i['parentid'], revision_j['revid'])
            - edit_distance(title_string, revision_i['revid'], revision_j['revid']))
            / edit_distance(title_string, revision_i['parentid'], revision_i['revid']))


def get_contributing_authors(arg_tuple):
    global minimum_authors, minimum_contribution_pct
    title_object, title_revs = arg_tuple
    title = title_object['title']
    edit_longevity = dict()
    for i in range(1, len(title_revs)-1):
        prev_rev = title_revs[i-1]
        curr_rev = title_revs[i]
        if 'revid' not in curr_rev or 'revid' not in prev_rev:
            continue
        otherrevs = [title_revs[j] for j in range(i+1, len(title_revs[i+1:i+11]))]
        non_author_revs = filter(lambda x: x.get('userid', 0) != curr_rev.get('userid', 0), otherrevs)
        average_edit_quality = (
            sum(
                [edit_distance(title, curr_rev['revid'], otherrev['revid'])
                 for otherrev in non_author_revs]
            )
            * max([1, len(set([non_author_rev.get('userid', 0) for non_author_rev in non_author_revs]))])
        )
        edit_longevity[curr_rev['revid']] = (average_edit_quality
                                             * edit_distance(title, prev_rev['revid'], curr_rev['revid']))
    authors = list(set([title_rev.get('userid', 0) for title_rev in title_revs]))
    title_contribs = {}
    for author in authors:
        title_contribs[author] = sum([edit_longevity[title_rev['revid']] for title_rev in title_revs
                                      if title_rev.get('userid', 0) == author and title_rev['revid'] in edit_longevity])

    all_contribs_sum = sum(title_contribs.values())
    top_authors_to_contrib = []
    top_author_pct = 0
    for author in authors:
        if author != 0 and title_contribs[author] > 0:
            author_contrib_pct = float(title_contribs[author])/all_contribs_sum
            top_authors_to_contrib += [(author, author_contrib_pct)]
            top_author_pct += author_contrib_pct
        if len(top_authors_to_contrib) >= minimum_authors and top_author_pct >= minimum_contribution_pct:
            break

    return [title, sorted(top_authors_to_contrib, key=lambda x: x[1], reverse=True)]


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
    global cpus
    pool = multiprocessing.Pool(processes=cpus)
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
    author_graph.add_nodes(list(set(['author_%s' % author for value in titles_to_authors.values()
                                     for author, contrib in value])))
    map(author_graph.add_edge,
        [('title_%s' % title, 'author_%s' % author)
         for title in titles_to_authors
         for author, contrib in titles_to_authors[title]])

    # casting to int here but maybe want to use username instead?
    return dict([(int('_'.join(item[0].split('_')[1:])), item[1])
                 for item in pagerank(author_graph).items() if item[0].startswith('author_')])


try:
    cpus = multiprocessing.cpu_count()
except NotImplementedError:
    cpus = 2   # arbitrary default

wiki_id = sys.argv[1]
test_run = len(sys.argv) >= 3
minimum_authors = 5
minimum_contribution_pct = 0.7

aplimit = 500 if not test_run else 10

# get wiki info
wiki_data = requests.get('http://www.wikia.com/api/v1/Wikis/Details', params={'ids': wiki_id}).json()['items'][wiki_id]
api_url = '%sapi.php' % wiki_data['url']

# can't be parallelized since it's an enum
all_titles = get_all_titles(api_url, aplimit=aplimit)
print "Got %d titles" % len(all_titles)

pool = multiprocessing.Pool(processes=cpus)
all_revisions = dict(pool.map(get_all_revisions, all_titles))

print "%d Revisions" % sum([len(rev) for rev in all_revisions])

top_authors = dict(pool.map(get_contributing_authors,
                            [(title_obj, all_revisions[title_obj['title']]) for title_obj in all_titles]))

centralities = author_centrality(dict(top_authors))

# this com_qscore_pr, the best metric per Qin and Cunningham
print [(title['title'], sum([score * centralities[author] for author, score in top_authors[title['title']]]))
       for title in all_titles]

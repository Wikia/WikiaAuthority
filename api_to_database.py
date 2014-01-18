from lxml import html
from nltk import bigrams
import requests
import sys
import multiprocessing


edit_distance_memoization_cache = {}


def get_all_titles(apfrom=None):
    global api_url
    params = {'action': 'query', 'list': 'allpages', 'aplimit': 500, 'apfilterredir': 'nonredirects', 'format': 'json'}
    if apfrom is not None:
        params['apfrom'] = apfrom
    response = requests.get(api_url, params=params).json()
    allpages = response.get('query', {}).get('allpages', [])
    if 'query-continue' in response:
        return allpages + get_all_titles(apfrom=response['query-continue']['allpages']['apfrom'])
    return allpages


def get_all_revisions(title, rvstartid=None):
    global api_url
    params = {'action': 'query',
              'prop': 'revisions',
              'titles': title,
              'rvprop': 'ids|user|userid',
              'rvlimit': 'max',
              'rvdir': 'newer',
              'format': 'json'}
    if rvstartid is not None:
        params['rvstartid'] = rvstartid
    response = requests.get(api_url, params=params).json()
    revisions = response.get('query', {}).get('pages', {0: {}}).values()[0].get('revisions', [])
    if 'query-continue' in response:
        return (revisions
                + get_all_revisions(title, rvstartid=response['query-continue']['revisions']['rvstartid']))
    return revisions


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
    if 'diff' in revision and revision['diff']['*'] != '':
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
        # normalization
        distance = distance if distance >= -1 else -1
        distance = distance if distance <= 1 else 1
        return distance
    return 0


def edit_quality(title, revision_i, revision_j):
    return ((edit_distance(title, revision_i['parentid'], revision_j['revid'])
            - edit_distance(title, revision_i['revid'], revision_j['revid']))
            / edit_distance(title, revision_i['parentid'], revision_i['revid']))


try:
    cpus = multiprocessing.cpu_count()
except NotImplementedError:
    cpus = 2   # arbitrary default


wiki_id = sys.argv[1]

# get wiki info
wiki_data = requests.get('http://www.wikia.com/api/v1/Wikis/Details', params={'ids': wiki_id}).json()['items'][wiki_id]
api_url = '%sapi.php' % wiki_data['url']

all_titles = get_all_titles(api_url)
print "Got %d titles" % len(all_titles)

pool = multiprocessing.Pool(processes=cpus)
all_revisions = pool.map(get_all_revisions, [page['title'] for page in all_titles])
print "%d Revisions" % sum([len(rev) for rev in all_revisions])

edit_longevity = {}
contribs = {}

# parallelize
for k in range(0, len(all_titles)):
    title_obj = all_titles[k]
    title = title_obj['title']
    title_revs = all_revisions[k]
    for i in range(1, len(title_revs)-1):
        prev_rev = title_revs[i-1]
        curr_rev = title_revs[i]
        otherrevs = [title_revs[j] for j in range(i+1, len(title_revs[i+1:i+11]))]
        non_author_revs = filter(lambda x: x['userid'] != curr_rev['userid'], otherrevs)
        average_edit_quality = (
            sum(
                [edit_distance(title, curr_rev['revid'], otherrev['revid'])
                 for otherrev in non_author_revs]
            )
            * max([1, len(set([rev['userid'] for rev in non_author_revs]))])
        )
        edit_longevity[curr_rev['revid']] = (average_edit_quality
                                             * edit_distance(title, prev_rev['revid'], curr_rev['revid']))
    authors = list(set([rev['userid'] for rev in title_revs]))
    title_contribs = {}
    for author in authors:
        title_contribs[author] = sum([edit_longevity[rev['revid']] for rev in title_revs
                                      if rev['userid'] == author and rev['revid'] in edit_longevity])
    contribs[title_obj['pageid']] = title_contribs

from models import Revision


def as_revision(revision_or_id):
    return revision_or_id if isinstance(revision_or_id, Revision) else Revision(revision_or_id)


def words_added(revision_i, revision_j):
    return as_revision(revision_i).words_added(as_revision(revision_j))


def words_deleted(revision_i, revision_j):
    return as_revision(revision_i).words_deleted(as_revision(revision_j))


def words_moved(revision_i, revision_j):
    return as_revision(revision_i).words_moved(as_revision(revision_j))


def parent(revision):
    return as_revision(revision).parent


def edit_distance(revision_i, revision_j):
    changes = [len(words_added(revision_i, revision_j)),
               len(words_deleted(revision_i, revision_j))]
    max_change = max(changes)
    min_change = min(changes)
    return max_change - 0.5 * min_change + len(words_moved(revision_i, revision_j))


def edit_quality(revision_i, revision_j):
    return (
        (edit_distance(parent(revision_i), revision_j) - edit_distance(revision_i, revision_j))
        /
        edit_distance(parent(revision_i), revision_i)
    )


def edit_contribution(revision):
    return edit_distance(revision.parent, revision)


def child_chain(revision, num_revisions):
    if revision.child is None:
        return []
    chain = [revision.child]
    for i in range(0, num_revisions-1):
        if chain[-1].child is not None:
            chain.append(chain[-1].child)
        else:
            break
    return chain


def average_edit_quality(revision_i, revision_j):
    different_authors = [rev for rev in child_chain(revision_i, 10) if rev.author is not revision_i.author]
    if len(different_authors) == 0:
        return edit_quality(revision_i, revision_j)
    return(
        1.0 / len(different_authors)
        * sum([edit_quality(revision_i, revision_J) for revision_J in different_authors])
    )


def edit_longevity(revision):
    average_edit_quality(revision, revision.child) * edit_distance(revision)


def author_page_contributions(author, page):
    return sum(
        filter(
            lambda x: x > 0,
            [edit_longevity(revision) for revision in page.revisions_for_author(author)]
        )
    )


def all_page_contributions(page):
    return [(revision.author, edit_contribution(revision)) for revision in page.revisions]


def main_contributors(page,
                      minimum_contribution_threshold=None,
                      percentage_threshold=0.1):
    all_contributions = all_page_contributions(page)
    total_contributions = sum(map(lambda x: x[1], all_contributions))
    authored_revisions = filter(lambda x: not x[0].anonymous, all_contributions)
    worthwhile_authors = set(map(lambda x: x[0], authored_revisions))
    author_contributions = sorted(
        [(author, author_page_contributions(author, page)/total_contributions)
         for author in worthwhile_authors],
        key=lambda x: x[1]
    )
    if minimum_contribution_threshold is not None:
        author_contributions = filter(lambda x: x[1] >= minimum_contribution_threshold,  author_contributions)
    current_percentage = 0
    return_authors = []
    while (current_percentage < percentage_threshold) and (len(author_contributions) > 0):
        return_authors += author_contributions.pop()
        current_percentage += return_authors[-1][1]
    return map(lambda x: x[0], return_authors)






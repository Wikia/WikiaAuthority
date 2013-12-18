from edit_longevity import author_page_contributions


def longevity_quality_score(page):
    return sum([author_page_contributions(author, page) for author in page.authors])


def centrality_quality_score(page, centrality_function):
    return sum([centrality_function(author) for author in page.authors])


def centralized_author_page_contributions(author, page, centrality_function):
    return author_page_contributions(author, page) * centrality_function(author)


def combined_quality_score(page, centrality_function):
    return sum([centralized_author_page_contributions(author, page, centrality_function) for author in page.authors])


def betweenness_centrality(x):
    # both author and revision have betweenness
    return x.betwenness

"""
Other centrality scores to consider implementing:
* pagerank
* eigenvector
* degree
"""
"""
pretty sure this never got used
"""

"""
These models wrap drivers that extract this data
"""


class Author:

    def __init__(self, author_id, driver=None):
        self.driver = driver
        self.author_id = author_id

    @property
    def anonymous(self):
        return self.driver.is_author_anonymous(self.author_id)

    @property
    def betweenness(self):
        return (self.driver.shortest_page_paths_for_author(self.author_id)
                / self.driver.all_shortest_page_paths_by_author())


class Revision:

    def __init__(self, revision_id, driver=None):
        self.driver = driver
        self.revision_id = revision_id

    def words_deleted(self, compare_revision):
        return self.driver.words_deleted(self.revision_id, compare_revision.revision_id)

    def words_added(self, compare_revision):
        return self.driver.words_added(self.revision_id, compare_revision.revision_id)

    def words_moved(self, compare_revision):
        return self.driver.words_moved(self.revision_id, compare_revision.revision_id)

    @property
    def parent(self):
        return Revision(self.driver.parent(self.revision_id), self.driver)

    @property
    def children(self):
        # specified for future uses in other things
        return [Revision(rev_id, self.driver) for rev_id in self.driver.revision_children(self.revision_id)]

    @property
    def child(self):
        return self.children[0]

    @property
    def author(self):
        return Author(self.driver.author_for_revision(self.revision_id), self.driver)


class Page:

    def __init__(self, page_id, driver=None):
        self.driver = driver
        self.page_id = page_id

    @property
    def revisions_for_author(self, author):
        return [Revision(rev_id, self.driver)
                for rev_id in self.driver.page_revisions_for_author(self.page_id, author.id)]

    @property
    def authors(self):
        return [Author(author_id, self.driver) for author_id in self.driver.page_authors(self.page_id)]

    @property
    def revisions(self):
        return [Revision(rev_id, self.driver) for rev_id in self.driver.revisions_for_page(self.page_id)]

    @property
    def betweenness(self):
        return self.driver.shortest_paths_for_page(self.page_id) / self.driver.all_shortest_page_paths()




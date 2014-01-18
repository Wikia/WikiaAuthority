import requests

#http://muppet.wikia.com/api.php?action=query&prop=revisions&pageids=50&rvprop=user&rvlimit=500
class DriverAPI:

    def __init__(self, url):
        self.url = url
        self._revision_data = None

    def is_author_anonymous(self, author_id):
        """
        Got an ID, don't it?!
        """
        return author_id != 0

    def shortest_page_paths_for_author(self, author_id):
        pass

    def all_shortest_page_paths_by_author(self):
        pass

    def words_deleted(self, revision_id_a, revision_id_b):
        pass

    def words_added(self, revision_id_a, revision_id_b):
        pass

    def words_moved(self, revision_id_a, revision_id_b):
        pass

    def revision_children(self, revision_id):
        pass

    def author_for_revision(self, revision_id):

        pass

    def page_revisions_for_author(self, page_id, author_id):
        pass

    def page_authors(self, page_id):
        pass

    def revisions_for_page(self, page_id):
        pass

    def shortest_paths_for_page(self, page_id):
        pass

    def all_shortest_page_paths(self):
        pass
class JSONMapping(object):
    def __init__(self, mapping):
        self.mapping = mapping

    def transform(self, query, fields=None, include=None):
        pass

    def update(self, data):
        pass

    def insert(self, data):
        pass

    def delete(self, data):
        pass

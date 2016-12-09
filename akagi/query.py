import re


class Query(object):
    def __init__(self, query):
        self.query = query

    def wrap(self, query):
        raise NotImplementedError


class UnloadQuery(Query):
    def __init__(self, query, bundle, sort=False):
        super(UnloadQuery, self).__init__(query)

        self.bundle = bundle
        self.sort = sort

    @classmethod
    def wrap(cls, query, bundle, sort=False):
        return UnloadQuery(query, bundle, sort)

    @property
    def sql(self):
        return """
unload ('%(query)s')
to '%(bundle_url)s'
credentials '%(credential_string)s'
gzip
allowoverwrite
parallel %(enable_sort)s
delimiter ',' escape addquotes
            """ % ({
                'query': re.sub(r"'", "\\\\'", self.query),
                'bundle_url': self.bundle.url,
                'credential_string': self.bundle.credential_string,
                'enable_sort': "on" if self.sort else "off"
                })

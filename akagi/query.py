import re
import six


class Query(object):
    def __init__(self, body):
        self._body = body

    def wrap(self, query):
        raise NotImplementedError

    @property
    def body(self):
        if six.PY2:
            return self._body.decode('utf-8')
        else:
            return self._body

    def __str__(self):
        return self.body


class UnloadQuery(Query):
    def __init__(self, body, bundle, sort=False):
        super(UnloadQuery, self).__init__(body)

        self.bundle = bundle
        self.sort = sort

    @classmethod
    def wrap(cls, query_str, bundle, sort=False):
        return UnloadQuery(query_str, bundle, sort)

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
                'query': re.sub(r"'", "\\\\'", self.body),
                'bundle_url': self.bundle.url,
                'credential_string': self.bundle.credential_string,
                'enable_sort': "on" if self.sort else "off"
                })

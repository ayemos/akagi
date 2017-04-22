import os

from akagi.log import logger


class Datasource(object):
    '''Datasource is an base class of all data sources
    '''

    def save(self, tar_dir):
        for d in self.bundle.data_files:

            path = os.path.expanduser(os.path.join(tar_dir, d.key))
            dirname = os.path.dirname(path)

            if not os.path.isdir(dirname):
                os.makedirs(dirname)

            logger.info("Saved %(key)s to %(path)s" % ({"key": d.key, "path": path}))

            with open(path, 'wb') as f:
                f.write(d.content)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        raise NotImplementedError

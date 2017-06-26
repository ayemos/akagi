import os

from akagi.log import logger


class DataSource(object):
    '''DataSource is an base class of all data sources
    '''

    def __init__(self, name, bundle):
        self.name = name
        self.bundle = self._find_bundle or bundle

        self.freeze()
        self.save()

    def save(self, tar_dir, force=False):
        paths = []

        for d in self.bundle.data_files:
            path = os.path.expanduser(os.path.join(tar_dir, d.key))
            dirname = os.path.dirname(path)

            if not os.path.isdir(dirname):
                os.makedirs(dirname)

            if os.path.isfile(path):
                logger.debug("Skipped %(key)s" % ({"key": d.key}))
            else:
                with open(path, 'wb') as f:
                    f.write(d.raw_content)
                    logger.debug("Saved %(key)s to %(path)s" % ({"key": d.key, "path": path}))

            paths.append(path)

        return paths

    def freeze(self):
        pass

    def _find_bundle(self, bundle):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        raise NotImplementedError

    def __iter__(self):
        return iter(self.bundle)

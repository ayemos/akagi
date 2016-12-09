import os


class Datasource(object):
    '''Datasource is an base class of all data sources
    '''

    def save(self, tar_dir):
        for d in self.bundle.data_files:
            path = os.path.expanduser(os.path.join(tar_dir, d.key))
            dirname = os.path.dirname(path)

            if not os.path.isdir(dirname):
                os.makedirs(dirname)

            with open(path, 'wb') as f:
                f.write(d.content)

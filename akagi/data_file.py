import os


class DataFile(object):
    def __iter__(self):
        return self

    """
    def each_row(&block)
      f = if gzipped_object?
            Zlib::GzipReader.new(content)
          else
            content
          end
      @reader_class.new(f).each(&block)
    ensure
      content.close
    end
    """

    def __next__(self):
        raise NotImplementedError


    def _is_gzip(self):
        return os.path.splitext(self.filename)[-1] == '.gz'

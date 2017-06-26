from akagi.data_source import DataSource


class URLDataSource(DataSource):
    @classmethod
    def for_url(cls, url, file_format=FileFormat.CSV):
        bundle = URLDataFileBundle.for_url(url, file_format=file_format)

        return URLDataSource(bundle)


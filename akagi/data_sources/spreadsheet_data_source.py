from akagi.data_source import DataSource
from akagi.data_file import DataFile


class SpreadsheetDataSource(DataSource):
    '''SpreadsheetSource replesents a data on Google Spreadsheets
    '''

    def __init__(self, sheet_id, sheet_range='A:Z', no_cache=False):
        self._sheet_id = sheet_id
        self._sheet_range = sheet_range

    @property
    def data_files(self):
        return [DataFile.spreadsheet(self._sheet_id, self._sheet_range)]

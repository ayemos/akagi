import csv
import os
import json

from six import StringIO, BytesIO

from googleapiclient import discovery
from google.oauth2 import service_account

from akagi.content import Content
from akagi.iterator import Iterator


SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']


class SpreadsheetContent(Content):
    is_local = False

    def __init__(self, sheet_id, sheet_range='A:Z'):
        self._sheet_id = sheet_id
        self._range = sheet_range
        self.__credentials = None
        self.file_format = 'csv'
        self.iterator_class = Iterator.get_iterator_class(self.file_format)

    def __iter__(self):
        return self.iterator_class(self)

    @property
    def key(self):
        return 'spredsheet_' + self._sheet_id

    # XXX: Workaround for array-like result from Spreadsheet API
    @property
    def _body(self):
        result = self._discovered.spreadsheets().values()\
            .get(spreadsheetId=self._sheet_id, range=self._range).execute()
        out_io = StringIO()
        writer = csv.writer(out_io, delimiter=',')
        writer.writerows(result.get('values', []))
        out_io.seek(0)

        return BytesIO(out_io.read().encode())

    @property
    def _service_account_info(self):
        credential_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', None)
        if credential_path is not None:
            with open(credential_path, 'r') as f:
                return json.load(f)
        else:
            raise Exception('Service account credentials must be specified by '
                            'GOOGLE_APPLICATION_CREDENTIALS for Spreadsheet support.')

    @property
    def _credentials(self):
        if self.__credentials is None:
            self.__credentials = service_account.Credentials.from_service_account_info(
                self._service_account_info, scopes=SCOPES)

        return self.__credentials

    @property
    def _discovered(self):
        return discovery.build('sheets', 'v4', credentials=self._credentials)

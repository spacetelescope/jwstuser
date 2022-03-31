from collections import namedtuple
from csv import reader as csv_reader
from datetime import datetime
from getpass import getpass
from pathlib import Path
from os import getenv
from requests import get as requests_get

class UnauthorizedError(Exception):
    def __init__(self, message):
        super(UnauthorizedError, self).__init__(message)
        self.message = message


class EngDB:
    '''Access JWST engineering database hosted by MAST at STScI.'''
    def __init__(self, mast_api_token=None):
        self.token = self.get_token(mast_api_token)
        self.baseurl = 'https://mast.stsci.edu/jwst/api/v0.1/' \
            'Download/file?uri=mast:jwstedb'

    def get_token(self, mast_api_token=None, prompt=True):
        '''Get MAST API token. Precedence is arg, env, file, prompt.'''
        if not mast_api_token:
            mast_api_token = getenv('MAST_API_TOKEN')
        if not mast_api_token:
            path = Path.home() / '.mast_api_token'
            try:
                with open(path, 'r') as fp:
                    lines = fp.read().splitlines()
                    if len(lines) == 1:
                        mast_api_token = lines[0]
                    else:
                        print('Ignoring ~/.mast_api_token, expected one line') 
            except FileNotFoundError:
                pass
        if not mast_api_token and prompt:
            mast_api_token = input('Enter MAST API token: ')
        try:
            return self.verified_token(mast_api_token)
        except ValueError as e:
            raise e 

    def verified_token(self, token):
        '''Verify MAST API token type, length, and character set.'''
        is_not = 'MAST API token is not'
        if token:
            if type(token) is str:
                if len(token) == 32:
                    if token.isalnum():
                        return token
                    else:
                        raise ValueError(f"{is_not} alphanumeric: '{token}'")
                else:
                    raise ValueError(f"{is_not} 32 characters: '{token}'")
            else:
                raise ValueError(f"{is_not} type str: {type(token)}")
        else:
            raise ValueError(f"{is_not} defined")

    def format_date(self, date):
        '''Convert datetime object or ISO 8501 string to EDB date format.'''
        if type(date) is str:
            dtobj = datetime.fromisoformat(date)
        elif type(data) is datetime:
            dtobj = date
        else:
            raise ValueError('date must be ISO 8501 string or datetime obj')
        return dtobj.strftime('%Y%m%dT%H%M%S')

    def timeseries(self, mnemonic, start, end):
        '''Get engineering data for specified mnemonic and time interval.'''
        startdate = self.format_date(start)
        enddate = self.format_date(end)
        filename = f'{mnemonic}-{startdate}-{enddate}.csv'
        url = f'{self.baseurl}/{filename}'
        headers = {'Authorization': f'token {self.token}'}
        with requests_get(url, headers=headers, stream=True) as response:
            if response.status_code == 401:
                raise UnauthorizedError('check that MAST API token is valid')
            response.raise_for_status()
            return EdbTimeSeries(mnemonic, response.text.splitlines())

class EdbTimeSeries:
    '''Handle time series data from the JWST engineering database.'''
    def __init__(self, mnemonic, lines):
        self.mnemonic = mnemonic
        self.data = self.parse(lines)

    @property
    def times(self):
        '''Return timeseries times as list of datetime objects.'''
        return [sample[0] for sample in self.data]

    @property
    def values(self):
        '''Return timeseries values as list.'''
        return [sample[1] for sample in self.data]

    @property
    def times_values(self):
        '''Return timeseries times and values as separate lists.'''
        return list(zip(*self.data))

    def parse(self, lines):
        '''Parse lines of text returned by MAST EDB interface.'''
        cast = {'real': float, 'varchar': str}
        data = []
        for field in csv_reader(lines, delimiter=',', quotechar='"'):
            if field[0] == 'theTime':
                continue
            sqltype = field[3]
            utc = datetime.fromisoformat(field[0])
            value = cast[sqltype](field[2])
            data.append([utc, value])
        return data

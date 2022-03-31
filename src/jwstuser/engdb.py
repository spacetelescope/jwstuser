from collections import namedtuple
from csv import reader as csv_reader
from datetime import datetime
from getpass import getpass
from pathlib import Path
from os import getenv
from requests import get as requests_get
from statistics import mode

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
        self.time, self.value = self.parse(lines)
        self._cadence = None
        self._largest_gap = None

    @property
    def timestep_seconds(self):
        '''Return time step between successive times in seconds.'''
        try:
            return [(b - a).total_seconds() for  a, b in zip(
                self.time[:-1], self.time[1:])]
        except IndexError:
            return None

    @property
    def cadence_seconds(self):
        '''Return most common time step in seconds.'''
        timestep_seconds = self.timestep_seconds
        if timestep_seconds:
            self._cadence = mode(timestep_seconds)
            self._largest_gap = max(timestep_seconds)
        return self._cadence

    @property
    def largest_gap_seconds(self):
        '''Return most common time step in seconds.'''
        timestep_seconds = self.timestep_seconds
        if timestep_seconds:
            self._cadence = mode(timestep_seconds)
            self._largest_gap = max(timestep_seconds)
        return self._largest_gap

    def parse(self, lines):
        '''Parse lines of text returned by MAST EDB interface.'''
        cast = {'real': float, 'varchar': str}
        time = []
        value = []
        for field in csv_reader(lines, delimiter=',', quotechar='"'):
            if field[0] == 'theTime':
                continue
            sqltype = field[3]
            time.append(datetime.fromisoformat(field[0]))
            value.append(cast[sqltype](field[2]))
        return time, value

import numpy as np
from collections import namedtuple
from csv import reader as csv_reader
from datetime import datetime
from requests import get as requests_get
from statistics import mode

from .mastapi import get_mast_api_token

class UnauthorizedError(Exception):
    def __init__(self, message):
        super(UnauthorizedError, self).__init__(message)
        self.message = message


class EngineeringDatabase:
    '''Access JWST engineering database hosted by MAST at STScI.'''
    def __init__(self, mast_api_token=None):
        self.token = get_mast_api_token(mast_api_token)
        self.baseurl = 'https://mast.stsci.edu/jwst/api/v0.1/' \
            'Download/file?uri=mast:jwstedb'

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
        self.time, self.time_mjd, self.value = self.parse(lines)
        self._cadence = None
        self._largest_gap = None

    def __len__(self):
        '''Return number of points in time series.'''
        return len(self.time)

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
        # Define python analog of SQL data types.
        # https://docs.microsoft.com/en-us/sql/machine-learning/python
        #     /python-libraries-and-data-types?view=sql-server-ver15
        cast = {'bigint': float, 
                'binary': bytes,
                'bit': bool,
                'char': str,
                'date': datetime,
                'datetime': datetime,
                'float': float, 
                'nchar': str,
                'nvarchar': str,
                'nvarchar(max)': str,
                'real': float,
                'smalldatetime': datetime,
                'smallint': int, 
                'tinyint': int,
                'uniqueidentifier': str,
                'varbinary': bytes,
                'varbinary(max)': bytes,
                'varchar': str, 
                'varchar(n)': str,
                'varchar(max)': str}
        
        # Initialize return variables.
        time = []
        time_mjd = []
        value = []

        for field in csv_reader(lines, delimiter=',', quotechar='"'):

            # Ignore header row.
            if field[0] == 'theTime':
                continue

            # Extract SQL data type in engineering database.
            sqltype = field[3]

            # Convert SQL type to python type. Save time, MJD, and value.
            time.append(datetime.fromisoformat(field[0]))
            time_mjd.append(float(field[1]))
            value.append(cast[sqltype](field[2]))

        return time, time_mjd, value

from datetime import datetime, timezone
from getpass import getpass
from pathlib import Path
from os import getenv

from astropy.time import Time
from astroquery.mast import Mast, Observations

class JwstFilteredQuery:
    '''Manage MAST API query based on JWST FITS header keyword values.

    Example:
        query = JwstFilteredQuery('Nirspec')
        query.filter_by_values('readpatt', 'NRSRAPID, NRSIRS2RAPID')
        query.filter_by_minmax('nints', 2, 99)
        query.filter_by_timerange('date_beg', '2022-04-02 05:00:00', 59671.8)
        query.append_output_columns('pi_name')
        query.execute_query()
        query.browse()
    '''
    def __init__(self, collection):
        self._collection = collection
        self._service = f'Mast.Jwst.Filtered.{collection}'
        self.set_output_columns_to_default()
        self.filters = []
        self._params = None
        self.result = None

    @property
    def collection(self):
        '''Return JWST collection specified during instantiation.'''
        return self._collection

    @property
    def service(self):
        '''Return service access point for specified JWST collection.'''
        return self._service

    @property
    def params(self):
        '''Return parameter dictionary for most recent query.'''
        return self._params

    @property
    def datasets(self):
        '''Return dataset names for most recent query.'''
        try:
            filenames = self.result['filename']
            roots = ['_'.join(f.split('_')[:-1]) for f in filenames]
            return sorted(list(set(roots)))
        except TypeError:
            return None

    def filter_by_values(self, keyword, values):
        '''Require keyword value to be in enumerated list.

        Input values may be str with comma-separated values or list of str.
        Whitespace around commas is ignored
        Examples:
            filter_by_values('detector', 'NRS1')
            filter_by_values('detector', 'NRS1,NRS2')
            filter_by_values('detector', 'NRS1, NRS2')
            filter_by_values('detector', ['NRS1'])
            filter_by_values('detector', ['NRS1', 'NRS2'])
        '''
        try:
            valuelist = [v.strip() for v in values.split(',')]
        except AttributeError:
            valuelist = [str(v) for v in values]
        newfilter = {
            'paramName': str(keyword),
            'values': valuelist}
        self.filters.append(newfilter)

    def filter_by_minmax(self, keyword, minval, maxval):
        '''Require keyword value to be in specified range.

        Example:
            filter_by_values('nints', 2, 99)
        '''
        newfilter = {
            'paramName': str(keyword),
            'values': [{'min': minval, 'max': maxval}]}
        self.filters.append(newfilter)

    def filter_by_timerange(self, keyword, mintime, maxtime):
        '''Require time in keyword value to be in specified range.

        Specified keyword should have values that are absolute times.
        Apply filter to _mjd keyword because query fails for non-mjd keywords.
        Input times may be JD, MJD, astropy Time, datetime, or ISO 8601 string.
        Examples:
            filter_by_timerange('date_obs', '2022-04-02', '2022-04-03')
            filter_by_timerange('date_beg', '2022-04-02T11:00:00', 59671.5)
            filter_by_timerange('date_beg', '2022-04-02 11:00:00', 2459672)
        '''
        if keyword.lower().endswith('_mjd'):
            kw = keyword
        else:
            kw = keyword + '_mjd'
        try:
            minmjd = mjd_from_time(mintime)
            maxmjd = mjd_from_time(maxtime)
        except ValueError as e:
            raise e.with_traceback(e.__traceback__)
        newfilter = {
            'paramName': str(kw),
            'values': [{'min': minmjd, 'max': maxmjd}]}
        self.filters.append(newfilter)

    def set_output_columns(self, column_names):
        '''Set list of output columns to specified value.

        column_names: str or list of str
            Comma-separated column names or list of column names
        '''
        self.columns = []
        self.append_output_columns(column_names)

    def set_output_columns_to_default(self):
        '''Set list of output columns to default value.'''
        inst_configs = {
            'Fgs': 'lamp',
            'GuideStar': 'gdstarid, gs_order',
            'Miri': 'filter, coronmsk, lamp',
            'Nircam': 'module, channel, pupil, filter, coronmsk', 
            'Niriss': 'pupil, filter, lamp', 
            'Nirspec': 'filter, grating, msastate, lamp', 
            }
        self.columns = []
        self.append_output_columns('date_beg, obs_id, category, targname')
        if self.collection != 'GuideStar':
            self.append_output_columns('template, expripar, numdthpt')
        self.append_output_columns('apername')
        try:
            self.append_output_columns(inst_configs[self.collection])
        except KeyError:
            raise ValueError(
                f"unknown collection: {self.collection}\n"
                f"known collections: {' '.join(inst_configs)}")
        self.append_output_columns('exp_type, detector, subarray')
        self.append_output_columns('readpatt, nints, ngroups, duration')
        self.append_output_columns('productLevel, filename')

    def set_output_columns_to_all(self):
        '''Specify that all columns ('*') should be output.'''
        self.columns = '*'

    def append_output_columns(self, column_names):
        '''Append one or more output column names to current list.

        column_names: str or list of str
            Comma-separated column names or list of column names
        '''
        try:
            names = column_names.split(',')
        except AttributeError:
            names = column_names
        for name in names:
            stripped = name.strip()
            if stripped not in self.columns:
                self.columns.append(stripped)

    def remove_output_columns(self, column_names):
        '''Remove one or more output column names from current list.

        column_names: str or list of str
            Comma-separated column names or list of column names
        '''
        try:
            names = column_names.split(',')
        except AttributeError:
            names = column_names
        for name in names:
            stripped = name.strip()
            if stripped in self.columns:
                self.columns.remove(stripped)

    def execute_query(self, convert_dates=True):
        '''Execute query by calling MAST service with specified parameters.'''
        if not self.filters:
            raise ValueError('add search filter(s) before executing query')
        if not self.columns:
            raise ValueError('specify output columns before executing query')
        params = {
            'columns': ','.join(self.columns),
            'filters': self.filters}
        self._params = params
        self.result = Mast.service_request(self.service, params)
        if convert_dates:
            self.convert_dates()

    def convert_dates(self):
        '''Convert table values containing /Date()/ to datetime objects.'''
        if self.result is None:
            raise RuntimeError('execute query before parsing dates')
        if len(self.result) == 0:
            return
        for colname in self.result.colnames:
            values = list(self.result[colname].data.data)
            newval = [
                datetime.utcfromtimestamp(int(v[6:19]) / 1000)
                if isinstance(v, str) and len(v) == 21 and
                    v[:6] == '/Date(' and v[-2:] == ')/' and
                    v[6:19].isdigit()
                else v for v in values]
            if all([isinstance(v, datetime) for v in newval]):
                self.result[colname] = newval
            elif any([n != v for n, v in zip(newval, values)]):
                self.result[colname] = [v.isoformat()[:-3] for v in newval]

    def browse(self):
        '''Show query results in a browser window.'''
        if self.result is None:
            raise RuntimeError('execute query before trying to show result')
        self.result.show_in_browser(jsviewer=True)

class CaomProductList:
    '''Get list of CAOM products for one or more CAOM product group IDs.

    Examples:
        CaomProductList('71738577')
        CaomProductList('71738577, 71738600')
        CaomProductList(['71738577'])
        CaomProductList(['71738577', '71738600'])
        CaomProductList(71738577)
        CaomProductList([71738577])
        CaomProductList([71738577, 71738600])

    References:
        https://mast.stsci.edu/api/v0/pyex.html#MastCaomProductsPy
        https://mast.stsci.edu/api/v0/_services.html#MastCaomProducts
        https://mast.stsci.edu/api/v0/_productsfields.html
    '''
    def __init__(self, caom_obsid):
        self._obsid = self.parse_caom_obsid(caom_obsid)
        self.product_list = self.get_product_list()

    @property
    def obsid(self):
        '''Return CAOM obsid list as a comma-separated string.'''
        return self._obsid

    def parse_caom_obsid(self, caom_obsid):
        '''Parse input specification of one or more CAOM obsid.

        caom_obsid: str or int or iterable yielding those types
            specification of one or more CAOM obsid
        '''
        try:
            obsid = caom_obsid.split(',')
        except AttributeError:
            obsid = caom_obsid
        try:
            return ','.join([str(int(obsid))])
        except TypeError:
            pass
        try:
            return ','.join([str(int(i)) for i in obsid])
        except (TypeError, ValueError):
            raise TypeError('CAOM obsid must evaluate to one or more integers')

    def get_product_list(self):
        '''Get list of CAOM products for specified CAOM obsid.'''
        service = 'Mast.Caom.Products'
        params = {'obsid': self.obsid}
        return Mast.service_request(service, params)

    def browse(self):
        '''Show product list in a browser window.'''
        self.product_list.show_in_browser(jsviewer=True)

def mjd_from_time(time):
    '''Return modified Julian date equivalent to input time specification.

    time: JD/MJD float, astropy Time, tz aware/naive datetime, or ISO 8601 str
          Treat float-compatible input as MJD or JD, de[ending on value.
          Assume astropy Time is UTC.
          Treat timezone-naive datetime object as UTC.
          Treat ISO 8601 string without timezone specification as UTC.
    '''
    # Input is JD or MJD as numeric or str.
    try:
        jd_or_mjd = float(time)
        if jd_or_mjd > 2400000.5:
            mjd = jd_or_mjd - 2400000.5
        else:
            mjd = jd_or_mjd
        return mjd
    except (TypeError, ValueError):
        pass
    # Input is astropy Time object
    if isinstance(time, Time):
        return time.mjd
    # Input is python datetime object.
    if isinstance(time, datetime):
        dt = time
    else:
        try:
            dt = datetime.fromisoformat(time)
        except (TypeError, ValueError):
            raise ValueError(f'unable to parse time specification: {time}')
    # If timezone was not specified, assume UTC.
    naive = dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None
    if naive:
        return Time(dt.replace(tzinfo=timezone.utc)).mjd
    else:
        return Time(dt).mjd

def get_mast_api_token(mast_api_token=None, prompt=False):
    '''Get MAST API token. Precedence is argument, environment, file, prompt.'''
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
        mast_api_token = getpass('Enter MAST API token: ')
    try:
        assert(mast_api_token)
        assert(isinstance(mast_api_token, str))
        assert(len(mast_api_token) == 32)
        assert(mast_api_token.isalnum())
        return mast_api_token
    except AssertionError as e:
        raise ValueError(f"MAST API token is not a string " \
            f"with 32 alphanumeric characters: '{mast_api_token}'")

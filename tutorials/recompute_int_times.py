#!/usr/bin/env python

'''Reproduce values in INT_TIMES extension from values in GROUP extension and
values from the primary header. Input file(s) should be "uncal" files for
exposures with multiple integrations, e.g., time series observations.

The GROUP extension contains reported times when the observatory data handling
unit received the last byte of certain data groups. The INT_TIMES extension
contains calculated start, middle, and end time for every integration,
based on a linear fit to data in the GROUP extension.

Currently, the code has extra logic to handle a bug that caused INT_TIMES
values to be clearly incorrect. The bug has been fixed, but some data still
need to be reprocessed. Once all data have been reprocessed, the extra code
will be removed to reduce confusion.
'''

from datetime import datetime
from pathlib import Path
from sys import argv

from astropy.io.fits import open as fits_open
from astropy.table import Table, vstack
from astropy.time import Time, TimeDelta
from jwstuser.time import calc_rfi, rfi_end_time
from numpy import std as np_std

def read_group_info(pathlist):
    '''Read exposure configuration and available end of group times.

    Read header/footer packet data (integration_number, group_number,
    group_end_time) from the GROUP extension of one or more uncal files
    for a single exposure and detector. Read exposure configuration
    information from primary header.

    Parameters
        pathlist: list of str or Path objects, input files

    Returns
        headfoot: astropy Table object, end of group times
        expinfo: dictionary, exposure configuration from primary header

    Raises
        AssertionError if files have different exposure configurations.
    '''

    # Loop through uncal files for a single exposure.
    headfoot = None
    for path in pathlist:
        with fits_open(path) as hdulist:
            primhead = hdulist[0].header
            try:
                print(f"{Path(path).name}, "
                    f"seg {primhead['EXSEGNUM']}/{primhead['EXSEGTOT']}, "
                    f"ints {primhead['INTSTART']}-{primhead['INTEND']}")
            except KeyError:
                print(f"{Path(path).name}, NINTS={primhead['NINTS']}, "
                    f"unsegmented")
            table = Table(hdulist['GROUP'].data)
            if len(table) < 2:
                exit('This code does not handle only 1 header/footer packet.')

    # Read exposure parameters and GROUP extension in first pass.
            if not headfoot:
                nints = primhead['NINTS']
                nframes = primhead['NFRAMES']
                groupgap = primhead['GROUPGAP']
                ngroups = primhead['NGROUPS']
                nresets = primhead['NRESETS']
                tframe = primhead['TFRAME']
                bartdelt = primhead['BARTDELT']
                headfoot = table

    # Check exposure parameters. Append GROUP extension in subsequent passes.
            else:
                assert nints == primhead['NINTS']
                assert nframes == primhead['NFRAMES']
                assert groupgap == primhead['GROUPGAP']
                assert ngroups == primhead['NGROUPS']
                assert nresets == primhead['NRESETS']
                assert tframe == primhead['TFRAME']
                assert bartdelt == primhead['BARTDELT']
                headfoot = vstack([headfoot, table])

    # Keep only relevant columns from GROUP extension.
    headfoot = headfoot['integration_number', 'group_number', 'group_end_time']
    expinfo = {'NINTS': nints, 'NFRAMES':nframes, 'GROUPGAP':groupgap,
        'NGROUPS': ngroups, 'NRESETS': nresets, 'TFRAME': tframe,
        'BARTDELT': bartdelt}
    return headfoot, expinfo

def fit_group_info(headfoot, expinfo):
    '''Find a linear model (cadence, dt_ref, rfi_ref) that yields a relative
    time scale (in seconds), which matches time stamps reported in image
    header/footer packets.

    Parameters
        headfoot: astropy Table object, end of group times
        expinfo: dictionary, exposure configuration from primary header

    Returns
        model: dictionary, parameters of fit to end of group times
    '''

    # Calculate running frame index for each image header/footer packet.
    rfi = [calc_rfi(row['integration_number'], row['group_number'],
        expinfo['NGROUPS'], expinfo['NFRAMES'], expinfo['GROUPGAP'],
        expinfo['NRESETS']) for row in headfoot]
    headfoot.add_column(rfi, name='rfi', index=2)

    # Calculate cumulative time for each image header/footer packet.
    dt_ref = datetime.fromisoformat(headfoot[0]['group_end_time'])
    delta_time = [
        (datetime.fromisoformat(hf['group_end_time']) - dt_ref).total_seconds()
        for hf in headfoot]
    headfoot.add_column(delta_time, name='delta_time')

    # Determine apparent frame cadence. Compare with frame time in header.
    cadence = (headfoot['delta_time'][-1] - headfoot['delta_time'][0]) \
        / (headfoot['rfi'][-1] - headfoot['rfi'][0])
    print(f"measured frame cadence={cadence:.8} s, "
        f"nominal TFRAME={expinfo['TFRAME']} s")

    # Calculate model time based on measured first time and frame cadence.
    dt_ref = headfoot['delta_time'][0]
    rfi_ref = headfoot['rfi'][0]
    model_time = [rfi_end_time(rfi, rfi_ref, dt_ref, cadence)
        for rfi in headfoot['rfi']]

    # Calculate residual of measured cumulative time minus model time.
    resid = [cumul - model for cumul, model in zip(
        headfoot['delta_time'], model_time)]

    # Shift model times to yield zero residual on average.
    dt_ref += sum(resid) / len(resid)
    model_time = [rfi_end_time(rfi, rfi_ref, dt_ref, cadence)
        for rfi in headfoot['rfi']]
    resid = [cumul - model for cumul, model in zip(
        headfoot['delta_time'], model_time)]

    # Save updated model time and updated residuals in output table.
    headfoot.add_column(model_time, name='model_time')
    headfoot.add_column(resid, name='resid')

    # Print histogram of residuals for each group number in all integrations.
    for g in list(set([hf['group_number'] for hf in headfoot])):
        resid = [hf['resid'] for hf in headfoot if hf['group_number'] == g]
        avg = sum(resid) / len(resid)
        print(f'g={g:2}, min={min(resid):+.04f}, avg={avg:+.04f}, '
            f'uavg={np_std(resid)/len(resid):+.04f}, '
            f'std={np_std(resid):+.04f}, max={max(resid):+.04f}')

    # write image header/footer data and model to CSV file.
    csvfile = 'group_exten.csv'
    print(f'writing {csvfile}')
    headfoot.write(csvfile, overwrite=True)

    model = {'cadence': cadence, 'rfi_ref': rfi_ref, 'dt_ref': dt_ref}
    return model

def recalc_int_times(model, expinfo):
    '''This section of code uses the linear model found above to estimate the
    UTC start, middle, and end time of each integration in the exposure. This
    information should match what goes in the INT_TIMES extension.

    Parameters
        model: dictionary, parameters of fit to end of group times
        expinfo: dictionary, exposure configuration from primary header

    Returns
        int_times: astropy Table, calculated time for every integration
    '''

    # Extract model parameters from model.
    cadence = model['cadence']
    rfi_ref = model['rfi_ref']
    dt_ref = model['dt_ref']

    # Extract exposure parameters from expinfo.
    nints = expinfo['NINTS']
    ngroups = expinfo['NGROUPS']
    nframes = expinfo['NFRAMES']
    groupgap = expinfo['GROUPGAP']
    nresets = expinfo['NRESETS']

    # Generate integration numbers for entire exposure.
    integration_number = [i+1 for i in range(nints)]

    # Calculate running frame number.
    rfi_first_group = [calc_rfi(i, 1, ngroups, nframes, groupgap, nresets)
        for i in integration_number]
    rfi_last_group = [calc_rfi(i, ngroups, ngroups, nframes, groupgap, nresets)
        for i in integration_number]

    # Calculate UTC datetime at start and end of each integration.
    utc_ref = Time(headfoot[0]['group_end_time'], format='isot', scale='utc')
    int_start_utc = [utc_ref + TimeDelta(rfi_end_time(
            rfi, rfi_ref, dt_ref, cadence) - nframes * cadence, format='sec')
        for rfi in rfi_first_group]
    int_end_utc = [utc_ref + TimeDelta(rfi_end_time(
            rfi, rfi_ref, dt_ref, cadence), format='sec')
        for rfi in rfi_last_group]

    # Calculate MJD equivalent of UTC times.
    int_start_mjd_utc = [t.mjd for t in int_start_utc]
    int_end_mjd_utc = [t.mjd for t in int_end_utc]
    int_mid_mjd_utc = [
        (s + e) / 2 for s, e in zip(int_start_mjd_utc, int_end_mjd_utc)]

    # Calculate BJD equivalent of UTC times by adding barycentric time delta.
    bartdelt_day = expinfo['BARTDELT'] / 86400
    int_start_bjd_utc = [mjd + bartdelt_day for mjd in int_start_mjd_utc]
    int_mid_bjd_utc = [mjd + bartdelt_day for mjd in int_mid_mjd_utc]
    int_end_bjd_utc = [mjd + bartdelt_day for mjd in int_end_mjd_utc]

    # Contruct table corresponding to INT_TIMES extension with two extra
    # columns for debugging.
    int_times = Table([integration_number,
            int_start_mjd_utc, int_mid_mjd_utc, int_end_mjd_utc,
            int_start_bjd_utc, int_mid_bjd_utc, int_end_bjd_utc,
            int_start_utc, int_end_utc],
        names=['integration_number',
            'int_start_MJD_UTC', 'int_mid_MJD_UTC', 'int_end_MJD_UTC',
            'int_start_BJD_UTC', 'int_mid_BJD_UTC', 'int_end_BJD_UTC',
            'int_start_utc', 'int_end_utc'])

    # Write integration time intervals to CSV file.
    csvfile = 'int_times_exten.csv'
    print(f'writing {csvfile}')
    int_times.write(csvfile, overwrite=True)

    return int_times

if __name__ == '__main__':
    if len(argv) < 2:
        exit(f'syntax: {argv[0]} file1 [file2...]\n'
            f'  e.g.: {argv[0]} jw01118005001_04*seg*nrs1_uncal.fits')
    headfoot, expinfo = read_group_info(argv[1:])
    model = fit_group_info(headfoot, expinfo)
    int_times = recalc_int_times(model, expinfo)

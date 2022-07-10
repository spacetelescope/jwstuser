#!/usr/bin/env python

def calc_rfi(i, g, ngroups, nframes, groupgap, nresets):
    '''Calculate running frame index for last frame in an integration and group.

    Running frame index (rfi) begins at 1 for the first frame in the first
    group in the first integration of an exposure. That first frame may also
    be frame0 for NIRCam. Running frame index increments for every frame that
    contributes to a group, every dropped frame in a groupgap, and every
    full-frame reset between integrations. Running frame index increases
    monitonically throughout an exposure. Running frame index is a useful
    quantity because to first order, elapsed time in an exposure is linearly
    proportional to running frame index.

    Arguments:
        i: integration index starting with 1
        g: group index starting with 1, value of 0 indicates frame0
        nresets: number of full-frame resets between integrations (nreset2)

    Returns:
        Running frame index for specified integration and group
    '''
    frames_per_int = ngroups * nframes + (ngroups - 1) * groupgap + nresets
    if g == 0:
        return (i - 1) * frames_per_int + 1
    else:
        return (i - 1) * frames_per_int + g * nframes + (g - 1) * groupgap

def rfi_end_time(rfi, rfi_ref, dt_ref, cadence):
    '''Calculate time at end of specified running frame, relative to reference.

    Arguments:
        rfi: running frame index (see calc_rfi)
        rfi_ref: rfi adopted as a reference for linear model
        dt_ref: time adopted as a reference for linear model
        cadence: time (in s) between start of consecutive frames

    Returns:
        Time (in s) at end of specified running frame, relative to reference.
    '''
    return dt_ref + cadence * (rfi - rfi_ref)

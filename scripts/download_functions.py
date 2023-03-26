#########################################
###  container for various functions  ###
#########################################

import time
import os
import sys
import fnmatch
import datetime
import bz2

from requests import Session
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from cdo import Cdo

########################################################################
########################################################################
########################################################################

def download_global_ml_vars(task):
    [parallel, task_idx, path, date, fcst_hour, mlevels, targetgrid_name] = task

    delay = 1   # in s
    if parallel and delay > 0 and task_idx < 2:
        time.sleep(task_idx * delay)    # shift the first tasks a little


    # download, merge and interpolate ml data #

    var_list = ['p','t','qv','u','v','w']

    for var in var_list:
        temp_subdir = path['data'] + var
        if not os.path.isdir(path['base'] + temp_subdir):
            os.mkdir(path['base'] + temp_subdir)
        path['subdir'] = temp_subdir + '/'

        timer_start = time.time()
        for mlevel in mlevels:
            grib_filename = 'icon_global_icosahedral_model-level_{}{:02}{:02}{:02}_{:03}_{:2}_{}.grib2.bz2'.format(
                             date['year'], date['month'], date['day'], date['hour'], fcst_hour, mlevel, var.upper())

            url = 'https://opendata.dwd.de/weather/nwp/icon/grib/{:02}/{}/'.format(
                   date['hour'], var)

            if download(url, grib_filename, path):
                grib_filename = unzip(path, grib_filename)

        print('{:03d}h: download time {} ml{:02d}-{:02d}: {:.3f}s'.format(
               fcst_hour, var, mlevels[0], mlevels[-1], time.time() - timer_start))

        grib_matchname = 'icon_global_icosahedral_model-level_{}{:02}{:02}{:02}_{:03}_*_{}.grib2'.format(
                          date['year'], date['month'], date['day'], date['hour'], fcst_hour, var.upper())
        merged_filename = 'icon-global-det_icosahedral_model-level-{:02}-{:02}_{}{:02}{:02}{:02}_{:03}h_{}.grib2'.format(
                           mlevels[0], mlevels[-1],
                           date['year'], date['month'], date['day'], date['hour'],
                           fcst_hour, var)
        latlon_filename = 'icon-global-det_{}_model-level-{:02}-{:02}_{}{:02}{:02}{:02}_{:03}h_{}.nc'.format(
                           targetgrid_name, mlevels[0], mlevels[-1],
                           date['year'], date['month'], date['day'], date['hour'],
                           fcst_hour, var)

        timer_start = time.time()
        merge_mlevel_files_to_one_grib(path, grib_matchname, merged_filename)
        print('{:03d}h: merge time {} ml{:02d}-{:02d}: {:.3f}s'.format(
               fcst_hour, var, mlevels[0], mlevels[-1], time.time() - timer_start))

        timer_start = time.time()
        interpolate_icosahedral_to_latlon(path, merged_filename, latlon_filename, targetgrid_name,
                                          model='icon-global-det', output_as_netcdf=True)
        print('{:03d}h: interpolate time {} ml{:02d}-{:02d}: {:.3f}s'.format(
               fcst_hour, var, mlevels[0], mlevels[-1], time.time() - timer_start))

        os.remove(path['base'] + path['subdir'] + merged_filename)

    return True

########################################################################
########################################################################
########################################################################

def download(url, filename, path):
    session = Session()
    session.mount('https://', HTTPAdapter(max_retries=Retry(total=10, backoff_factor=0.5)))
    r = session.get(url + filename, timeout=120)
    with open(path['base'] + path['subdir'] + filename, 'wb') as file:
        file.write(r.content)
    r.close()
    return 1

########################################################################
########################################################################
########################################################################

def unzip(path, filename):
    newfilename = filename[:-4]     # cut file ending
    with open(path['base'] + path['subdir'] + newfilename, 'wb') as unzippedfile:
        with open(path['base'] + path['subdir'] + filename, 'rb') as zippedfile:
            decompressor = bz2.BZ2Decompressor()
            for datapart in iter(lambda : zippedfile.read(100 * 1024), b''):
                try:
                    unzippedfile.write(decompressor.decompress(datapart))
                except OSError:
                    try:
                        unzippedfile.write(decompressor.decompress(datapart))
                    except OSError:
                        try:
                            unzippedfile.write(decompressor.decompress(datapart))
                        except OSError:
                            try:
                                unzippedfile.write(decompressor.decompress(datapart))
                            except OSError:
                                unzippedfile.write(decompressor.decompress(datapart))

    os.remove(path['base'] + path['subdir'] + filename)
    return newfilename

########################################################################
########################################################################
########################################################################

# this function merges all mlevel gribfiles to one gribfile with cdo #

def merge_mlevel_files_to_one_grib(path, grib_matchname, merged_filename):

    cdo_module = Cdo()
    cdo_module.merge(input = path['base'] + path['subdir'] + grib_matchname,
                     output = path['base'] + path['subdir'] + merged_filename)

    for gribfile in fnmatch.filter(os.listdir(path['base'] + path['subdir']), grib_matchname):
        os.remove(path['base'] + path['subdir'] + gribfile)

    return

########################################################################
########################################################################
########################################################################

def interpolate_icosahedral_to_latlon(path, merged_filename, latlon_filename, targetgrid_name,
                                      model, output_as_netcdf):

    # interpolates icosahedral to regular latlon grid (both global) #

    if model == 'icon-global-det':
        targetgridfile = 'target_grid_global_{}.txt'.format(targetgrid_name)
        weightsfile = 'weights_con1_{}_icosahedral_to_{}.nc'.format(model, targetgrid_name)
        gridfile = 'icon_grid_0026_R03B07_G.nc'

    if output_as_netcdf:
        options_out = '-f nc'
    else:
        options_out = ''

    cdo_module = Cdo()
    if weightsfile not in os.listdir(path['base'] + path['grid']):
        print('create weightsfile...')
        cdo_module.gencon(path['base'] + path['grid'] + targetgridfile,
                          input = path['base'] + path['grid'] + gridfile,
                          output = path['base'] + path['grid'] + weightsfile)


    cdo_module.remap(path['base'] + path['grid'] + targetgridfile + ',' + path['base'] + path['grid'] + weightsfile,
                     input = path['base'] + path['subdir'] + merged_filename,
                     output = path['base'] + path['subdir'] + latlon_filename,
                     options=options_out)

    return

########################################################################
########################################################################
########################################################################

def calc_latest_run_time(model):

    # keep always on track with dwd / ecmwf-open-data update times #

    if model == 'icon-eu-eps':
        update_times_utc = [4+30/60, 9+49/60, 16+30/60, 21+49/60]
    elif model == 'icon-global-eps':
        update_times_utc = [4+24/60, 16+24/60]
    elif model == 'icon-eu-det':
        update_times_utc = [3+47/60, 9+41/60, 15+47/60, 21+41/60]
    elif model == 'icon-global-det':
        update_times_utc = [4+20/60, 15+29/60]
    elif model == 'ecmwf-hres':
        update_times_utc = [7+55/60, 12+45/60, 19+55/60, 24+45/60]


    datenow  = datetime.datetime.now().date()
    timenow  = datetime.datetime.now(datetime.timezone.utc).time()
    run_year  = datenow.year
    run_month = datenow.month
    run_day   = datenow.day
    run_time_float = timenow.hour + timenow.minute / 60

    if run_time_float < update_times_utc[0]:
        run_year, run_month, run_day = go_back_one_day(run_year, run_month, run_day)

    if len(update_times_utc) == 2:
        if   run_time_float >= update_times_utc[0] and run_time_float < update_times_utc[1]: run_hour = 0
        elif run_time_float >= update_times_utc[1]  or run_time_float < update_times_utc[0]: run_hour = 12
        else: exit()
    elif len(update_times_utc) == 4:
        if update_times_utc[3] > 24 and run_time_float < update_times_utc[3] - 24:
            run_time_float += 24
        if   run_time_float >= update_times_utc[0] and run_time_float < update_times_utc[1]: run_hour = 0
        elif run_time_float >= update_times_utc[1] and run_time_float < update_times_utc[2]: run_hour = 6
        elif run_time_float >= update_times_utc[2] and run_time_float < update_times_utc[3]: run_hour = 12
        elif run_time_float >= update_times_utc[3]: run_hour = 18
        elif run_time_float < update_times_utc[0]: run_hour = 18
        else: exit()

    date = dict(year = run_year, month = run_month, day = run_day, hour = run_hour)
    return date

########################################################################
########################################################################
########################################################################

def go_back_one_day(run_year, run_month, run_day):
    if run_day >= 2: run_day -= 1
    else:
        # run_day is 1
        if run_year % 4 == 0:
            # schaltjahre/leap years being considered
            days_of_month = [31,29,31,30,31,30,31,31,30,31,30,31]
        else:
            days_of_month = [31,28,31,30,31,30,31,31,30,31,30,31]
        if run_month >= 2:
            run_month -= 1
            run_day = days_of_month[run_month-1]
        else:
            # run_month is 1
            run_year -= 1
            run_month = 12
            run_day = days_of_month[run_month-1]

    return run_year, run_month, run_day
    
########################################################################
########################################################################
########################################################################

def get_timeshift():
    datenow  = datetime.datetime.now()
    dayofyear_now = (datenow-datetime.datetime(datenow.year,1,1)).days + 1

    #                     2019    2020    2021    2022    2023
    changedate_spring = [[3, 31],[3, 29],[3, 28],[3, 27],[3, 26]]
    changedate_autumn = [[10,27],[10,25],[10,31],[10,30],[10,29]]
    year_index = datenow.year - 2019
    date_spring = datetime.datetime(datenow.year, changedate_spring[year_index][0],
                                    changedate_spring[year_index][1], 0, 0)
    dayofyear_spring = (date_spring - datetime.datetime(datenow.year,1,1)).days + 1
    date_autumn = datetime.datetime(datenow.year, changedate_autumn[year_index][0],
                                    changedate_autumn[year_index][1], 0, 0)
    dayofyear_autumn = (date_autumn - datetime.datetime(datenow.year,1,1)).days + 1

    if dayofyear_now >= dayofyear_spring and dayofyear_now < dayofyear_autumn:
        timeshift = 2
    else:
        timeshift = 1

    return timeshift

########################################################################
########################################################################
########################################################################


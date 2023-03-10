
import time
import os
import sys

from download_forecast import download, unzip
from download_forecast import merge_mlevel_files_to_one_grib
from download_forecast import interpolate_icosahedral_to_latlon


def download_global_ml_vars(task):
    [parallel, task_idx, path, date, fcst_hour, mlevels, latlon_resolution] = task


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
        #latlon_filename = 'icon-global-det_latlon_{:.3f}_model-level-{:02}-{:02}_{}{:02}{:02}{:02}_{:03}h_{}.nc'.format(
        #                   latlon_resolution, mlevels[0], mlevels[-1],
        #                   date['year'], date['month'], date['day'], date['hour'],
        #                   fcst_hour, var)

        timer_start = time.time()
        merge_mlevel_files_to_one_grib(path, grib_matchname, merged_filename)
        print('{:03d}h: merge time {} ml{:02d}-{:02d}: {:.3f}s'.format(
               fcst_hour, var, mlevels[0], mlevels[-1], time.time() - timer_start))

        '''match var:
            case 'p' | 't' | 'u' | 'v':
                do_interpolate = True
            case 'qv' | 'w':
                do_interpolate = False

        if do_interpolate:
            timer_start = time.time()
            interpolate_icosahedral_to_latlon(path, merged_filename, latlon_filename, latlon_resolution,
                                              model='icon-global-det', output_as_netcdf=True)
            print('{:03d}h: interpolate time {} ml{:02d}-{:02d}: {:.3f}s'.format(
                   fcst_hour, var, mlevels[0], mlevels[-1], time.time() - timer_start))'''


    return True

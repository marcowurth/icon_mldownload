
import time
import os
import sys
import datetime

import distributed

from download_functions import calc_latest_run_time, download_global_ml_vars


def main():

    # settings #

    fcst_hours_list = list(range(0, 24+1, 3))
    #fcst_hours_list = [12]

    mlevels = list(range(58, 120+1, 1))
    #mlevels = [120]

    targetgrid_name = 'ssa_0.04deg'

    #parallel = True     # parallization works over fcst_hours, not vars
    parallel = False


    # get latest run time #

    run = calc_latest_run_time('icon-global-det')
    #run = dict(year = 2023, month = 2, day = 16, hour = 0)
    #run['hour'] = 0

    print('download and calculate from run_{}{:02}{:02}{:02}'.format(
           run['year'], run['month'], run['day'], run['hour']))


    # create paths #

    path = dict(base = os.getcwd()[:-8] + '/',
                grid = 'data/grid/',
                data = 'data/forecasts')
    if not os.path.isdir(path['base'] + path['data']):
        os.mkdir(path['base'] + path['data'])
    path['data'] += '/run_{}{:02}{:02}{:02}'.format(run['year'], run['month'], run['day'], run['hour'])
    if not os.path.isdir(path['base'] + path['data']):
        os.mkdir(path['base'] + path['data'])
    path['data'] = path['data'] + '/'

    for varfolder in ['p','t','qv','u','v','w']:
        if not os.path.isdir(path['base'] + path['data'] + varfolder):
            os.mkdir(path['base'] + path['data'] + varfolder)
 


    # create tasks #

    tasks = []
    task_idx = 0
    for fcst_hour in fcst_hours_list:
        tasks.append([parallel, task_idx, path, run, fcst_hour, mlevels, targetgrid_name])
        task_idx += 1


    if parallel:
        # option: parallelized #

        num_workers = 1
        num_threads = 4
        client = distributed.Client(name='icon_mlevel',
                                    n_workers=num_workers, threads_per_worker=num_threads,
                                    dashboard_address='127.0.0.1:5000')

        futures = client.map(download_global_ml_vars, tasks, pure=True)
        futures = client.gather(futures)
        client.close()

        print('finished')
        if all(futures):
            print('all tasks successful')
        else:
            print('the following tasks were not successful:')
            for i, future in enumerate(futures):
                if not future:
                    print(tasks[i][:2])

    else:
        # option: unparallelized/serial #

        for task in tasks:
            future = download_global_ml_vars(task)


    # remove last run folder

    #run_datetime = datetime.datetime(run['year'], run['month'], run['day'], run['hour'])
    #last_run_datetime = run_datetime - datetime.timedelta(seconds=60*60*12)
    #pathtoremove = '{}{}run_{}{:02}{:02}{:02}'.format(
    #                path['base'], path['data'][:-14],
    #                last_run_datetime.year, last_run_datetime.month, last_run_datetime.day, last_run_datetime.hour)
    #os.system('rm -rf ' + pathtoremove)

    return

########################################################################
########################################################################
########################################################################

if __name__ == '__main__':
    t1 = time.time()
    main()
    t2 = time.time()
    delta_t = t2-t1
    if delta_t < 60:
        print('total script time:  {:.1f}s'.format(delta_t))
    elif 60 <= delta_t <= 3600:
        print('total script time:  {:.0f}min{:.0f}s'.format(delta_t//60, delta_t-delta_t//60*60))
    else:
        print('total script time:  {:.0f}h{:.1f}min'.format(delta_t//3600, (delta_t-delta_t//3600*3600)/60))

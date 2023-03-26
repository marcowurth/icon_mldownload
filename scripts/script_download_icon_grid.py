
import time
import os

import requests


def main():

    path = dict(base = os.getcwd()[:-8] + '/',
                grid = 'data/grid/')
    filename = 'icon_grid_0026_R03B07_G.nc'     # grid of operational icon-global-det (13km)
    url = 'http://icon-downloads.mpimet.mpg.de/grids/public/edzw/'

    print('downloading {} to {}'.format(filename, path['base'] + path['grid']))
    r = requests.get(url + filename, timeout=10)
    with open(path['base'] + path['grid'] + filename, 'wb') as file:
        file.write(r.content)
    r.close()

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

# icon_mldownload
Script for downloading Model Level Data of the ICON-Global-Det Model and interpolating it to latlon grids

Use:
Run download_icon_grid.py to download the icon gridfile.
Then set settings in script_download_icon_forecast_ml.py and run it.


Folder Tree: icon_mldownload
                |       |
             scripts   data
                       |  |
                    grid  forecasts


Needed pkgs: distributed, requests, cdo, xarray, netcdf4, (for dask browser viz: bokeh=2)

# About
# =====
# Required packages: netcdf4, dask[complete], xarray, xclim
#
# Data requirements: All *.NC files to be included in calculations
#     must have the same dimensions: time, lat, lon, and data types.
#     By default, the region to be calculated apparently should not
#     contain missing values though there is discussion about how to
#     overcome this towards the bottom of this page:
#         https://xclim.readthedocs.io/en/stable/notebooks/usage.html
#
# Usage:
# 1. Put all *.NC files to be included in calculations in the same folder
#        with no other *.NC files.
# 2. Run this script. It will:
#        a. Prompt to select the folder containing the *.NC files.
#        b. Perform the calculations.
#        c. Prompt for file to save the results to.
#        Note: the script may spit out a number of runtime
#            warnings- I am not sure of their exact effect yet.
#
# Other:
#     There are performance-improving features for big data such as
#     chunking or using dask. I am not familiar with this area yet.
#
import xarray as xr
import utils
import xclim as xc
import warnings


def do_generate_indices():
    # Select folder containing all *.NC files to be included in calculations
    fldr_in = utils.get_folder_path('Select input folder')
    if not fldr_in:
        raise Exception('Folder selection aborted')
    fldr_in += r'*.nc'

    # Select file to save as- can't overwrite input file!
    # Don't save to input folder!
    file_out = utils.get_save_path('Select output file')
    if not file_out:
        raise Exception('Save file selection aborted')


    # Set xarray to keep attributes for DataArrays and Datasets
    # Causes problems with unit conversions, don't use
    #xr.set_options(keep_attrs=True)


    # Open and concatencate all *.NC files in folder into a Dataset to be
    # used in calculations
    ds = xr.open_mfdataset(fldr_in, engine='netcdf4')

    ds = ds.resample(time='D').mean(keep_attrs=True)

    # Ouput info about Dataset so you can see the available variables
    #print(ds)

    # You could calculate based on a selection of data based on lat and/or
    # lon and/or time as long as the ranges are in the *.NC files. If you
    # do this, you need to replace references to `ds` with `ds_sel` in
    # the calculations.
    #ds_sel = ds.sel(lat=slice(45, 50), lon=slice(150, 175)) #, time=slice('2090', '2100'))

    # Define an output Dataset with attributes from input Dataset
    ds_out = xr.Dataset(attrs=ds.attrs)

    # Calculations of `Indicators`, which are xclim wrappers around indice functions
    # providing additional functionality over the underlying indice function.
    # List of available Indicators:
    # https://xclim.readthedocs.io/en/stable/indicators.html


    # The following calculations assume the `pr`, `tasmax`, and `tasmin` variables exist
    
    # Precipitation indicators based on `pr` variable
    # R1mm; Number of days per year when precipitation ≥ 1 mm
    da = xc.atmos.wetdays(ds.pr, thresh='1 mm/day', freq='YS')
    ds_out[da.name] = da
    # CDD; Maximum number of consecutive days with daily precipitation < 1 mm
    da = xc.atmos.maximum_consecutive_dry_days(ds.pr, thresh='1 mm/day', freq='YS')
    ds_out[da.name] = da
    # CWD; Maximum number of consecutive days with daily precipitation ≥ 1 mm
    da = xc.atmos.maximum_consecutive_wet_days(ds.pr, thresh='1 mm/day', freq='YS')
    ds_out[da.name] = da
    # PRCPTOT; Annual total precipitation in wet days (daily precipitation ≥ 1 mm)
    da = xc.atmos.precip_accumulation(ds.pr, freq='YS')
    ds_out[da.name] = da
    # SDII; Annual total precipitation divided by the number of wet days
    da = xc.atmos.daily_pr_intensity(ds.pr, thresh='1 mm/day', freq='YS')
    ds_out[da.name] = da
    # RX1day; Annual maximum 1-day precipitation
    da = xc.atmos.max_1day_precipitation_amount(ds.pr, freq='YS')
    ds_out[da.name] = da
    # RX5day; Annual maximum 5-day precipitation
    da = xc.atmos.max_n_day_precipitation_amount(ds.pr, window=5, freq='YS')
    ds_out[da.name] = da
    # TXx Annual maximum daily maximum temperature
    da = xc.atmos.tx_max(ds.tasmax, freq='YS')
    ds_out[da.name] = da
    # TNx Annual maximum daily minimum temperature
    da = xc.atmos.tn_max(ds.tasmin, freq='YS')
    ds_out[da.name] = da
    # TXn Annual minimum daily maximum temperature
    da = xc.atmos.tx_min(ds.tasmax, freq='YS')
    ds_out[da.name] = da
    # TNn Annual minimum daily minimum temperature
    da = xc.atmos.tn_min(ds.tasmin, freq='YS')
    ds_out[da.name] = da
    #FD Number of days per year when daily minimum temperature < 0°C
    da = xc.atmos.frost_days(ds.tasmin, freq='YS')
    ds_out[da.name] = da
    #ID Number of days per year when daily maximum temperature < 0°C
    da = xc.atmos.ice_days(ds.tasmax, freq='YS')
    ds_out[da.name] = da
    # SU Number of days per year when daily maximum temperature > 25°C
    da = xc.atmos.tx_days_above(ds.tasmax, thresh='25 degC', freq='YS')
    ds_out[da.name] = da
    # TR Number of days per year when daily minimum temperature > 20°C
    da = xc.atmos.tropical_nights(ds.tasmin, thresh='20 degC', freq='YS')
    ds_out[da.name] = da


    # Local DataArrays (not part of Dataset) to be used below
    tas = xc.indices.tas(ds.tasmin, ds.tasmax)
    t10 = xc.core.calendar.percentile_doy(tas, per=10)
    t90 = xc.core.calendar.percentile_doy(tas, per=90)
    tn10 = xc.core.calendar.percentile_doy(ds.tasmin, per=10)

 
    # TX10p Percentage of days with daily maximum temperature < 10th percentile of the base period
    da = xc.atmos.tx10p(ds.tasmax, t10, freq='YS')
    ds_out[da.name] = da

    # TX90p Percentage of days with daily maximum temperature > 90th percentile of the base period
    da = xc.atmos.tx90p(ds.tasmax, t90, freq='YS')
    ds_out[da.name] = da

    # TN10p Percentage of nights with daily minimum temperature < 10th percentile of the base period
    da = xc.atmos.tn10p(ds.tasmin, t10, freq='YS')
    ds_out[da.name] = da

    # TN90p Percentage of nights with daily minimum temperature > 90th percentile of the base period
    da = xc.atmos.tn90p(ds.tasmin, t90, freq='YS')
    ds_out[da.name] = da

    # WSDI Number of days per year with at least 6 consecutive days when daily maximum temperature > 90th percentile of the base period

    # CSDI Number of days per year with at least 6 consecutive days when daily minimum temperature < 10th percentile of the base period
    da = xc.atmos.cold_spell_duration_index(ds.tasmin, tn10, window= 1, freq='YS')
    ds_out[da.name] = da

    # Done with calculations

    # Get default encodings for use with Dataset::to_netcdf() method
    encodings = utils.get_to_netcdf_encodings(ds=ds_out, comp_level=4)

    # Save Dataset to file with encodings
    ds_out.to_netcdf(path=file_out, engine='netcdf4', encoding=encodings)


if __name__ == '__main__':
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore')
        do_generate_indices()
        print('Done!!!')

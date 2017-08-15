# ecmwf_api_howto

## Examples of use

Specifiying a single year:

    $ luigi --module ecmwf_temperature_download DownloadEcwmfData --year 2005 --outdir /some/output/directory --workers 12

Specifiying a range of years (all dates in the range inclusive of start and end are retrieved):

    luigi --module ecmwf_temperature_download DownloadEcwmfData --year 2005-01-01-2009-12-31 --outdir /some/output/directory --workers 12

## Converting .grib files

The ``ecmwfapi`` generally delivers ``grib`` files. To convert ``grib`` files to ``geotiff`` 
files, use the ``to_geotiff.py`` script.

```
usage: to_geotiff.py [-h] [-d OUT_DIR] [-v] [in_paths [in_paths ...]]

Convert file(s) to GeoTiff

positional arguments:
  in_paths              paths to file to be converted

optional arguments:
  -h, --help            show this help message and exit
  -d OUT_DIR, --out_dir OUT_DIR
                        path to output directory (Default to input directory)
  -v, --verbose         print output filenames
```

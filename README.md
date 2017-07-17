# ecmwf_api_howto

## Examples of use

Specifiying a single year:

    $ luigi --module ecmwf_temperature_download DownloadEcwmfData --year 2005 --outdir /some/output/directory --workers 12

Specifiying a range of years (all dates in the range inclusive of start and end are retrieved):

    luigi --module ecmwf_temperature_download DownloadEcwmfData --year 2005-01-01-2009-12-31 --outdir /some/output/directory --workers 12

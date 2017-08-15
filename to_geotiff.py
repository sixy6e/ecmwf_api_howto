#!/usr/bin/env python
"""
Convert file(s) to GeoTiff format
"""

from os.path import basename, splitext, join, exists, abspath, dirname
import sys
import rasterio
from argparse import ArgumentParser

def convert(in_path, out_path):
    with rasterio.open(in_path) as ds:
        kwargs = {'count': ds.count,
                  'height': ds.height,
                  'width': ds.width,
                  'driver': 'GTiff',
                  'crs': ds.crs,
                  'transform': ds.transform,
                  'dtype': 'float64',
                  'nodata': ds.nodata,
                  'compress': 'deflate',
                  'zlevel': 6,
                  'predictor': 3}

        with rasterio.open(out_path, 'w', **kwargs) as outds:
            for i in range(1, ds.count +1):
                outds.write(ds.read(i), i)
                outds.update_tags(i, **ds.tags(i))

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def die(*args, **kwargs):
    eprint(*args, **kwargs)
    sys.exit(1)


def main():
    parser = ArgumentParser(description="Convert file(s) to GeoTiff")
    parser.add_argument('in_paths', nargs='*', help='paths to file to be converted')
    parser.add_argument('-d', '--out_dir', help='path to output directory (Default to input directory)', required=False)
    parser.add_argument('-v', '--verbose', help='print output filenames', action='store_true')
    parser.set_defaults(verbose=False)
    args = parser.parse_args()

    # check the output directory 
    od = args.out_dir
    if od is not None:
        od = abspath(od)
        if not exists(od):
            die(od + " doesn't exist")

    # iterate over the input files  
    for in_path in args.in_paths:
        p = abspath(in_path)
        if exists(p):
            if od is None:
                od = dirname(p)
            out_path = '{0}.tif'.format(join(od, splitext(basename(p))[0]))
        else:
            eprint(p, "doesn't exist")
            continue

        # do the conversion
        try:
            convert(p, out_path)
        except Exception as e: 
            eprint(str(e))
            continue
        if args.verbose:
            print(out_path)

if __name__ == "__main__":
    main() 

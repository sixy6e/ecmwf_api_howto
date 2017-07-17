#!/usr/bin/env python

import datetime
import os
from os.path import join as pjoin, splitext, exists, dirname, basename
import luigi
from ecmwfapi import ECMWFDataServer
import rasterio


T2M = "temperature-2metre"
D2M = "dewpoint-temperature"
SP = "surface-pressure"
T = "temperature"
GP = "geo-potential"
RH = "relative-humidity"
WV = "water-vapour"

OUTPUT_FMT = "{product}-{start}-{end}.grib"
LEVEL_LIST = ("1/2/3/5/7/10/20/30/50/70/100/125/150/175/200/225/250/300/350/"
              "400/450/500/550/600/650/700/750/775/800/825/850/875/900/925/"
              "950/975/1000")


def retrieve_settings(start_date, end_date, product, surface=True):
    product_code = {T2M: "167.128",
                    D2M: "168.128",
                    SP: "134.128",
                    GP: "129.128",
                    T: "130.128",
                    RH: "157.128",
                    WV: "137.128"}

    dt_fmt = "{start}/to/{end}"

    settings = {"class": "ei",
                "dataset": "interim",
                "expver": "1",
                "grid": "0.125/0.125",
                "stream": "oper",
                "time": "00:00:00",
                "step": "0",
                "area": "0/100/-50/160",
                "type": "an"}

    settings["levtype"] = "sfc" if surface else "pl"
    settings["param"] = product_code[product]
    settings["date"] = dt_fmt.format(start=start_date.strftime('%Y-%m-%d'),
                                     end=end_date.strftime('%Y-%m-%d'))

    if surface:
        settings["levtype"] = "sfc"
    else:
        settings["levtype"] = "pl"
        settings["levelist"] = LEVEL_LIST

    return settings.copy()


def retrieve_out_fname(day, product):
    output_fmt = "{product}/{year}/grib/{product}_{day}.grib"
    ymd = day.strftime('%Y-%m-%d')
    out_fname = output_fmt.format(product=product, year=day.year, day=ymd)
    return out_fname


def convert_format(fname, out_fname):
    with rasterio.open(fname) as ds:
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

        with rasterio.open(out_fname, 'w', **kwargs) as outds:
            for i in range(1, ds.count +1):
                outds.write(ds.read(i), i)
                outds.update_tags(i, **ds.tags(i))


class InvariantGeoPotential(luigi.Task):

    outdir = luigi.Parameter()

    def output(self):
        out_fname = pjoin(self.outdir, 'invariant',
                          'geo-potential-dem.grib')
        return luigi.LocalTarget(out_fname)

    def run(self):
        server = ECMWFDataServer()
        settings = {"class": "ei",
                    "dataset": "interim",
                    "date": "1989-01-01",
                    "expver": "1",
                    "grid": "0.125/0.125",
                    "levtype": "sfc",
                    "param": "129.128",
                    "step": "0",
                    "stream": "oper",
                    "time": "12:00:00",
                    "type": "an"}
        with self.output().temporary_path() as out_fname:
            settings["target"] = out_fname
            server.retrieve(settings)


class TotalColumnWaterVapour(luigi.Task):

    day = luigi.DateParameter()
    outdir = luigi.Parameter()

    def output(self):
        out_fname = retrieve_out_fname(self.day, WV)
        return luigi.LocalTarget(pjoin(self.outdir, out_fname))

    def run(self):
        server = ECMWFDataServer()
        settings = retrieve_settings(self.day, self.day, WV)
        with self.output().temporary_path() as out_fname:
            settings["target"] = out_fname
            server.retrieve(settings)


class Temperature2m(luigi.Task):

    day = luigi.DateParameter()
    outdir = luigi.Parameter()

    def output(self):
        out_fname = retrieve_out_fname(self.day, T2M)
        return luigi.LocalTarget(pjoin(self.outdir, out_fname))

    def run(self):
        server = ECMWFDataServer()
        settings = retrieve_settings(self.day, self.day, T2M)
        with self.output().temporary_path() as out_fname:
            settings["target"] = out_fname
            server.retrieve(settings)


class DewPointTemperature(luigi.Task):

    day = luigi.DateParameter()
    outdir = luigi.Parameter()

    def output(self):
        out_fname = retrieve_out_fname(self.day, D2M)
        return luigi.LocalTarget(pjoin(self.outdir, out_fname))

    def run(self):
        server = ECMWFDataServer()
        settings = retrieve_settings(self.day, self.day, D2M)
        with self.output().temporary_path() as out_fname:
            settings["target"] = out_fname
            server.retrieve(settings)


class SurfacePressure(luigi.Task):

    day = luigi.DateParameter()
    outdir = luigi.Parameter()

    def output(self):
        out_fname = retrieve_out_fname(self.day, SP)
        return luigi.LocalTarget(pjoin(self.outdir, out_fname))

    def run(self):
        server = ECMWFDataServer()
        settings = retrieve_settings(self.day, self.day, SP)
        with self.output().temporary_path() as out_fname:
            settings["target"] = out_fname
            server.retrieve(settings)


class GeoPotential(luigi.Task):

    day = luigi.DateParameter()
    outdir = luigi.Parameter()

    def output(self):
        out_fname = retrieve_out_fname(self.day, GP)
        return luigi.LocalTarget(pjoin(self.outdir, out_fname))

    def run(self):
        server = ECMWFDataServer()
        settings = retrieve_settings(self.day, self.day, GP, False)
        with self.output().temporary_path() as out_fname:
            settings["target"] = out_fname
            server.retrieve(settings)


class Temperature(luigi.Task):

    day = luigi.DateParameter()
    outdir = luigi.Parameter()

    def output(self):
        out_fname = retrieve_out_fname(self.day, T)
        return luigi.LocalTarget(pjoin(self.outdir, out_fname))

    def run(self):
        server = ECMWFDataServer()
        settings = retrieve_settings(self.day, self.day, T, False)
        with self.output().temporary_path() as out_fname:
            settings["target"] = out_fname
            server.retrieve(settings)


class RelativeHumidity(luigi.Task):

    day = luigi.DateParameter()
    outdir = luigi.Parameter()

    def output(self):
        out_fname = retrieve_out_fname(self.day, RH)
        return luigi.LocalTarget(pjoin(self.outdir, out_fname))

    def run(self):
        server = ECMWFDataServer()
        settings = retrieve_settings(self.day, self.day, RH, False)
        with self.output().temporary_path() as out_fname:
            settings["target"] = out_fname
            server.retrieve(settings)


class ConvertFormat(luigi.Task):

    task = luigi.TaskParameter()
    day = luigi.DateParameter()

    def requires(self):
        return self.task

    def output(self):
        fname = self.input().path
        base_dir = dirname(dirname(fname))
        out_fname = pjoin(base_dir, 'tif', basename(fname))
        return luigi.LocalTarget(splitext(out_fname)[0] + '.tif')

    def run(self):
        with self.output().temporary_path() as out_fname:
            convert_format(self.input().path, out_fname)

            # do we have an xml for large metadata info
            metadata_fname = out_fname + '.aux.xml'
            if exists(metadata_fname):
                new_fname = self.output().path + '.aux.xml'
                os.rename(metadata_fname, new_fname)


class DownloadEcwmfData(luigi.WrapperTask):

    """A helper task that submits specific product downloads."""

    year = luigi.DateIntervalParameter()
    outdir = luigi.Parameter()

    def requires(self):
        dates = self.year.dates()
        for date in dates:
            args = [date, self.outdir]
            yield ConvertFormat(Temperature2m(*args), date)
            yield ConvertFormat(DewPointTemperature(*args), date)
            yield ConvertFormat(SurfacePressure(*args), date)
            yield ConvertFormat(GeoPotential(*args), date)
            yield ConvertFormat(Temperature(*args), date)
            yield ConvertFormat(RelativeHumidity(*args), date)
            yield ConvertFormat(TotalColumnWaterVapour(*args), date)


if __name__ == "__main__":
    luigi.run()

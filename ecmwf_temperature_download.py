#!/usr/bin/env python

import datetime
from os.path import join as pjoin, splitext
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
                "area": "-5/110/-45/160",
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


def retrieve_out_fname(start_datetime, end_datetime, product):
    output_fmt = "{product}/{product}_{start}_{end}.grib"
    start_date = start_datetime.strftime('%Y-%m-%d')
    end_date = end_datetime.strftime('%Y-%m-%d')
    out_fname = output_fmt.format(product=product, start=start_date,
                                  end=end_date)
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

    output_dir = luigi.Parameter()

    def output(self):
        out_fname = pjoin(self.output_dir, 'invariant',
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

    start_date = luigi.DateParameter(default=datetime.date(1979, 1, 1))
    end_date = luigi.DateParameter(default=datetime.date.today())
    output_dir = luigi.Parameter()

    def output(self):
        out_fname = retrieve_out_fname(self.start_date, self.end_date, WV)
        return luigi.LocalTarget(pjoin(self.output_dir, out_fname))

    def run(self):
        server = ECMWFDataServer()
        settings = retrieve_settings(self.start_date, self.end_date, WV)
        with self.output().temporary_path() as out_fname:
            settings["target"] = out_fname
            server.retrieve(settings)


class Temperature2m(luigi.Task):

    start_date = luigi.DateParameter(default=datetime.date(1979, 1, 1))
    end_date = luigi.DateParameter(default=datetime.date.today())
    output_dir = luigi.Parameter()

    def output(self):
        out_fname = retrieve_out_fname(self.start_date, self.end_date, T2M)
        return luigi.LocalTarget(pjoin(self.output_dir, out_fname))

    def run(self):
        server = ECMWFDataServer()
        settings = retrieve_settings(self.start_date, self.end_date, T2M)
        with self.output().temporary_path() as out_fname:
            settings["target"] = out_fname
            server.retrieve(settings)


class DewPointTemperature(luigi.Task):

    start_date = luigi.DateParameter(default=datetime.date(1979, 1, 1))
    end_date = luigi.DateParameter(default=datetime.date.today())
    output_dir = luigi.Parameter()

    def output(self):
        out_fname = retrieve_out_fname(self.start_date, self.end_date, D2M)
        return luigi.LocalTarget(pjoin(self.output_dir, out_fname))

    def run(self):
        server = ECMWFDataServer()
        settings = retrieve_settings(self.start_date, self.end_date, D2M)
        with self.output().temporary_path() as out_fname:
            settings["target"] = out_fname
            server.retrieve(settings)


class SurfacePressure(luigi.Task):

    start_date = luigi.DateParameter(default=datetime.date(1979, 1, 1))
    end_date = luigi.DateParameter(default=datetime.date.today())
    output_dir = luigi.Parameter()

    def output(self):
        out_fname = retrieve_out_fname(self.start_date, self.end_date, SP)
        return luigi.LocalTarget(pjoin(self.output_dir, out_fname))

    def run(self):
        server = ECMWFDataServer()
        settings = retrieve_settings(self.start_date, self.end_date, SP)
        with self.output().temporary_path() as out_fname:
            settings["target"] = out_fname
            server.retrieve(settings)


class GeoPotential(luigi.Task):

    start_date = luigi.DateParameter(default=datetime.date(1979, 1, 1))
    end_date = luigi.DateParameter(default=datetime.date.today())
    output_dir = luigi.Parameter()

    def output(self):
        out_fname = retrieve_out_fname(self.start_date, self.end_date, GP)
        return luigi.LocalTarget(pjoin(self.output_dir, out_fname))

    def run(self):
        server = ECMWFDataServer()
        settings = retrieve_settings(self.start_date, self.end_date, GP, False)
        with self.output().temporary_path() as out_fname:
            settings["target"] = out_fname
            server.retrieve(settings)


class Temperature(luigi.Task):

    start_date = luigi.DateParameter(default=datetime.date(1979, 1, 1))
    end_date = luigi.DateParameter(default=datetime.date.today())
    output_dir = luigi.Parameter()

    def output(self):
        out_fname = retrieve_out_fname(self.start_date, self.end_date, T)
        return luigi.LocalTarget(pjoin(self.output_dir, out_fname))

    def run(self):
        server = ECMWFDataServer()
        settings = retrieve_settings(self.start_date, self.end_date, T, False)
        with self.output().temporary_path() as out_fname:
            settings["target"] = out_fname
            server.retrieve(settings)


class RelativeHumidity(luigi.Task):

    start_date = luigi.DateParameter(default=datetime.date(1979, 1, 1))
    end_date = luigi.DateParameter(default=datetime.date.today())
    output_dir = luigi.Parameter()

    def output(self):
        out_fname = retrieve_out_fname(self.start_date, self.end_date, RH)
        return luigi.LocalTarget(pjoin(self.output_dir, out_fname))

    def run(self):
        server = ECMWFDataServer()
        settings = retrieve_settings(self.start_date, self.end_date, RH, False)
        with self.output().temporary_path() as out_fname:
            settings["target"] = out_fname
            server.retrieve(settings)


class ConvertFormat(luigi.Task):

    task = luigi.TaskParameter()

    def requires(self):
        return self.task

    def output(self):
        out_fname = self.input().path
        return luigi.LocalTarget(splitext(out_fname)[0] + '.tif')

    def run(self):
        with self.output().temporary_path() as out_fname:
            convert_format(self.input().path, out_fname)

        # remove the downloaded file
        self.input().remove()


class DownloadEcwmfData(luigi.WrapperTask):

    """A helper task that submits specific product downloads."""

    year_month = luigi.DateIntervalParameter(batch_method=max)
    output_dir = luigi.Parameter()

    def requires(self):
        dates = self.year_month.dates()
        args = [dates[0], dates[-1], self.output_dir]
        reqs = {T2M: ConvertFormat(Temperature2m(*args)),
                D2M: ConvertFormat(DewPointTemperature(*args)),
                SP: ConvertFormat(SurfacePressure(*args)),
                GP: ConvertFormat(GeoPotential(*args)),
                T: ConvertFormat(Temperature(*args)),
                RH: ConvertFormat(RelativeHumidity(*args)),
                WV: ConvertFormat(TotalColumnWaterVapour(*args))}

        return reqs


if __name__ == "__main__":
    luigi.run()

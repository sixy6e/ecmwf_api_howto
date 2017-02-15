#!/usr/bin/env python

import datetime
from os.path import join as pjoin
import luigi
import argparse
from argparse import RawTextHelpFormatter
from ecmwfapi import ECMWFDataServer


T2M = "temperature-2metre"
D2M = "dewpoint-temperature"
SP = "surface-pressure"
T = "temperature"
GP = "geo-potential"
RH = "relative-humidity"

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
                    RH: "157.128"}

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
    output_fmt = "{product}_{start}_{end}.grib"
    start_date = start_datetime.strftime('%Y-%m-%d')
    end_date = end_datetime.strftime('%Y-%m-%d')
    out_fname = output_fmt.format(product=product, start=start_date,
                                  end=end_date)
    return out_fname


class Temperature2m(luigi.Task):

    start_date = luigi.DateParameter(default=datetime.date(1979, 01, 01))
    end_date = luigi.DateParameter(default=datetime.date.today())
    output_dir = luigi.Parameter()

    def output(self):
        out_fname = retrieve_out_fname(self.start_date, self.end_date, T2M)
        return {T2M: luigi.LocalTarget(pjoin(self.output_dir, out_fname))}

    def run(self):
        server = ECMWFDataServer()
        settings = retrieve_settings(self.start_date, self.end_date, T2M)
        with self.output()[T2M].temporary_path() as out_fname:
            settings["target"] = out_fname
            server.retrieve(settings)


class DewPointTemperature(luigi.Task):

    start_date = luigi.DateParameter(default=datetime.date(1979, 01, 01))
    end_date = luigi.DateParameter(default=datetime.date.today())
    output_dir = luigi.Parameter()

    def output(self):
        out_fname = retrieve_out_fname(self.start_date, self.end_date, D2M)
        return {D2M: luigi.LocalTarget(pjoin(self.output_dir, out_fname))}

    def run(self):
        server = ECMWFDataServer()
        settings = retrieve_settings(self.start_date, self.end_date, D2M)
        with self.output()[D2M].temporary_path() as out_fname:
            settings["target"] = out_fname
            server.retrieve(settings)


class SurfacePressure(luigi.Task):

    start_date = luigi.DateParameter(default=datetime.date(1979, 01, 01))
    end_date = luigi.DateParameter(default=datetime.date.today())
    output_dir = luigi.Parameter()

    def output(self):
        out_fname = retrieve_out_fname(self.start_date, self.end_date, SP)
        return {SP: luigi.LocalTarget(pjoin(self.output_dir, out_fname))}

    def run(self):
        server = ECMWFDataServer()
        settings = retrieve_settings(self.start_date, self.end_date, SP)
        with self.output()[SP].temporary_path() as out_fname:
            settings["target"] = out_fname
            server.retrieve(settings)


class GeoPotential(luigi.Task):

    start_date = luigi.DateParameter(default=datetime.date(1979, 01, 01))
    end_date = luigi.DateParameter(default=datetime.date.today())
    output_dir = luigi.Parameter()

    def output(self):
        out_fname = retrieve_out_fname(self.start_date, self.end_date, GP)
        return {GP: luigi.LocalTarget(pjoin(self.output_dir, out_fname))}

    def run(self):
        server = ECMWFDataServer()
        settings = retrieve_settings(self.start_date, self.end_date, GP, False)
        with self.output()[GP].temporary_path() as out_fname:
            settings["target"] = out_fname
            server.retrieve(settings)


class Temperature(luigi.Task):

    start_date = luigi.DateParameter(default=datetime.date(1979, 01, 01))
    end_date = luigi.DateParameter(default=datetime.date.today())
    output_dir = luigi.Parameter()

    def output(self):
        out_fname = retrieve_out_fname(self.start_date, self.end_date, T)
        return {T: luigi.LocalTarget(pjoin(self.output_dir, out_fname))}

    def run(self):
        server = ECMWFDataServer()
        settings = retrieve_settings(self.start_date, self.end_date, T, False)
        with self.output()[T].temporary_path() as out_fname:
            settings["target"] = out_fname
            server.retrieve(settings)


class RelativeHumidity(luigi.Task):

    start_date = luigi.DateParameter(default=datetime.date(1979, 01, 01))
    end_date = luigi.DateParameter(default=datetime.date.today())
    output_dir = luigi.Parameter()

    def output(self):
        out_fname = retrieve_out_fname(self.start_date, self.end_date, RH)
        return {RH: luigi.LocalTarget(pjoin(self.output_dir, out_fname))}

    def run(self):
        server = ECMWFDataServer()
        settings = retrieve_settings(self.start_date, self.end_date, RH, False)
        with self.output()[RH].temporary_path() as out_fname:
            settings["target"] = out_fname
            server.retrieve(settings)


class DownloadEcwmfData(luigi.WrapperTask):

    """A helper task that submits specific product downloads."""

    start_date = luigi.DateParameter(default=datetime.date(1979, 01, 01))
    end_date = luigi.DateParameter(default=datetime.date.today())
    output_dir = luigi.Parameter()

    def requires(self):
        args = [self.start_date, self.end_date, self.output_dir]
        reqs = {'2mT': Temperature2m(*args),
                'DT': DewPointTemperature(*args),
                'SP': SurfacePressure(*args),
                'GP': GeoPotential(*args),
                'T': Temperature(*args),
                'RH': RelativeHumidity(*args)}

        return reqs

if __name__ == "__main__":
    luigi.run()

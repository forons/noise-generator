#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import random

from .AbstractNoiseGen import AbstractNoiseGen
from pyspark.sql.types import *
from pyspark.sql import functions as F

factor = 10.


class ScaleGen(AbstractNoiseGen):
    """
    This class introduces a change of scale into the data by
    multiplying/dividing the given value.
    """

    def __init__(self, df, columns, distribution, given_factor=factor):
        super().__init__(df, columns, distribution)
        global factor
        if not given_factor:
            given_factor = factor
        else:
            given_factor = float(given_factor)
        factor = given_factor

    @staticmethod
    def description(**kwargs):
        return '{} multiplies/divides the input element by a factor of {}' \
            .format(ScaleGen.name(), factor)

    @staticmethod
    def name(**kwargs):
        return 'SCALE'

    @staticmethod
    def scale_generation(elem, distribution):
        if not distribution.generate(elem):
            if elem is None or not elem:
                return elem
            return elem * 1.
        if elem is None or not elem:
            return elem
        if random.random() < 0.5:
            return elem / factor
        return elem * factor

    def string_udf(self, distribution):
        pass

    def int_udf(self, distribution):
        return F.udf(lambda elem: ScaleGen.scale_generation(elem, distribution),
                     DoubleType())

    def double_udf(self, distribution):
        return F.udf(lambda elem: ScaleGen.scale_generation(elem, distribution),
                     DoubleType())

    def bigint_udf(self, distribution):
        return F.udf(lambda elem: ScaleGen.scale_generation(elem, distribution),
                     DoubleType())

    def tinyint_udf(self, distribution):
        return F.udf(lambda elem: ScaleGen.scale_generation(elem, distribution),
                     DoubleType())

    def decimal_udf(self, distribution):
        return F.udf(lambda elem: ScaleGen.scale_generation(elem, distribution),
                     DoubleType())

    def smallint_udf(self, distribution):
        return F.udf(lambda elem: ScaleGen.scale_generation(elem, distribution),
                     DoubleType())

    def date_udf(self, distribution):
        pass

    def timestamp_udf(self, distribution):
        pass

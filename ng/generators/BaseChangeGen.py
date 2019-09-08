#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from .AbstractNoiseGen import AbstractNoiseGen
from ..utils import baseconvert
from pyspark.sql.types import *
from pyspark.sql import functions as F

base = 2


class BaseChangeGen(AbstractNoiseGen):
    """
    This class is introduces a change of base into the data.
    """

    def __init__(self, df, columns, distribution, given_base=base):
        super().__init__(df, columns, distribution)
        global base
        if not given_base:
            given_base = base
        else:
            given_base = int(given_base)
        base = given_base

    @staticmethod
    def description(**kwargs):
        return '{} changes to base {} the input element' \
            .format(BaseChangeGen.name(), base)

    @staticmethod
    def name(**kwargs):
        return 'BASE CHANGE'

    @staticmethod
    def base_change_generation(elem, distribution):
        if not distribution.generate(elem):
            return elem
        if elem is None or not elem:
            return elem
        return baseconvert.base(elem, 10, base, string=True)

    def string_udf(self, distribution):
        pass

    def int_udf(self, distribution):
        return F.udf(lambda elem:
                     BaseChangeGen.base_change_generation(elem,
                                                          distribution),
                     StringType())

    def double_udf(self, distribution):
        return F.udf(lambda elem:
                     BaseChangeGen.base_change_generation(elem,
                                                          distribution),
                     StringType())

    def bigint_udf(self, distribution):
        return F.udf(lambda elem:
                     BaseChangeGen.base_change_generation(elem,
                                                          distribution),
                     StringType())

    def tinyint_udf(self, distribution):
        return F.udf(lambda elem:
                     BaseChangeGen.base_change_generation(elem,
                                                          distribution),
                     StringType())

    def decimal_udf(self, distribution):
        return F.udf(lambda elem:
                     BaseChangeGen.base_change_generation(elem,
                                                          distribution),
                     StringType())

    def smallint_udf(self, distribution):
        return F.udf(lambda elem:
                     BaseChangeGen.base_change_generation(elem,
                                                          distribution),
                     StringType())

    def date_udf(self, distribution):
        pass

    def timestamp_udf(self, distribution):
        pass

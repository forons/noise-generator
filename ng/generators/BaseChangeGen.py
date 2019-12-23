#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from .AbstractNoiseGen import AbstractNoiseGen
from noise.ng.utils import baseconvert
from pyspark.sql.types import *
from pyspark.sql import functions as F


class BaseChangeGen(AbstractNoiseGen):
    """
    This class is introduces a change of base into the data.
    """

    def __init__(self, df, columns, base=2):
        super().__init__(df, columns)
        self.base = base

    @staticmethod
    def description(**kwargs):
        return '{} changes the base of the input element' \
            .format(BaseChangeGen.name())

    @staticmethod
    def name(**kwargs):
        return 'BASE CHANGE'

    @staticmethod
    def base_change_generation(elem, distribution, base):
        if not distribution.generate(elem):
            return elem
        if elem is None or not elem:
            return elem
        return baseconvert.base(elem, 10, base, string=True)

    def string_udf(self, distribution):
        pass

    def int_udf(self, distribution):
        base = self.base
        return F.udf(lambda elem:
                     BaseChangeGen.base_change_generation(elem,
                                                          distribution,
                                                          base),
                     StringType())

    def double_udf(self, distribution):
        base = self.base
        return F.udf(lambda elem:
                     BaseChangeGen.base_change_generation(elem,
                                                          distribution,
                                                          base),
                     StringType())

    def bigint_udf(self, distribution):
        base = self.base
        return F.udf(lambda elem:
                     BaseChangeGen.base_change_generation(elem,
                                                          distribution,
                                                          base),
                     StringType())

    def tinyint_udf(self, distribution):
        base = self.base
        return F.udf(lambda elem:
                     BaseChangeGen.base_change_generation(elem,
                                                          distribution,
                                                          base),
                     StringType())

    def decimal_udf(self, distribution):
        base = self.base
        return F.udf(lambda elem:
                     BaseChangeGen.base_change_generation(elem,
                                                          distribution,
                                                          base),
                     StringType())

    def smallint_udf(self, distribution):
        base = self.base
        return F.udf(lambda elem:
                     BaseChangeGen.base_change_generation(elem,
                                                          distribution,
                                                          base),
                     StringType())

    def date_udf(self, distribution):
        pass

    def timestamp_udf(self, distribution):
        pass
    
    def __str__(self):
        return '{} - {}'.format(BaseChangeGen.name(), self.columns)

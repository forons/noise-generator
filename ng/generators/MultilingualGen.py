#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from .AbstractNoiseGen import AbstractNoiseGen
from pyspark.sql.types import *
from pyspark.sql import functions as F


class MultilingualGen(AbstractNoiseGen):
    """
    This class replaces the data with a translation.
    """

    def __init__(self, df, columns):
        super().__init__(df, columns)

    @staticmethod
    def description(**kwargs):
        return '{} translates into another language the input element'.format(MultilingualGen.name())

    @staticmethod
    def name(**kwargs):
        return 'MULTILINGUAL'

    @staticmethod
    def multilingual_generation(elem, distribution):
        pass

    def string_udf(self, distribution):
        return F.udf(lambda elem: MultilingualGen.multilingual_generation(elem, distribution),
                     StringType())

    def int_udf(self, distribution):
        pass

    def double_udf(self, distribution):
        pass

    def bigint_udf(self, distribution):
        pass

    def tinyint_udf(self, distribution):
        pass

    def decimal_udf(self, distribution):
        pass

    def smallint_udf(self, distribution):
        pass

    def date_udf(self, distribution):
        pass

    def timestamp_udf(self, distribution):
        pass

    def __str__(self):
        return '{} - {}'.format(MultilingualGen.name(), self.columns)
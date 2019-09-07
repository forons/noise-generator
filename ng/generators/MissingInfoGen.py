#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from .AbstractNoiseGen import AbstractNoiseGen
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType


class MissingInfoGen(AbstractNoiseGen):
    """
    This class is introduces missing tuples into the data.
    """

    def __init__(self, df, distribution):
        super().__init__(df, [], distribution)

    @staticmethod
    def description(**kwargs):
        return '{} removes the input tuples'.format(MissingInfoGen.name())

    @staticmethod
    def name(**kwargs):
        return 'MISSING INFO'

    @staticmethod
    def filter_func(elem, distribution):
        return not distribution.generate(elem)

    @staticmethod
    def filter_udf(distribution):
        return F.udf(
            lambda elem: MissingInfoGen.filter_func(elem, distribution),
            BooleanType())

    def generate(self):
        udf = self.filter_udf(self.distribution)
        if callable(udf):
            return self.df.filter(udf(self.df[self.df.columns[0]]))
        raise IndexError('The udf is not callable!')

    def string_udf(self, distribution):
        pass

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

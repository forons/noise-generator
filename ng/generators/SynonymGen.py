#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from .AbstractNoiseGen import AbstractNoiseGen
from pyspark.sql.types import *
from pyspark.sql import functions as F


class SynonymGen(AbstractNoiseGen):
    """
    This class replaces the data with synonyms.
    """

    def __init__(self, df, columns):
        super().__init__(df, columns)

    @staticmethod
    def description(**kwargs):
        return '{} replaces the input element with a synonym'.format(SynonymGen.name())

    @staticmethod
    def name(**kwargs):
        return 'SYNONYM'

    @staticmethod
    def synonym_generation(elem, distribution):
        pass

    def string_udf(self, distribution):
        return F.udf(lambda elem: SynonymGen.synonym_generation(elem, distribution),
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
        return '{} - {}'.format(SynonymGen.name(), self.columns)
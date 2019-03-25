#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from .AbstractNoiseGen import AbstractNoiseGen
from pyspark.sql.types import *
from pyspark.sql import functions as F


class NullGen(AbstractNoiseGen):
    """
    This class is introduces nulls into the data.
    """

    def __init__(self, df, columns, distribution):
        super().__init__(df, columns, distribution)

    @staticmethod
    def description(**kwargs):
        return '{} nullifies the input element'.format(NullGen.name())

    @staticmethod
    def name(**kwargs):
        return 'NULL'

    @staticmethod
    def null_generation(elem, distribution):
        if distribution.generate(elem):
            return None
        return elem

    def string_udf(self, distribution):
        return F.udf(lambda elem: NullGen.null_generation(elem, distribution), StringType())

    def int_udf(self, distribution):
        return F.udf(lambda elem: NullGen.null_generation(elem, distribution), IntegerType())

    def double_udf(self, distribution):
        return F.udf(lambda elem: NullGen.null_generation(elem, distribution), DoubleType())

    def bigint_udf(self, distribution):
        return F.udf(lambda elem: NullGen.null_generation(elem, distribution), LongType())

    def tinyint_udf(self, distribution):
        return F.udf(lambda elem: NullGen.null_generation(elem, distribution), ByteType())

    def decimal_udf(self, distribution):
        return F.udf(lambda elem: NullGen.null_generation(elem, distribution), DecimalType())

    def smallint_udf(self, distribution):
        return F.udf(lambda elem: NullGen.null_generation(elem, distribution), ShortType())

    def date_udf(self, distribution):
        return F.udf(lambda elem: NullGen.null_generation(elem, distribution), DateType())

    def timestamp_udf(self, distribution):
        return F.udf(lambda elem: NullGen.null_generation(elem, distribution), TimestampType())

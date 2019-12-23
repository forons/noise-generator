#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import math
from .AbstractNoiseGen import AbstractNoiseGen
from pyspark.sql.types import *
from pyspark.sql import functions as F


class AbbreviationGen(AbstractNoiseGen):
    """
    This class is introduces abbreviations into the data.
    """

    def __init__(self, df, columns):
        super().__init__(df, columns)

    @staticmethod
    def description(**kwargs):
        return '{} keeps only the first char or digit of the input value' \
            .format(AbbreviationGen.name())

    @staticmethod
    def name(**kwargs):
        return 'ABBREVIATION'

    @staticmethod
    def abbreviation_str_generation(elem, distribution):
        if not distribution.generate(elem):
            if isinstance(elem, str):
                return elem
            return str(elem)
        if elem is None or not elem:
            return elem
        if isinstance(elem, str):
            return elem[0]
        return str(elem)[:1]

    @staticmethod
    def abbreviation_num_generation(elem, distribution):
        if not distribution.generate(elem):
            return elem
        if elem is None or elem == 0:
            return elem
        if elem < 0:
            return -(-elem // 10 ** int(math.log(-elem, 10)))
        return elem // 10 ** int(math.log(elem, 10))

    def string_udf(self, distribution):
        return F.udf(
            lambda elem:
            AbbreviationGen.abbreviation_str_generation(elem, distribution),
            StringType())

    def int_udf(self, distribution):
        return F.udf(
            lambda elem:
            AbbreviationGen.abbreviation_num_generation(elem, distribution),
            IntegerType())

    def double_udf(self, distribution):
        return F.udf(
            lambda elem:
            AbbreviationGen.abbreviation_num_generation(elem, distribution),
            DoubleType())

    def bigint_udf(self, distribution):
        return F.udf(
            lambda elem:
            AbbreviationGen.abbreviation_num_generation(elem, distribution),
            LongType())

    def tinyint_udf(self, distribution):
        return F.udf(
            lambda elem:
            AbbreviationGen.abbreviation_num_generation(elem, distribution),
            ByteType())

    def decimal_udf(self, distribution):
        return F.udf(
            lambda elem:
            AbbreviationGen.abbreviation_num_generation(elem, distribution),
            DecimalType())

    def smallint_udf(self, distribution):
        return F.udf(
            lambda elem:
            AbbreviationGen.abbreviation_num_generation(elem, distribution),
            ShortType())

    def date_udf(self, distribution):
        return F.udf(
            lambda elem:
            AbbreviationGen.abbreviation_str_generation(elem, distribution),
            StringType())

    def timestamp_udf(self, distribution):
        return F.udf(
            lambda elem:
            AbbreviationGen.abbreviation_str_generation(elem, distribution),
            StringType())

    def __str__(self):
        return '{} - {}'.format(AbbreviationGen.name(), self.columns)
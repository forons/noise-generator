#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from .AbstractNoiseGen import AbstractNoiseGen
from pyspark.sql.types import *
from pyspark.sql import functions as F


class NegationGen(AbstractNoiseGen):
    """
    This class is introduces a change of the sign of the data.
    """

    def __init__(self, df, columns):
        super().__init__(df, columns)

    @staticmethod
    def description(**kwargs):
        return '{} changes to sign of the input element' \
            .format(NegationGen.name())

    @staticmethod
    def name(**kwargs):
        return 'NEGATION'

    @staticmethod
    def negation_generation(elem, distribution):
        if not distribution.generate(elem):
            return elem
        if elem is None or not elem:
            return elem
        return -elem

    def string_udf(self, distribution):
        pass

    def int_udf(self, distribution):
        return F.udf(
            lambda elem: NegationGen.negation_generation(elem, distribution),
            IntegerType())

    def double_udf(self, distribution):
        return F.udf(
            lambda elem: NegationGen.negation_generation(elem, distribution),
            DoubleType())

    def bigint_udf(self, distribution):
        return F.udf(
            lambda elem: NegationGen.negation_generation(elem, distribution),
            LongType())

    def tinyint_udf(self, distribution):
        return F.udf(
            lambda elem: NegationGen.negation_generation(elem, distribution),
            BinaryType())

    def decimal_udf(self, distribution):
        return F.udf(
            lambda elem: NegationGen.negation_generation(elem, distribution),
            DecimalType())

    def smallint_udf(self, distribution):
        return F.udf(
            lambda elem: NegationGen.negation_generation(elem, distribution),
            ShortType())

    def date_udf(self, distribution):
        pass

    def timestamp_udf(self, distribution):
        pass

    def __str__(self):
        return '{} - {}'.format(NegationGen.name(), self.columns)
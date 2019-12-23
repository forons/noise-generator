#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import random

from .AbstractNoiseGen import AbstractNoiseGen
from pyspark.sql.types import *
from pyspark.sql import functions as F


class ReplaceGen(AbstractNoiseGen):
    """
    This class introduces a change of scale into the data by
    multiplying/dividing the given value.
    """

    def __init__(self, df, columns, replacements=[]):
        super().__init__(df, columns)
        self.replacements = replacements

    @staticmethod
    def description(**kwargs):
        return '{} replaces the input with a value in the list {}' \
            .format(ReplaceGen.name(), self.replacements)

    @staticmethod
    def name(**kwargs):
        return 'REPLACE'

    @staticmethod
    def replace_generation(elem, distribution, replacements):
        if not distribution.generate(elem):
            return elem
        if elem is None or not elem:
            return elem
        if len(replacements) == 0:
            return None
        return random.choice(replacements)

    def string_udf(self, distribution):
        replacements = self.replacements
        return F.udf(
            lambda elem: ReplaceGen.replace_generation(elem, distribution, replacements),
            StringType())

    def int_udf(self, distribution):
        replacements = self.replacements
        return F.udf(
            lambda elem: ReplaceGen.replace_generation(elem, distribution, replacements),
            IntegerType())

    def double_udf(self, distribution):
        replacements = self.replacements
        return F.udf(
            lambda elem: ReplaceGen.replace_generation(elem, distribution, replacements),
            DoubleType())

    def bigint_udf(self, distribution):
        replacements = self.replacements
        return F.udf(
            lambda elem: ReplaceGen.replace_generation(elem, distribution, replacements),
            LongType())

    def tinyint_udf(self, distribution):
        replacements = self.replacements
        return F.udf(
            lambda elem: ReplaceGen.replace_generation(elem, distribution, replacements),
            IntegerType())

    def decimal_udf(self, distribution):
        replacements = self.replacements
        return F.udf(
            lambda elem: ReplaceGen.replace_generation(elem, distribution, replacements),
            IntegerType())

    def smallint_udf(self, distribution):
        replacements = self.replacements
        return F.udf(
            lambda elem: ReplaceGen.replace_generation(elem, distribution, replacements),
            IntegerType())

    def date_udf(self, distribution):
        replacements = self.replacements
        return F.udf(
            lambda elem: ReplaceGen.replace_generation(elem, distribution, replacements),
            DateType())

    def timestamp_udf(self, distribution):
        replacements = self.replacements
        return F.udf(
            lambda elem: ReplaceGen.replace_generation(elem, distribution, replacements),
            TimestampType())
    
    def __str__(self):
        return '{} - {}'.format(ReplaceGen.name(), self.columns)

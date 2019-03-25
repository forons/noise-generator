#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import random

from .AbstractNoiseGen import AbstractNoiseGen
from pyspark.sql.types import *
from pyspark.sql import functions as F

replacements = []


class ReplaceGen(AbstractNoiseGen):
    """
    This class introduces a change of scale into the data by multiplying/dividing the given value.
    """

    def __init__(self, df, columns, distribution, given_replacements=replacements):
        super().__init__(df, columns, distribution)
        global replacements
        if not given_replacements:
            given_replacements = replacements
        replacements = given_replacements

    @staticmethod
    def description(**kwargs):
        return '{} replaces the input with a value in the list {}'.format(ReplaceGen.name(), replacements)

    @staticmethod
    def name(**kwargs):
        return 'REPLACE'

    @staticmethod
    def replace_generation(elem, distribution):
        if not distribution.generate(elem):
            return elem
        if elem is None or not elem:
            return elem
        if len(replacements) == 0:
            return None
        return random.choice(replacements)

    def string_udf(self, distribution):
        return F.udf(lambda elem: ReplaceGen.replace_generation(elem, distribution), StringType())

    def int_udf(self, distribution):
        return F.udf(lambda elem: ReplaceGen.replace_generation(elem, distribution), IntegerType())

    def double_udf(self, distribution):
        return F.udf(lambda elem: ReplaceGen.replace_generation(elem, distribution), DoubleType())

    def bigint_udf(self, distribution):
        return F.udf(lambda elem: ReplaceGen.replace_generation(elem, distribution), LongType())

    def tinyint_udf(self, distribution):
        return F.udf(lambda elem: ReplaceGen.replace_generation(elem, distribution), IntegerType())

    def decimal_udf(self, distribution):
        return F.udf(lambda elem: ReplaceGen.replace_generation(elem, distribution), IntegerType())

    def smallint_udf(self, distribution):
        return F.udf(lambda elem: ReplaceGen.replace_generation(elem, distribution), IntegerType())

    def date_udf(self, distribution):
        return F.udf(lambda elem: ReplaceGen.replace_generation(elem, distribution), DateType())

    def timestamp_udf(self, distribution):
        return F.udf(lambda elem: ReplaceGen.replace_generation(elem, distribution), TimestampType())

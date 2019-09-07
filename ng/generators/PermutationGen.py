#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import random
from .AbstractNoiseGen import AbstractNoiseGen
from pyspark.sql.types import *
from pyspark.sql import functions as F

shuffle = False


class PermutationGen(AbstractNoiseGen):
    """
    This class introduces a permutation in a number or word.
    """

    def __init__(self, df, columns, distribution, given_shuffle=shuffle):
        super().__init__(df, columns, distribution)
        global shuffle
        if not given_shuffle:
            given_shuffle = shuffle
        else:
            given_shuffle = bool(given_shuffle)
        shuffle = given_shuffle

    @staticmethod
    def description(**kwargs):
        return f'{PermutationGen.name()} creates a permutation of ' \
               f'two elements of the same word or number or shuffles the input'

    @staticmethod
    def name(**kwargs):
        return 'PERMUTATION'

    @staticmethod
    def permutation_generation(elem, is_shuffle):
        if is_shuffle:
            return PermutationGen.create_shuffle(elem)
        return PermutationGen.create_permutation(elem)

    @staticmethod
    def create_shuffle(elem):
        return ''.join(random.sample(elem, len(elem)))

    @staticmethod
    def create_permutation(elem):
        position = random.randint(0, len(elem) - 1)
        if random.random() < 0.5 and position != 0 or position == len(elem) - 1:
            first_part = elem[:max(0, position - 1)]
            last_part = elem[min(position + 1, len(elem)):]
            return first_part + elem[position] + elem[position - 1] + last_part
        else:
            first_part = elem[:position]
            last_part = elem[min(position + 2, len(elem)):]
            return first_part + elem[position + 1] + elem[position] + last_part

    @staticmethod
    def permute(elem, distribution, is_shuffle):
        if not distribution.generate(elem):
            return elem
        if elem is None or not elem:
            return elem
        if isinstance(elem, str):
            return PermutationGen.permutation_generation(elem, is_shuffle)
        if isinstance(elem, int):
            to_return = int(PermutationGen.permutation_generation(str(elem)[:1],
                                                                  is_shuffle))
            if elem < 0:
                return -to_return
            return to_return
        if isinstance(elem, float):
            is_negative = elem < 0.
            has_exponent = str(elem).upper().find('E') != -1
            chars = str(elem).upper()
            if is_negative:
                chars = chars[:1]
            if has_exponent:
                chars = chars.split('E')[0]
            chars = PermutationGen.permutation_generation(chars, is_shuffle)
            if has_exponent:
                chars = chars + 'E'.join(chars.split('E')[1:])
            value = float(chars)
            if is_negative:
                return -value
            return value
        raise IndexError(f'The type {type(elem)} is not supported')

    def string_udf(self, distribution):
        return F.udf(lambda elem: PermutationGen.permute(elem, distribution,
                                                         is_shuffle=shuffle),
                     StringType())

    def int_udf(self, distribution):
        return F.udf(lambda elem: PermutationGen.permute(elem, distribution,
                                                         is_shuffle=shuffle),
                     LongType())

    def double_udf(self, distribution):
        return F.udf(lambda elem: PermutationGen.permute(elem, distribution,
                                                         is_shuffle=shuffle),
                     DoubleType())

    def bigint_udf(self, distribution):
        return F.udf(lambda elem: PermutationGen.permute(elem, distribution,
                                                         is_shuffle=shuffle),
                     LongType())

    def tinyint_udf(self, distribution):
        return F.udf(lambda elem: PermutationGen.permute(elem, distribution,
                                                         is_shuffle=shuffle),
                     IntegerType())

    def decimal_udf(self, distribution):
        return F.udf(lambda elem: PermutationGen.permute(elem, distribution,
                                                         is_shuffle=shuffle),
                     IntegerType())

    def smallint_udf(self, distribution):
        return F.udf(lambda elem: PermutationGen.permute(elem, distribution,
                                                         is_shuffle=shuffle),
                     IntegerType())

    def date_udf(self, distribution):
        pass

    def timestamp_udf(self, distribution):
        pass

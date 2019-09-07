#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import random
import re
from .AbstractNoiseGen import AbstractNoiseGen
from pyspark.sql.types import *
from pyspark.sql import functions as F

max_acronyms = 4


class AcronymGen(AbstractNoiseGen):
    """
    This class introduces acronyms into the data.
    """

    def __init__(self, df, columns, distribution, given_max_acronyms=None):
        super().__init__(df, columns, distribution)
        global max_acronyms
        if not given_max_acronyms:
            given_max_acronyms = max_acronyms
        else:
            given_max_acronyms = int(given_max_acronyms)
        max_acronyms = given_max_acronyms

    @staticmethod
    def description(**kwargs):
        return f'{AcronymGen.name()} creates an acronym ' \
               f'of maximum {max_acronyms} words'

    @staticmethod
    def name(**kwargs):
        return 'ACRONYM'

    @staticmethod
    def create_acronym(elem, max_length):
        words = re.split('\s', elem)
        non_empty_words = 0
        for word in words:
            if word.strip():
                non_empty_words = non_empty_words + 1
        if not words or non_empty_words == 0 or max_length <= 0:
            return elem

        from_word_index = random.randint(0, len(words) - 1)
        to_word_index = -1
        while not words[from_word_index]:
            from_word_index = random.randint(0, len(words) - 1)

        len_acronym = max(1, random.randint(1, min(max_length,
                                                   non_empty_words + 1)))
        non_empty_words = 0
        for idx in range(from_word_index, len(words) - 1):
            if words[idx]:
                non_empty_words = non_empty_words + 1
                if non_empty_words >= len_acronym:
                    to_word_index = idx
                    break

        if to_word_index < 0 and from_word_index + len_acronym > non_empty_words:
            to_word_index = len(words) - 1

        if from_word_index == 0:
            from_char_index = 0
        else:
            from_char_index = AcronymGen.find_nth_overlapping(elem, ' ',
                                                              from_word_index) + 1
        to_char_index = AcronymGen.find_nth_overlapping(elem, ' ',
                                                        to_word_index + 1)
        if to_char_index == -1:
            to_char_index = len(elem)

        output = elem[:from_char_index]
        non_empty_words = 0
        for idx in range(from_word_index, to_word_index + 1):
            if words[idx]:
                if non_empty_words >= len_acronym:
                    break
                output = output + words[idx][:1].upper()
                non_empty_words = non_empty_words + 1
        return output + elem[to_char_index:]

    @staticmethod
    def find_nth_overlapping(haystack, needle, n):
        start = haystack.find(needle)
        while start >= 0 and n > 1:
            start = haystack.find(needle, start + 1)
            n -= 1
        return start

    @staticmethod
    def acronym_generation(elem, distribution):
        if not distribution.generate(elem):
            return elem
        if elem is None or not elem:
            return elem
        return AcronymGen.create_acronym(elem, max_acronyms)

    def string_udf(self, distribution):
        return F.udf(
            lambda elem: AcronymGen.acronym_generation(elem, distribution),
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

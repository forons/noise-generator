#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import random
import re
from .AbstractNoiseGen import AbstractNoiseGen
from pyspark.sql.types import *
from pyspark.sql import functions as F

num_shuffles = 3


class ShufflingGen(AbstractNoiseGen):
    """
    This class introduces word shuffling into the data.
    """

    def __init__(self, df, columns, distribution, given_num_shuffles=num_shuffles):
        super().__init__(df, columns, distribution)
        global num_shuffles
        if not given_num_shuffles:
            given_num_shuffles = num_shuffles
        else:
            given_num_shuffles = int(given_num_shuffles)
        num_shuffles = given_num_shuffles

    @staticmethod
    def description(**kwargs):
        return '{} shuffles up to {} words in the input element'.format(ShufflingGen.name(), num_shuffles)

    @staticmethod
    def name(**kwargs):
        return 'SHUFFLING'

    @staticmethod
    def shuffling_generation(elem, distribution):
        if not distribution.generate(elem):
            return elem
        if elem is None or not elem:
            return elem
        words = re.split('\s', elem)
        non_empty_words = 0
        for word in words:
            if word.strip():
                non_empty_words = non_empty_words + 1
        if not words or non_empty_words == 0:
            return elem
        count = 0
        while count < num_shuffles:
            words = ShufflingGen.generate_shuffle(words)
            count = count + 1
        return ' '.join(words)

    @staticmethod
    def generate_shuffle(words):
        if len(words) <= 1:
            return words
        fst_word_idx = random.randint(0, len(words) - 1)
        while not words[fst_word_idx]:
            fst_word_idx = random.randint(0, len(words) - 1)
        snd_word_idx = random.randint(0, len(words) - 1)
        while not words[snd_word_idx] or fst_word_idx == snd_word_idx:
            snd_word_idx = random.randint(0, len(words) - 1)
        tmp = words[fst_word_idx]
        words[fst_word_idx] = words[snd_word_idx]
        words[snd_word_idx] = tmp
        return words

    @staticmethod
    def find_nth_overlapping(haystack, needle, n):
        start = haystack.find(needle)
        while start >= 0 and n > 1:
            start = haystack.find(needle, start + 1)
            n -= 1
        return start

    def string_udf(self, distribution):
        return F.udf(lambda elem: ShufflingGen.shuffling_generation(elem, distribution), StringType())

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

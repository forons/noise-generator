#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import random
import string
from .AbstractNoiseGen import AbstractNoiseGen
from .Edit import Edit
from pyspark.sql.types import *
from pyspark.sql import functions as F

num_edits = 3


class EditGen(AbstractNoiseGen):
    """
    This class introduces edits (according to the edit distance) into the data.
    """

    def __init__(self, df, columns, distribution, given_num_edits=num_edits):
        super().__init__(df, columns, distribution)
        global num_edits
        if not given_num_edits:
            given_num_edits = num_edits
        else:
            given_num_edits = int(given_num_edits)
        num_edits = given_num_edits

    @staticmethod
    def description(**kwargs):
        return '{} edits the given value with {} changes'.format(EditGen.name(),
                                                                 num_edits)

    @staticmethod
    def name(**kwargs):
        return 'EDIT'

    @staticmethod
    def create_edit(elem, max_changes, only_numbers):
        for n in range(0, max_changes):
            choice = random.choice(list(Edit))
            if choice is Edit.INSERTION:
                elem = EditGen.create_insertion(elem, only_numbers)
            elif choice is Edit.SUBSTITUTION:
                elem = EditGen.create_substitution(elem, only_numbers)
            elif choice is Edit.DELETION:
                elem = EditGen.create_deletion(elem)
            else:
                raise IndexError(
                    'The value {} is not a supported value'.format(choice))
        return elem

    @staticmethod
    def create_insertion(elem, only_numbers):
        if only_numbers:
            char = random.choice(string.digits)
        else:
            char = random.choice(string.ascii_letters)
        if not elem:
            return elem + char
        position = random.randint(0, len(elem))
        return elem[:position] + char + elem[position:]

    @staticmethod
    def create_substitution(elem, only_numbers):
        if not elem:
            return elem
        position = random.randint(0, len(elem) - 1)
        char = elem[position]
        while char == elem[position]:
            if only_numbers:
                char = random.choice(string.digits)
            else:
                char = random.choice(string.ascii_letters)
        return elem[:max(0, position)] + char + elem[
                                                min(position + 1, len(elem)):]

    @staticmethod
    def create_deletion(elem):
        if not elem:
            return elem
        position = random.randint(0, len(elem) - 1)
        return elem[:position] + elem[position + 1:]

    @staticmethod
    def acronym_generation(elem, distribution, only_numbers=False):
        if not distribution.generate(elem):
            return elem
        if elem is None or not elem:
            return elem
        if isinstance(elem, int):
            return int(EditGen.create_edit(str(elem), num_edits, only_numbers))
        elif isinstance(elem, float):
            return float(
                EditGen.create_edit(str(elem), num_edits, only_numbers))
        elif isinstance(elem, str):
            return EditGen.create_edit(str(elem), num_edits, only_numbers)
        raise IndexError('Type {} not supported'.format(type(elem)))

    def string_udf(self, distribution):
        return F.udf(
            lambda elem: EditGen.acronym_generation(elem, distribution),
            StringType())

    def int_udf(self, distribution):
        return F.udf(lambda elem: EditGen.acronym_generation(elem, distribution,
                                                             only_numbers=True),
                     IntegerType())

    def double_udf(self, distribution):
        return F.udf(lambda elem: EditGen.acronym_generation(elem, distribution,
                                                             only_numbers=True),
                     DoubleType())

    def bigint_udf(self, distribution):
        return F.udf(lambda elem: EditGen.acronym_generation(elem, distribution,
                                                             only_numbers=True),
                     LongType())

    def tinyint_udf(self, distribution):
        return F.udf(lambda elem: EditGen.acronym_generation(elem, distribution,
                                                             only_numbers=True),
                     IntegerType())

    def decimal_udf(self, distribution):
        return F.udf(lambda elem: EditGen.acronym_generation(elem, distribution,
                                                             only_numbers=True),
                     IntegerType())

    def smallint_udf(self, distribution):
        return F.udf(lambda elem: EditGen.acronym_generation(elem, distribution,
                                                             only_numbers=True),
                     IntegerType())

    def date_udf(self, distribution):
        pass

    def timestamp_udf(self, distribution):
        pass

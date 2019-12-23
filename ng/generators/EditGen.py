#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import random
import string
from .AbstractNoiseGen import AbstractNoiseGen
from .Edit import Edit
from pyspark.sql.types import *
from pyspark.sql import functions as F


class EditGen(AbstractNoiseGen):
    """
    This class introduces edits (according to the edit distance) into the data.
    """

    def __init__(self, df, columns, edits=3):
        super().__init__(df, columns)
        self.edits = edits

    @staticmethod
    def description(**kwargs):
        return '{} edits the given value with some changes' \
            .format(EditGen.name())

    @staticmethod
    def name(**kwargs):
        return 'EDIT'

    @staticmethod
    def create_edit(elem, edits, only_numbers):
        for n in range(edits):
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
    def edit_generation(elem, distribution, edits, only_numbers=False):
        if not distribution.generate(elem):
            return elem
        if elem is None or not elem:
            return elem
        if isinstance(elem, int) or isinstance(elem, float) or isinstance(elem, str):
            ret = EditGen.create_edit(str(elem), edits, only_numbers)
        else:
            raise IndexError('Type {} not supported'.format(type(elem)))
        if isinstance(elem, int):
            if not ret:
                ret = 0
            return int(ret)
        elif isinstance(elem, float):
            if not ret:
                ret = 0
            return float(ret)
        return ret

    def string_udf(self, distribution):
        edits = self.edits
        return F.udf(lambda elem: EditGen.edit_generation(elem, distribution,
                                                          edits,
                                                          only_numbers=False),
                     StringType())

    def int_udf(self, distribution):
        edits = self.edits
        return F.udf(lambda elem: EditGen.edit_generation(elem, distribution,
                                                          edits,
                                                          only_numbers=True),
                     IntegerType())

    def double_udf(self, distribution):
        edits = self.edits
        return F.udf(lambda elem: EditGen.edit_generation(elem, distribution,
                                                          edits,
                                                          only_numbers=True),
                     DoubleType())

    def bigint_udf(self, distribution):
        edits = self.edits
        return F.udf(lambda elem: EditGen.edit_generation(elem, distribution,
                                                          edits,
                                                          only_numbers=True),
                     LongType())

    def tinyint_udf(self, distribution):
        edits = self.edits
        return F.udf(lambda elem: EditGen.edit_generation(elem, distribution,
                                                          edits,
                                                          only_numbers=True),
                     IntegerType())

    def decimal_udf(self, distribution):
        edits = self.edits
        return F.udf(lambda elem: EditGen.edit_generation(elem, distribution,
                                                          edits,
                                                          only_numbers=True),
                     IntegerType())

    def smallint_udf(self, distribution):
        edits = self.edits
        return F.udf(lambda elem: EditGen.edit_generation(elem, distribution,
                                                          edits,
                                                          only_numbers=True),
                     IntegerType())

    def date_udf(self, distribution):
        pass

    def timestamp_udf(self, distribution):
        pass

    def __str__(self):
        return '{} - {}'.format(EditGen.name(), self.columns)
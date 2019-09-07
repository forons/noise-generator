#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import unittest
import findspark
import logging
from ng import generator
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

spark = SparkSession.builder.appName('NoiseGenerators - Test').getOrCreate()
df = spark.read.csv('/Users/forons/Desktop/test.csv', inferSchema=True,
                    header=True, nullValue='null')


class BasicTestSuite(unittest.TestCase):
    """Advanced test cases."""

    def test_abbreviation(self):
        columns = [0, 1, 2, 3, 4, 5, 6, 7]
        self.assertIs(type(
            generator.generate_noise(df, 'ABBREVIATION', 'UNIFORM', columns)),
                      DataFrame)

    def test_acronym(self):
        columns = [1, 4, 5, 7]
        self.assertIs(
            type(generator.generate_noise(df, 'ACRONYM', 'UNIFORM', columns)),
            DataFrame)

    def test_base_change(self):
        columns = [0, 2, 3, 6]
        self.assertIs(type(
            generator.generate_noise(df, 'BASE_CHANGE', 'UNIFORM', columns)),
                      DataFrame)

    def test_edit(self):
        columns = [1, 4, 5, 7]
        self.assertIs(
            type(generator.generate_noise(df, 'EDIT', 'UNIFORM', columns)),
            DataFrame)

    def test_missing_info(self):
        self.assertIs(
            type(generator.generate_noise(df, 'MISSING_INFO', 'UNIFORM')),
            DataFrame)

    def test_null(self):
        columns = [0, 1, 2]
        self.assertIs(
            type(generator.generate_noise(df, 'NULL', 'UNIFORM', columns)),
            DataFrame)

    def test_negation(self):
        columns = [0, 2, 3, 6]
        self.assertIs(
            type(generator.generate_noise(df, 'NEGATION', 'UNIFORM', columns)),
            DataFrame)

    def test_permutation(self):
        columns = [1, 4, 5, 7]
        self.assertIs(type(
            generator.generate_noise(df, 'PERMUTATION', 'UNIFORM', columns)),
                      DataFrame)

    def test_replace(self):
        columns = [1, 5, 7]
        self.assertIs(
            type(generator.generate_noise(df, 'REPLACE', 'UNIFORM', columns)),
            DataFrame)

    def test_scale(self):
        columns = [0, 2, 3, 6]
        self.assertIs(
            type(generator.generate_noise(df, 'SCALE', 'UNIFORM', columns)),
            DataFrame)

    def test_shift(self):
        columns = [0, 2, 3, 6]
        self.assertIs(
            type(generator.generate_noise(df, 'SHIFT', 'UNIFORM', columns)),
            DataFrame)

    def test_shuffling(self):
        columns = [1, 4, 5, 7]
        self.assertIs(
            type(generator.generate_noise(df, 'SHUFFLING', 'UNIFORM', columns)),
            DataFrame)


if __name__ == '__main__':
    findspark.init('/Users/forons/Downloads/spark-2.3.2-bin-hadoop2.7')
    logging.basicConfig(format='%(asctime)s - %(name)s - %(message)s',
                        level=logging.INFO)
    unittest.main()

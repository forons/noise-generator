#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from abc import ABC, abstractmethod
from functools import reduce


class AbstractNoiseGen(ABC):
    """
    This class is an abstract class for the
    generation of the noise into the data.
    """

    def __init__(self, df, columns, distribution):
        self.df = df
        self.columns = columns
        self.distribution = distribution

    @staticmethod
    def description(self):
        pass

    @staticmethod
    def name(self):
        pass

    def generate(self):
        df = reduce(
            lambda data, col: data.transform(
                lambda ds: self.generate_noise(ds, col)),
            self.columns,
            self.df)
        return df

    def generate_noise(self, df, col):
        _, col_type = df.dtypes[col]
        typed_udf = self.get_typed_udf(col_type)
        if callable(typed_udf):
            return df.withColumn(df.columns[col], typed_udf(df.columns[col]))
        else:
            IndexError(
                'The given typed udf -{}-is not callable'.format(typed_udf))
            return df

    def get_typed_udf(self, col_type):
        if col_type == 'string':
            return self.string_udf(self.distribution)
        if col_type == 'int':
            return self.int_udf(self.distribution)
        if col_type == 'double':
            return self.double_udf(self.distribution)
        if col_type == 'bigint':
            return self.bigint_udf(self.distribution)
        if col_type == 'date':
            return self.date_udf(self.distribution)
        if col_type == 'timestamp':
            return self.timestamp_udf(self.distribution)
        raise IndexError(
            'The given value {} is not a column type supported'
                .format(col_type))

    @abstractmethod
    def string_udf(self, distribution):
        pass

    @abstractmethod
    def int_udf(self, distribution):
        pass

    @abstractmethod
    def double_udf(self, distribution):
        pass

    @abstractmethod
    def bigint_udf(self, distribution):
        pass

    @abstractmethod
    def tinyint_udf(self, distribution):
        # byte
        pass

    @abstractmethod
    def decimal_udf(self, distribution):
        # decimal
        pass

    @abstractmethod
    def smallint_udf(self, distribution):
        # short
        pass

    @abstractmethod
    def date_udf(self, distribution):
        pass

    @abstractmethod
    def timestamp_udf(self, distribution):
        pass

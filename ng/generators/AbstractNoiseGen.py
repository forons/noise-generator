#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from abc import ABC, abstractmethod
from functools import reduce


class AbstractNoiseGen(ABC):
    """
    This class is an abstract class for the
    generation of the noise into the data.
    """

    def __init__(self, df, columns):
        self.df = df
        self.columns = columns

    @staticmethod
    def description():
        pass

    @staticmethod
    def name():
        pass

    def generate(self, distribution):
        df = reduce(
            lambda data, col: data.transform(
                lambda ds: self.generate_noise(ds, col, distribution)),
            self.columns,
            self.df)
        return df
#         df = self.df
#         for col in self.columns:
#             df = df.transform(lambda elem: self.generate_noise(elem, col, distribution))
#         return df

    def generate_noise(self, df, col, distribution):
        _, col_type = df.dtypes[col]
        typed_udf = self.get_typed_udf(col_type, distribution)
        if callable(typed_udf):
            return df.withColumn(df.columns[col], typed_udf(df.columns[col]))
        else:
            # raise IndexError('Udf -{}-is not callable'.format(typed_udf))
            return df

    def get_typed_udf(self, col_type, distribution):
        if col_type == 'string':
            return self.string_udf(distribution)
        if col_type == 'int':
            return self.int_udf(distribution)
        if col_type == 'double':
            return self.double_udf(distribution)
        if col_type == 'bigint':
            return self.bigint_udf(distribution)
        if col_type == 'date':
            return self.date_udf(distribution)
        if col_type == 'timestamp':
            return self.timestamp_udf(distribution)
        raise ValueError('Value type `{}` not supported'.format(col_type))

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
    
    def __str__(self):
        return self.description()

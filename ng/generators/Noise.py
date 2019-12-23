#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from enum import Enum
from .AbbreviationGen import AbbreviationGen
from .AcronymGen import AcronymGen
from .BaseChangeGen import BaseChangeGen
from .EditGen import EditGen
from .MissingInfoGen import MissingInfoGen
from .MultilingualGen import MultilingualGen
from .NegationGen import NegationGen
from .NullGen import NullGen
from .PermutationGen import PermutationGen
from .ReplaceGen import ReplaceGen
from .ScaleGen import ScaleGen
from .ShiftGen import ShiftGen
from .ShufflingGen import ShufflingGen
from .SynonymGen import SynonymGen


class Noise(Enum):
    ABBREVIATION = 0
    ACRONYM = 1
    BASE_CHANGE = 2
    EDIT = 3
    MISSING_INFO = 4
    MULTILINGUAL = 5
    NEGATION = 6
    NULL = 7
    PERMUTATION = 8
    REPLACE = 9
    SCALE = 10
    SHIFT = 11
    SHUFFLING = 12
    SYNONYM = 13

    @staticmethod
    def determine_generator(noise, df, columns, noise_params):
        noise_value = noise.upper()
        if not Noise[noise_value] or Noise[noise_value] is None:
            raise IndexError('The given value {} is not supported.Try one of: {}'.format(
                noise, [(elem.value, elem.name) for elem in Noise]))
                
        if Noise[noise_value] is Noise.ABBREVIATION:
            return AbbreviationGen(df, columns)
        if Noise[noise_value] is Noise.ACRONYM:
            if noise_params is None:
                return AcronymGen(df, columns)
            return AcronymGen(df, columns, noise_params)
        if Noise[noise_value] is Noise.BASE_CHANGE:
            if noise_params is None:
                return BaseChangeGen(df, columns)
            return BaseChangeGen(df, columns, noise_params)
        if Noise[noise_value] is Noise.EDIT:
            if noise_params is None:
                return EditGen(df, columns)
            return EditGen(df, columns, noise_params)
        if Noise[noise_value] is Noise.MISSING_INFO:
            return MissingInfoGen(df)
        if Noise[noise_value] is Noise.MULTILINGUAL:
            return MultilingualGen(df, columns)
        if Noise[noise_value] is Noise.NEGATION:
            return NegationGen(df, columns)
        if Noise[noise_value] is Noise.NULL:
            return NullGen(df, columns)
        if Noise[noise_value] is Noise.PERMUTATION:
            if noise_params is None:
                return PermutationGen(df, columns)
            return PermutationGen(df, columns, noise_params)
        if Noise[noise_value] is Noise.REPLACE:
            if noise_params is None:
                return ReplaceGen(df, columns)
            return ReplaceGen(df, columns, noise_params)
        if Noise[noise_value] is Noise.SCALE:
            if noise_params is None:
                return ScaleGen(df, columns)
            return ScaleGen(df, columns, noise_params)
        if Noise[noise_value] is Noise.SHIFT:
            if noise_params is None:
                return ShiftGen(df, columns)
            return ShiftGen(df, columns, noise_params)
        if Noise[noise_value] is Noise.SHUFFLING:
            if noise_params is None:
                return ShufflingGen(df, columns)
            return ShufflingGen(df, columns, noise_params)
        if Noise[noise_value] is Noise.SYNONYM:
            return SynonymGen(df, columns)
        
        raise IndexError('The given value {} is not supported. Try one of: {}'.format(
                    noise, [(elem.value, elem.name) for elem in Noise]))

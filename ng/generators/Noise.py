#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from enum import Enum
from .AbbreviationGen import AbbreviationGen
from .AcronymGen import AcronymGen
from .BaseChangeGen import BaseChangeGen
from .EditGen import EditGen
from .MissingInfoGen import MissingInfoGen
from .NegationGen import NegationGen
from .NullGen import NullGen
from .PermutationGen import PermutationGen
from .ReplaceGen import ReplaceGen
from .ScaleGen import ScaleGen
from .ShiftGen import ShiftGen
from .ShufflingGen import ShufflingGen


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
    def determine_generator(given_noise, df, columns, distribution,
                            noise_params):
        try:
            noise = given_noise.upper()
            if Noise[noise] is Noise.ABBREVIATION:
                return AbbreviationGen(df, columns, distribution)
            if Noise[noise] is Noise.ACRONYM:
                if not noise_params:
                    return AcronymGen(df, columns, distribution)
                return AcronymGen(df, columns, distribution, noise_params)
            if Noise[noise] is Noise.BASE_CHANGE:
                if not noise_params:
                    return BaseChangeGen(df, columns, distribution)
                return BaseChangeGen(df, columns, distribution, noise_params)
            if Noise[noise] is Noise.EDIT:
                if not noise_params:
                    return EditGen(df, columns, distribution)
                return EditGen(df, columns, distribution, noise_params)
            if Noise[noise] is Noise.MISSING_INFO:
                return MissingInfoGen(df, distribution)
            if Noise[noise] is Noise.MULTILINGUAL:
                pass
            if Noise[noise] is Noise.NEGATION:
                return NegationGen(df, columns, distribution)
            if Noise[noise] is Noise.NULL:
                return NullGen(df, columns, distribution)
            if Noise[noise] is Noise.PERMUTATION:
                if not noise_params:
                    return PermutationGen(df, columns, distribution)
                return PermutationGen(df, columns, distribution, noise_params)
            if Noise[noise] is Noise.REPLACE:
                if not noise_params:
                    return ReplaceGen(df, columns, distribution)
                return ReplaceGen(df, columns, distribution, noise_params)
            if Noise[noise] is Noise.SCALE:
                if not noise_params:
                    return ScaleGen(df, columns, distribution)
                return ScaleGen(df, columns, distribution, noise_params)
            if Noise[noise] is Noise.SHIFT:
                if not noise_params:
                    return ShiftGen(df, columns, distribution)
                return ShiftGen(df, columns, distribution, noise_params)
            if Noise[noise] is Noise.SHUFFLING:
                if not noise_params:
                    return ShufflingGen(df, columns, distribution)
                return ShufflingGen(df, columns, distribution, noise_params)
            if Noise[noise] is Noise.SYNONYM:
                pass
        except Exception:
            raise IndexError(
                f'The given value {given_noise} is not a noise supported')

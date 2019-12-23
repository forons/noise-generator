#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
from enum import Enum
from .NormalDist import NormalDist
from .UniformDist import UniformDist


class Distribution(Enum):
    UNIFORM = 0
    GAUSSIAN = 1
    POISSON = 2

    @staticmethod
    def determine_distribution(distribution, distribution_params):
        distribution_upper = distribution.upper()
        if not Distribution[distribution_upper] or Distribution[distribution_upper] is None:
            raise IndexError('Distribution not supported `{}`. Try one of: {}'.format(
                distribution, [(elem.value, elem.name) for elem in Distribution]))
        if Distribution[distribution_upper] == Distribution.UNIFORM:
            if not distribution_params:
                distribution_params = 0.5
            return UniformDist(rate=float(distribution_params))
        if Distribution[distribution_upper] == Distribution.GAUSSIAN:
            if not distribution_params:
                distribution_params = [0., 1.]
            return NormalDist(loc=float(distribution_params[0]),
                              scale=float(distribution_params[1]))
        if Distribution[distribution_upper] is Distribution.POISSON:
            pass
        raise IndexError('Distribution not supported `{}`. Try one of: {}'.format(
                distribution, [(elem.value, elem.name) for elem in Distribution]))

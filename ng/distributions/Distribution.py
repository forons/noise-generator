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
    def determine_distribution(given_distribution, distribution_params):
        try:
            distribution = given_distribution.upper()
            if Distribution[distribution] is Distribution.UNIFORM:
                if not distribution_params:
                    distribution_params = 0.5
                return UniformDist(given_rate=float(distribution_params))
            if Distribution[distribution] is Distribution.GAUSSIAN:
                if not distribution_params:
                    distribution_params = [0., 1.]
                return NormalDist(given_loc=distribution_params[0], given_scale=distribution_params[1])
            if Distribution[distribution] is Distribution.POISSON:
                pass
        except Exception:
            msg = 'The given value {} is not a distribution supported'.format(given_distribution)
            logging.info(msg)
            raise IndexError(msg)

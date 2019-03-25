#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import random
from .AbstractDist import AbstractDist
from scipy.stats import norm

loc = 0.
scale = 1.


class NormalDist(AbstractDist):
    """
    This class is implements a normal noise distribution.
    """

    def __init__(self, given_loc=0., given_scale=1., is_negative=False, seed=123):
        super().__init__(seed)
        self.is_negative = is_negative
        global loc
        loc = given_loc
        global scale
        scale = given_scale

    @staticmethod
    def description(**kwargs):
        return 'Normal distribution, with loc={} and scale={}'.format(loc, scale)

    @staticmethod
    def name(**kwargs):
        return 'Normal distribution'

    def generate(self, elem):
        value = norm.pdf(elem, loc, scale)
        if self.is_negative:
            value = 1. - value
        return value >= random.uniform(0, 1)

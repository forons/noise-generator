#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import random
from .AbstractDist import AbstractDist
from scipy.stats import norm


class NormalDist(AbstractDist):
    """
    This class is implements a normal noise distribution.
    """

    def __init__(self, loc=0., scale=1., is_negative=False,
                 seed=123):
        super().__init__(seed)
        self.is_negative = is_negative
        self.loc = loc
        self.scale = scale

    def description(self, **kwargs):
        return 'Normal distribution, loc={}, scale={}'.format(self.loc, self.scale)

    @staticmethod
    def name(**kwargs):
        return 'Normal distribution'

    def generate(self, elem):
        value = norm.pdf(elem, self.loc, self.scale)
        if self.is_negative:
            value = 1. - value
        return value >= random.uniform(0, 1)

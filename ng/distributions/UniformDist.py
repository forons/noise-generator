#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import random
from .AbstractDist import AbstractDist

rate = 0.5


class UniformDist(AbstractDist):
    """
    This class is implements a normal noise distribution.
    """

    def __init__(self, given_rate, seed=123):
        super().__init__(seed)
        global rate
        rate = given_rate

    @staticmethod
    def description(**kwargs):
        return 'Uniform distribution, with rate={}'.format(rate)

    @staticmethod
    def name(**kwargs):
        return 'Uniform distribution'

    def generate(self, elem):
        return rate >= random.uniform(0, 1)

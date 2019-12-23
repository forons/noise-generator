#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import random
from .AbstractDist import AbstractDist


class UniformDist(AbstractDist):
    """
    This class is implements a normal noise distribution.
    """

    def __init__(self, rate, seed=123):
        super().__init__(seed)
        self.rate = rate

    def description(self, **kwargs):
        return 'Uniform distribution, with rate={}'.format(self.rate)

    @staticmethod
    def name(**kwargs):
        return 'Uniform distribution'

    def generate(self, elem):
        if self.rate == 0.0:
            return False
        elif self.rate == 1.0:
            return True
        return self.rate >= random.uniform(0, 1)

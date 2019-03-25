#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from abc import ABC, abstractmethod


class AbstractDist(ABC):
    """
    This class is an abstract class for the
    generation of a noise distribution.
    """

    def __init__(self, seed):
        if seed:
            self.seed = seed

    @staticmethod
    def description(**kwargs):
        pass

    @staticmethod
    def name(**kwargs):
        pass

    @abstractmethod
    def generate(self, elem):
        pass

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from .distributions.Distribution import Distribution
from .generators.Noise import Noise


def generate_noise(df, noise_name, distribution_name, columns=None, noise_params=None, distribution_params=None):
    if columns is None:
        columns = []
    distribution = Distribution.determine_distribution(distribution_name, distribution_params)
    generator = Noise.determine_generator(noise_name, df, columns, distribution, noise_params)
    result = generator.generate()
    return result

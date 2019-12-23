#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from noise.ng.distributions.Distribution import Distribution
from noise.ng.generators.Noise import Noise


def generate_noise(df, noise_name, distribution_name, columns=None,
                   noise_params=None, distribution_params=None):
    if columns is None:
        columns = []
    distribution = Distribution.determine_distribution(distribution_name,
                                                       distribution_params)
    generator = Noise.determine_generator(noise_name, df, columns, noise_params)
    print('noise: {}; distribution: {}'.format(generator, distribution))
    result = generator.generate(distribution)
    return result

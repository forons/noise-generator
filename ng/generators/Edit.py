#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from enum import Enum


class Edit(Enum):
    INSERTION = 0
    SUBSTITUTION = 1
    DELETION = 2

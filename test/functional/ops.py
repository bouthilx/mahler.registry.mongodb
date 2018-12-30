import random

import mahler.core


@mahler.core.operator.wrap()
def run(dummy=None):
    return dict(objective=random.random(), dummy=dummy), dict()

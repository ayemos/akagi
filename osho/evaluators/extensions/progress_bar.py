from tqdm import tqdm
from math import floor

from osho.evaluators.extension import Extension


class ProgressBar(Extension):
    def __init__(self):
        self._bar = tqdm(total=100)
        self._progress = 0

    def __call__(self, evaluator):
        progress = floor(max(1, evaluator.get_progress()) * 100)
        self._bar.update(progress - self._progress)
        self._progress = progress


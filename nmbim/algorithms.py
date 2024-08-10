#################################################################
# Algorithms for processing waveform data with the NMBIM model. #
#################################################################
import numpy as np
from numpy.typing import ArrayLike
from typing import Union
from scipy import ndimage

IntOrFloat = Union[int, float]

def remove_noise(wf: ArrayLike, mean_noise: float) -> ArrayLike:
    return wf - mean_noise
        
def smooth_waveform(wf: ArrayLike, float, sd: IntOrFloat) -> ArrayLike:
    return ndimage.gaussian.filter1d(wf, sd)

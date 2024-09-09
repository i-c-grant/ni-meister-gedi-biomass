#################################################################
# Algorithms for processing waveform data with the NMBIM model. #
#################################################################
import numpy as np
from numpy.typing import ArrayLike
from typing import Dict, Union
from scipy import ndimage

IntOrFloat = Union[int, float]

def calc_dz(ht: ArrayLike) -> float:
    """
    Calculate height increment (dz) between waveform returns.

    Parameters
    ----------
    ht : ArrayLike
        Height (m) of each waveform return relative to ground.

    Returns
    -------
    float
        Height increment (m) between waveform returns.
    """
    return ht[1] - ht[0]



def remove_noise(wf: ArrayLike, mean_noise: float) -> ArrayLike:
    # Remove mean noise from waveform
    return wf - mean_noise
        
def smooth_waveform(wf: ArrayLike, sd: IntOrFloat) -> ArrayLike:
    # Smooth waveform using Gaussian filter
    return ndimage.gaussian_filter1d(wf, sd)

def calc_height(wf: ArrayLike, elev_top, elev_bottom, elev_ground) -> ArrayLike:
    '''
    Calculate height of each waveform return relative to ground.

    Parameters
    ----------
    wf : ArrayLike
        Waveform returns.

    elev_top : float
        Elevation of the top of the waveform in meters.

    elev_bottom : float
        Elevation of the bottom of the waveform in meters.

    elev_ground : float
        Elevation of the ground in meters.

    Returns
    -------
    ArrayLike
        Height (m) relative to ground for each return, beginning at top
    '''
    elev_range = np.linspace(elev_top, elev_bottom, len(wf))
    return elev_range - elev_ground
    
    
    

def normalize_waveform(wf: ArrayLike) -> ArrayLike:
    # Normalize waveform by dividing by total waveform sum
    return np.divide(wf, np.nansum(wf))

def calc_wf_per_height(wf: ArrayLike, dz: float) -> ArrayLike:
    # Calculate waveform returns per unit height (m)
    dp_dz = wf / dz
    # Set negative return values to zero
    dp_dz[dp_dz < 0] = 0
    return dp_dz

def separate_veg_ground(wf: ArrayLike,
                        ht: ArrayLike,
                        rh: ArrayLike,
                        veg_floor: IntOrFloat) -> Dict:
    """
    Calculate indices of waveform returns corresponding to ground and vegetation. Assumes a symmetric ground return. Note: indices start from the top of the waveform.

    Parameters
    ----------
    wf : ArrayLike
        Waveform returns.
    ht : ArrayLike
        Height of each wf return relative to ground.
    rh : ArrayLike
        Relative height values for waveform.
    veg_floor : IntOrFloat, optional
        Minimum height above ground of bottom-most (i.e. last) veg. return,
        by default 5 m.

    Returns
    -------
    Dict
        Dictionary with keys "ground_top", "ground_bottom", "veg_top", and "veg_bottom" and values giving the correspoding indices within the waveform. 
"""
    
    # Get index of first vegetation return (top of canopy)
    veg_first_idx = np.argmax(ht <= rh[100])

    # Get index of ground return
    min_height = np.min(np.absolute(ht))
    ground_idx = np.where(np.absolute(ht) == min_height)[0][0]

    # Calculate waveform's noise level from part of waveform above vegetation
    wf_above_veg = wf[veg_first_idx:]
    noise = np.std(wf_above_veg) * 2

    # Find indices below first ground return that are below noise level
    # (indexes are relative to the first ground return)
    below_ground_noise_idxs = np.where(wf[:ground_idx] < noise)[0]

    # Check if any below-noise returns were found below first ground return
    if (len(below_ground_noise_idxs) == 0):
        # If not, redefine noise level using waveform until ground return
        wf_above_ground = wf[:ground_idx - 1]
        noise = np.std(wf_above_ground) * 2
        # Find below-noise indices with new noise level
        below_ground_noise_idxs = np.where(wf[:ground_idx] < noise)[0]
        # If still no below-noise returns, raise exception
        if (len(below_ground_noise_idxs) == 0):
            raise ValueError(f"No returns below noise level {noise} found below ground return")

    min_ground_noise_idx = np.min(below_ground_noise_idxs)

    if ground_idx + min_ground_noise_idx < len(wf):
        last_ground_idx = ground_idx + min_ground_noise_idx
    else:
        last_ground_idx = len(wf) - 1

    # Calculate first ground index, assuming symmetric ground return
    last_ground_height = ht[last_ground_idx]
    first_ground_height = -last_ground_height
    first_ground_idx = np.argmax(ht <= first_ground_height)

    # Calculate last vegetation return index
    last_veg_height = np.min([veg_floor, -ht[last_ground_idx]])
    veg_last_idx = np.max(np.where(ht >= last_veg_height))

    ans = {
        "ground_top": first_ground_idx,
        "ground_bottom": last_ground_idx,
        "veg_top": veg_first_idx,
        "veg_bottom": veg_last_idx,
    }

    return ans

def calc_gap_prob(wf_per_height: np.ndarray,
                  veg_first_idx: int,
                  veg_last_idx: int,
                  ground_last_idx: int) -> dict:
    """
    Calculate gap probability, vegetation cover, and related metrics for a single waveform.

    Parameters
    ----------
    wf_per_height : np.ndarray
        Return energy per unit height (waveform / dz).
    veg_first_idx : int
        Index of the first vegetation return.
    veg_last_idx : int
        Index of the last vegetation return.
    ground_last_idx : int
        Index of the last ground return.

    Returns
    -------
    dict
        Dictionary with calculated values for gap probability, vegetation cover, and related metrics.
    """

    # Calculate vegetation and ground return sums
    veg_sum = np.nansum(wf_per_height[veg_first_idx: veg_last_idx])
    # TODO: could instead go from first to last ground return?
    ground_sum = np.nansum(wf_per_height[veg_last_idx + 1: ground_last_idx])

    # Calculate vegetation cover assuming veg. to ground ratio = 1
    veg_cover = veg_sum / (veg_sum + ground_sum)

    # Calculate cumulative vegetation return
    veg_cuml = np.nancumsum(wf_per_height[veg_first_idx: veg_last_idx])

    # Calculate gap probability
    p_gap = 1 - (veg_cuml / (veg_sum + ground_sum))

    # Pad gap probability with NaNs to match length of wf_per_height
    p_gap = np.pad(p_gap,
                   (veg_first_idx, len(wf_per_height) - veg_last_idx),
                   constant_values=np.nan)

    # Foliage accumulation and density
    foliage_accum = -np.log(p_gap) / 0.5
    foliage_dens = wf_per_height * (1 / p_gap) / 0.5

    return {
        "veg_cover": veg_cover,
        "gap_prob": p_gap,
        "foliage_density": foliage_dens,
        "foliage_accum": foliage_accum
    }

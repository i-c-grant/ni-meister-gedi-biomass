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
    return ht[0] - ht[1]

def remove_noise(wf: ArrayLike, mean_noise: float) -> ArrayLike:
    # Remove mean noise from waveform,
    # with floor of zero
    return np.maximum(wf - mean_noise, 0)

def create_ground_return(wf: ArrayLike,
                         ht: ArrayLike,
                         ground_bottom: ArrayLike) -> ArrayLike:

    ground_index = np.where(np.abs(ht) == np.min(np.abs(ht)))
    ground_peak = wf[ground_index]

    # Initialize new array of same length as wf
    ground_wf = np.zeros_like(wf)

    # Define standard deviation for Gaussian
    sigma = ground_bottom / 2

    # Assign values as a Gaussian centered at the ground return
    for i in range(len(ground_wf)):
        ground_wf[i] = np.exp(-(ht[i] ** 2) / (2 * sigma ** 2))
    ground_wf = np.round(ground_wf, 2)

    # Scale to the peak of the ground return
    ground_wf *= ground_peak

    return ground_wf


def smooth_waveform(wf: ArrayLike, sd: IntOrFloat) -> ArrayLike:
    # Smooth waveform using Gaussian filter
    return ndimage.gaussian_filter1d(wf, sd)

def truncate_waveform(floor: IntOrFloat,
                      ceiling: IntOrFloat,
                      wf: ArrayLike,
                      ht: ArrayLike) -> ArrayLike:

    """
    Truncate waveform to specified height range in m, inclusive. Heights are relative to the ground return.
    
    Parameters
    ----------

    floor : IntOrFloat
        Minimum height of waveform in meters.

    ceiling : IntOrFloat
        Maximum height of waveform in meters.

    wf : ArrayLike
        Waveform returns.

    ht : ArrayLike
        Height of each waveform return relative to ground.

    Returns
    -------
    truncated_wf : ArrayLike
        Truncated waveform returns.
"""
    trunc_wf = wf.copy()
    mask = (ht >= floor) & (ht <= ceiling)
    trunc_wf[~mask] = np.nan
    return trunc_wf


def calc_biomass_index_simple(dp_dz: ArrayLike,
                              dz: float,
                              ht: ArrayLike,
                              ceiling: float,
                              floor: float,
                              hse: float) -> float:
    """
    Calculate a simple biomass index for a waveform. Sum of waveform returns raised to the HSE, with below-ground returns set to negative absolute value to cancel out the above-ground component of the ground return.

    Parameters
    ----------
    dp_dz: ArrayLike
        Change in gap probability per unit height. Assumed to be >= 0.

    dz: float
        Height increment (m) between waveform returns.

    ht: ArrayLike
        Height of each waveform return relative to ground.

    hse: float
        Height scaling exponent.

    Returns
    -------
    biomass_index: float
        Biomass index for the waveform.
    """
    mask = (ht >= floor) & (ht <= ceiling)
    biomass_index = (
        np.nansum(dp_dz[mask] *
                  np.abs(ht[mask]) ** hse *
                  np.sign(ht[mask]))
    )
    biomass_index *= dz
    return biomass_index

def calc_height(wf: ArrayLike,
                elev_top: float,
                elev_bottom: float,
                elev_ground: float) -> ArrayLike:
    """
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
    """
    elev_range = np.linspace(start=elev_top,
                             stop=elev_bottom,
                             num=len(wf))

    return elev_range - elev_ground


def normalize_waveform(wf: ArrayLike) -> ArrayLike:
    # Normalize waveform by dividing by total waveform sum
    return np.divide(wf, np.nansum(wf))


def calc_dp_dz(wf: ArrayLike, dz: float) -> ArrayLike:
    """
    Calculate change in gap probability per unit height from waveform returns.

    Parameters
    ----------
    wf : ArrayLike
        Waveform returns.

    dz : float
        Height increment for calculating dp/dz.

    Returns
    -------
    ArrayLike
        Change in gap probability per unit height.
    """
    
    # Calculate waveform returns per unit height (m)
    dp_dz = wf / dz
    # Set negative return values to zero
    dp_dz[dp_dz < 0] = 0
    return dp_dz


def separate_veg_ground(
        wf: ArrayLike,
        ht: ArrayLike,
        rh: ArrayLike,
        veg_floor: float,
        veg_buffer: float = 0,
) -> Dict:
    """
    Calculate indices of waveform returns corresponding to ground and vegetation.
    Assumes a symmetric ground return. Note: indices start from the top of the waveform.

    Parameters
    ----------
    wf : ArrayLike
        Waveform returns.
    ht : ArrayLike
        Height of each wf return relative to ground.
    rh : ArrayLike
        Relative height values for waveform.
    veg_floor : IntOrFloat, optional
        Minimum height above ground of bottom-most (i.e. last) veg. return.

    Returns
    -------
    Dict
        Dictionary with keys "ground_top", "ground_bottom", "veg_top", and "veg_bottom"
    and values giving the corresponding indices within the waveform.
    """

    # Get index of first vegetation return (top of canopy)
    veg_first_idx = np.argmax(ht <= rh[100])

    # Get index of ground return
    min_height = np.min(np.absolute(ht))
    ground_idx = np.where(np.absolute(ht) == min_height)[0][0]

    # Calculate waveform's noise level from part of waveform above vegetation
    wf_above_veg = wf[0:veg_first_idx]
    noise = np.std(wf_above_veg) * 2

    # Find indices below first ground return that are below noise level
    # (indexes are relative to the first ground return)
    below_ground_noise_idxs = np.where(wf[ground_idx:] < noise)[0]

    # Check whether below-noise returns were found below first ground return
    if len(below_ground_noise_idxs) == 0:
        # If not, redefine noise level using full above-ground waveform
        wf_above_ground = wf[:ground_idx - 1]
        noise = np.std(wf_above_ground) * 2
        # Find below-noise indices with new noise level
        below_ground_noise_idxs = np.where(wf[ground_idx:] < noise)[0]
        # If still no below-noise returns, raise exception
        if len(below_ground_noise_idxs) == 0:
            raise ValueError(
                f"No returns below noise level {noise} found below ground return"
            )

    # Ground return index +/- ground offset gives the indices
    # in the ground return region
    ground_offset = np.min(below_ground_noise_idxs)
    last_ground_idx = min(ground_idx + ground_offset, len(wf) - 1)
    first_ground_idx = max(ground_idx - ground_offset, 0)

    # Calculate last vegetation return index
    last_veg_height = min([veg_floor, -ht[last_ground_idx]])
    veg_last_idx = np.max(np.where(ht >= last_veg_height))

    # Add buffer to vegetation height to account for continuous dropoff
    # in canopy between highest detected return and first empty bin
    dz = ht[1] - ht[0]
    veg_buffer = dz * .5

    ans = {
        "ground_top": ht[first_ground_idx],
        "ground_bottom": ht[last_ground_idx],
        "veg_top": rh[100] + veg_buffer,
        "veg_bottom": ht[veg_last_idx],
    }

    return ans


def calc_gap_prob(
    wf_per_height: np.ndarray,
    veg_first_idx: int,
    veg_last_idx: int,
    ground_last_idx: int,
) -> dict:
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
    veg_sum = np.nansum(wf_per_height[veg_first_idx:veg_last_idx])
    # TODO: could instead go from first to last ground return?
    ground_sum = np.nansum(wf_per_height[veg_last_idx + 1 : ground_last_idx])

    # Calculate vegetation cover assuming veg. to ground ratio = 1
    veg_cover = veg_sum / (veg_sum + ground_sum)

    # Calculate cumulative vegetation return
    veg_cuml = np.nancumsum(wf_per_height[veg_first_idx:veg_last_idx])

    # Calculate gap probability
    p_gap = 1 - (veg_cuml / (veg_sum + ground_sum))

    # Pad gap probability with NaNs to match length of wf_per_height
    p_gap = np.pad(
        p_gap,
        (veg_first_idx, len(wf_per_height) - veg_last_idx),
        constant_values=np.nan,
    )

    # Foliage accumulation and density
    foliage_accum = -np.log(p_gap) / 0.5
    foliage_dens = wf_per_height * (1 / p_gap) / 0.5

    return {
        "veg_cover": veg_cover,
        "gap_prob": p_gap,
        "foliage_density": foliage_dens,
        "foliage_accum": foliage_accum,
    }

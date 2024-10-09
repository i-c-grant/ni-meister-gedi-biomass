#################################################################
# Algorithms for processing waveform data with the NMBIM model. #
#################################################################
import warnings
from typing import Dict, Union

import numpy as np
from numpy.typing import ArrayLike
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


def scale_raw_wf(
    wf_raw: ArrayLike, wf_smooth: ArrayLike, dz: float
) -> ArrayLike:
    """
    Scale raw waveform returns to match smoothed waveform returns.

    Replicates the normalization and scaling used on the smoothed waveform
    so that the raw waveform can be compared to the smoothed waveform.
    """
    if (np.nansum(wf_smooth) * dz) == 0:
        warnings.warn("Smoothed waveform sum is zero, returning zero array.")
        scaled_raw = np.zeros_like(wf_raw)
    else:
        scaled_raw = wf_raw / (np.nansum(wf_smooth) * dz)
    
    return scaled_raw


def remove_noise(wf: ArrayLike, mean_noise: float) -> ArrayLike:
    # Remove mean noise from waveform,
    # with floor of zero
    return np.maximum(wf - mean_noise, 0)


def calc_noise(
    wf: ArrayLike,
    ht: ArrayLike,
    veg_top: float,
    ground_bottom: float,
    noise_ratio: float = 1,
) -> float:
    """
    Calculate the mean noise level of a waveform from the region above
    the canopy and below the ground return.

    Can be applied after removing the mean noise reported in the L1B
    file to calculate residual noise.

    Parameters
    ----------
    wf : ArrayLike
        Waveform returns.

    ht : ArrayLike
        Height of each waveform return relative to ground.

    veg_top : float
        Height of the top of the vegetation in meters.

    ground_bottom : float
        Height of the ground return in meters.

    noise_ratio : float, optional
        Factor by which to multiply the mean noise level. Defaults to 1.
    """
    # Get indices of waveform returns above canopy and below ground
    noise_idxs = np.where((ht > veg_top) | (ht < ground_bottom))
    noise = np.mean(wf[noise_idxs])
    return noise * noise_ratio


def create_ground_return(
    wf: ArrayLike,
    ht: ArrayLike,
    ground_return_max_height: float,
    sd_ratio: float,
) -> ArrayLike:
    """
    Create a synthetic ground return for a waveform using a Gaussian centered at the ground return.

    Parameters
    ----------
    wf : ArrayLike
        Waveform returns.

    ht : ArrayLike
        Height of each waveform return relative to ground.

    ground_return_max_height : ArrayLike
        Height at which the ground return is expected to fall below
        the noise level.

    sd_ratio : float
        Standard deviation of the Gaussian as a ratio of the ground
        return height.
    """

    ground_index = np.where(np.abs(ht) == np.min(np.abs(ht)))
    ground_peak = wf[ground_index]

    # Initialize new array of same length as wf
    ground_wf = np.zeros_like(wf)

    # Define standard deviation for Gaussian
    sigma = ground_return_max_height * sd_ratio

    # Assign values as a Gaussian centered at the ground return
    for i in range(len(ground_wf)):
        ground_wf[i] = np.exp(-(ht[i] ** 2) / (2 * sigma**2))
    ground_wf = np.round(ground_wf, 2)

    # Scale to the peak of the ground return
    ground_wf *= ground_peak

    return ground_wf


def smooth_waveform(wf: ArrayLike, sd: IntOrFloat) -> ArrayLike:
    # Smooth waveform using Gaussian filter
    return ndimage.gaussian_filter1d(wf, sd)


def truncate_waveform(
    floor: IntOrFloat, ceiling: IntOrFloat, wf: ArrayLike, ht: ArrayLike
) -> ArrayLike:
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


def calc_biomass_index(
    dp_dz: ArrayLike, dz: float, ht: ArrayLike, hse: float, n_modes: int
) -> float:
    """
    Calculate a simple biomass index for a waveform. Sum of height raised to the HSE weighted by waveform returns.
    """

    # A single-mode waveform means no vegetation is present
    if n_modes == 1:
        return 0

    biomass_index = np.nansum(dp_dz * np.abs(ht) ** hse)
    biomass_index *= dz
    return biomass_index


def calc_height(
    wf: ArrayLike, elev_top: float, elev_bottom: float, elev_ground: float
) -> ArrayLike:
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
    elev_range = np.linspace(start=elev_top, stop=elev_bottom, num=len(wf))

    return elev_range - elev_ground

def normalize_waveform(wf: ArrayLike) -> ArrayLike:
    # Normalize waveform by dividing by total waveform sum
    if np.nansum(wf) == 0:
        warnings.warn("Waveform sum is zero, returning zero array.")
        normalized = np.zeros_like(wf)
    else:
        normalized = np.divide(wf, np.nansum(wf))
    return normalized


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
    dz: float,
    rh: ArrayLike,
    min_veg_bottom: float,
    max_veg_bottom: float,
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
    min_veg_bottom : IntOrFloat, optional
        Minimum height above ground of bottom-most (i.e. last) veg. return.
    veg_buffer : float, optional
        Buffer to add to the top of vegetation.

    Returns
    -------
    Dict
        Dictionary with keys "ground_top", "ground_bottom", "veg_top", and "veg_bottom"
    and values giving the corresponding heights within the waveform.
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
        wf_above_ground = wf[: ground_idx - 1]
        noise = np.std(wf_above_ground) * 2
        # Find below-noise indices with new noise level
        below_ground_noise_idxs = np.where(wf[ground_idx:] < noise)[0]
        # If still no below-noise returns, default to 5 m below ground
        if len(below_ground_noise_idxs) == 0:
            warnings.warn(
                f"No returns below noise level {noise} "
                f"found below ground return"
            )
            below_ground_noise_idxs = (
                np.where((ht[ground_idx:] > -5) & (ht[ground_idx:] < 0))
            )[0]

    # Ground return index +/- ground offset gives the indices
    # in the ground return region
    ground_offset = np.min(below_ground_noise_idxs)
    last_ground_idx = min(ground_idx + ground_offset, len(wf) - 1)
    first_ground_idx = max(ground_idx - ground_offset, 0)

    # Calculate last vegetation return index
    last_veg_height = -ht[last_ground_idx]
    if last_veg_height < min_veg_bottom:
        last_veg_height = min_veg_bottom
    elif last_veg_height > max_veg_bottom:
        last_veg_height = max_veg_bottom
    
    veg_indices = np.where(ht >= last_veg_height)[0]
    if len(veg_indices) > 0:
        veg_last_idx = min(np.max(veg_indices), len(wf) - 1)
    else:
        warnings.warn(
            f"No returns found above last vegetation height {last_veg_height},"
            f"using lowest positive height return as vegetation bottom."
        )
        positive_indices = np.where(ht > 0)[0]
        if len(positive_indices) > 0:
            veg_last_idx = np.min(positive_indices)
        else:
            warnings.warn(
                f"No positive height returns found. Using ground index as vegetation bottom."
            )
            veg_last_idx = ground_idx

    ans = {
        "ground_top": ht[first_ground_idx],
        "ground_bottom": ht[last_ground_idx],
        "veg_top": rh[100] + veg_buffer,
        "veg_bottom": ht[veg_last_idx],
    }

    return ans


def isolate_vegetation(
    wf: ArrayLike, ht: ArrayLike, veg_top: float, ground_return: ArrayLike
) -> ArrayLike:
    """
    Isolate vegetation returns from a waveform by subtracting the ground return with a floor of zero and setting below-ground and above-canopy returns to zero.
    """

    # Subtract ground return from waveform
    wf_no_ground = np.maximum(wf - ground_return, 0)

    # Set below-ground and above-canopy returns to zero
    mask = (ht < 0) | (ht > veg_top)

    wf_no_ground[mask] = 0

    return wf_no_ground


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

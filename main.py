#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 24 10:20:58 2022

@author: arojas

"""

import pandas as pd
import numpy as np
import os
import geopandas as gpd
import h5py
import re
import glob
import sys
from itertools import repeat
# Import pgap function from wenge 
from pgap import GapDS, wf_smooth
# import custom functions, etc.
from download_gedi import download_gedi
from get_gedi_data import get_gedi_data

## GET CWD of file to locate path
CWD = os.path.dirname(os.path.abspath(__file__))

## Function to return beam dataframe with some meta data
def get_beam_gdf(beam,l1b_ds,l2a_ds):
    # Create gdf to filter for GEDI returns
    shot_number = l1b_ds[beam]["shot_number"][:]
    lat = l1b_ds[beam]['geolocation']['latitude_bin0'][:]
    lon = l1b_ds[beam]['geolocation']['longitude_bin0'][:]
    degrade = l1b_ds[beam]['geolocation']['degrade'][:]
    sf = l1b_ds[beam]['stale_return_flag'][:]
    l2_qf = l2a_ds[beam]['quality_flag'][:]
    modis_nonveg = l2a_ds[beam]['land_cover_data']["modis_nonvegetated"][:]

    # create geodataframe to filter for each domain
    beam_gdf = gpd.GeoDataFrame({"shot_number":shot_number, "lat":lat, "lon":lon,
                                 "sf":sf, "degrade":degrade, "qf":l2_qf, "modis_nonveg":modis_nonveg},
                                geometry=gpd.points_from_xy(lon,lat),
                                crs="EPSG:4326")
    return beam_gdf


def gedi_bioindex(index,l1b_ds,l2a_ds, beam, beam_filt, allom_df):
    wfCount = l1b_ds[beam]['rx_sample_count'][index] # Number of samples in the waveform
    wfStart = int(l1b_ds[beam]['rx_sample_start_index'][index] - 1)  # Subtract one because python array indexing starts at 0 not 1
    noise_mean_corrected = l1b_ds[beam]['noise_mean_corrected'][index]
    # Grab the elevation recorded at the start and end of the full waveform capture
    zStart = l1b_ds[f'{beam}/geolocation/elevation_bin0'][index] # Height of the start of the rx window
    zEnd = l1b_ds[f'{beam}/geolocation/elevation_lastbin'][index]
    # level 2 metrics
    zGround = l2a_ds[beam]['elev_lowestmode'][index] # ground
    zTop = l2a_ds[beam]['elev_highestreturn'][index] # top of canopy (rh100)
    rh100 = l2a_ds[beam]['rh'][index][-2]

    ## Calculate bioindex from waveform
    elev_arr = np.linspace(zStart, zEnd, wfCount)
    # convert elev to ht by subtracting ground
    ht_arr = elev_arr - zGround
    
    # Retrieve the waveform sds layer using the sample start index and sample count information to slice the correct dimensions
    waveform = l1b_ds[beam]['rxwaveform'][wfStart: wfStart + wfCount]
    waveform_noisermvd = waveform - noise_mean_corrected
    # smooth waveform
    waveform_smooth = wf_smooth(waveform_noisermvd)
    
    # get bioindex using HSE from NEON db analysis
    beam_domain = beam_filt.loc[index]['DomainID']
    try:
        if pd.isnull(beam_domain):
            cval = 1.63
        else:
            cval =  allom_df[allom_df['domain']==beam_domain]['HSE'].values[0]
        # get pgap
        pgap = GapDS(waveform_smooth, ht_arr, np.array([rh100]), calc_refl_vg = False,
                        utm_x=None,utm_y=None,cval=cval)
    except:
        # bad data (fix for future)
        return (np.nan,np.nan,np.nan,np.nan,np.nan,np.nan)
    # return a tuple of biwf and bfp
    return (pgap.biWF[0], pgap.biFP[0], np.nanmin(pgap.gap), np.nanmax(pgap.lai), rh100, cval)



##########
## Process entire CONUS data
##########
if __name__ == '__main__':
    
    # File handling
    l1b_url = sys.argv[1] # first index is python file name, second is arg1, etc
    l2a_url = sys.argv[2] # e.g. 'GEDI01_B' or 'GEDI02_A'
    outdir = sys.argv[3]
    #Download L1B and L2a
    # download_gedi(l1b_url,"GEDI01_B")
    # download_gedi(l2a_url,"GEDI02_A")
    # Get filenames for downloaded gedi
    l1b_basename = os.path.basename(l1b_url)
    l2a_basename = os.path.basename(l2a_url)
    
    # Read in domain polys and allom data
    domain_poly_fp = os.path.join(CWD, "NEON_Domains/NEON_Domains.shp")
    domain_polys = gpd.read_file(domain_poly_fp)
    allom_fp = os.path.join(CWD, "NEON-DOMAINS-HSE-2020to2023-edited.csv")
    allom_df = pd.read_csv(allom_fp)
    
    
    try:
        # get level 1 and level 2 data
        print(l1b_basename)
        print(l2a_basename)
        CWD = os.path.dirname(os.path.abspath(__file__))
        l1b_ds = get_gedi_data(l1b_url)
        l2a_ds = get_gedi_data(l2a_url)
        
        # l1b_ds = h5py.File(os.path.join(CWD, f"{l1b_basename}"))
        # l2a_ds = h5py.File(os.path.join(CWD, f"{l2a_basename}"))
    except Exception as e:
        # Some raw L1B files are corrupt?
        print("Corrupt file: ", l1b_basename)
        sys.exit()
        
    # Init output file
    orbit_num = re.findall("O[0-9]{5}", l1b_basename)[0]
    track_num = re.findall("T[0-9]{5}", l1b_basename)[0]
    date_str = re.findall("[0-9]{13}", l1b_basename)[0]
    # outfp = os.path.join(outdir, "output",f"GEDI_bioindex_{date_str}_{orbit_num}_{track_num}.csv") # already saves to output
    outfp = os.path.join(outdir, f"GEDI_bioindex_{date_str}_{orbit_num}_{track_num}.csv")
    
    
    # init df_list to concat afterwards
    df_list = []
    # Loop through each beam
    beamNames = [g for g in l1b_ds.keys() if g.startswith('BEAM')]
    for beam in beamNames:

        # get the beam gdf
        try:
            beam_gdf = get_beam_gdf(beam,l1b_ds,l2a_ds)
        except:
            print(f"File {l1b_basename} has an issue at {beam}")
            continue
        beam_gdf = beam_gdf.loc[beam_gdf['qf']==1] # quality flag filter

        # Spatially filter beam_gdf (fastest approach)
        beam_filt = gpd.sjoin(beam_gdf, domain_polys[["DomainID", 'geometry']], op='intersects', how="inner")
        if len(beam_filt)==0:
            continue

        # get shotnumber indices
        idx = beam_filt.index.to_list()
        if not isinstance(idx, list):
            idx = [idx]

        # run function in parallel
        # pool = mp.Pool(10)
        # results = pool.map(gedi_bioindex, idx)
        # results = pool.starmap(gedi_bioindex, zip(idx, repeat(l1b_ds), repeat(l2a_ds)))

        # biWF_list = [bi[0] for bi in results]
        # biFP_list = [bi[1] for bi in results]
        # gap_list = [bi[2] for bi in results]
        # lai_list = [bi[3] for bi in results]
        # rh_list = [bi[4] for bi in results]


        biWF_list = []
        biFP_list = []
        gap_list = []
        lai_list = []
        rh_list = []
        cval_list = []
        print("Running pgap iteratively...")
        for i in idx:
            results = gedi_bioindex(i,l1b_ds,l2a_ds,beam,beam_filt, allom_df)
            # save to lists
            biWF_list.append(results[0])
            biFP_list.append(results[1])
            gap_list.append(results[2])
            lai_list.append(results[3])
            rh_list.append(results[4])
            cval_list.append(results[5])
        print("Done!")

        # Create out_df to output as csv
        new_df = beam_filt[['shot_number', "lat", "lon", 'qf',"DomainID"]].copy().reset_index(drop=True)
        ## Add new variables
        new_df['beam'] = beam
        new_df['biwf'] = np.round(biWF_list,3)
        new_df['bifp'] = np.round(biFP_list,3)
        new_df['gap'] = np.round(gap_list,3)
        new_df['lai'] = np.round(lai_list,3)
        new_df['rh'] = np.round(rh_list,3)
        new_df['cval'] = np.round(cval_list,3)
        
        # append to df_list
        df_list.append(new_df)

    # # Save
    # try:
    #     out_df = pd.concat(df_list, axis=0, ignore_index=True)
    #     out_df.to_csv(outfp, index=False)  
    # except Exception as e:
    #     print("Couldnt save file: ", l1b_basename)
    #     print("outdir: ", outdir)
    #     print("outfp: ", outfp)
    #     print(e)
    #     sys.exit()
        # Save
    out_df = pd.concat(df_list, axis=0, ignore_index=True)
    out_df.to_csv(outfp, index=False)  

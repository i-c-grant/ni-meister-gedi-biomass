# Import libraries
import numpy as np
from scipy.spatial import KDTree
import warnings
warnings.filterwarnings("ignore")
from scipy import ndimage

# --------------------------DATA STRUCTURE FOR PGAP PROCESSED WAVEFORM-------------------------- #

class GapDS:
    

    """Calculates gap probability and bioindex using full waveform lidar data.

    Parameters
    ----------
    wf : ndarray
        Input directory with LVIS level 1 (.h5) and level 2 (.TXT) files to be joined.
    ht : ndarray
        Output directory to save the joined LVIS files. Default is None, which output to the same directory as the input files.
    rh : array-like
        1d array or list of RH values indicating canopy top.
    calc_refl_vg : bool
        Calculate reflectance of vegetation / reflectance of ground. Default is set to False and refl_v/refl_g attribute is set to 1.
    

    """


    def __init__(self, wf, ht, rh, calc_refl_vg = False,
        utm_x=None,utm_y=None,cval=2, **kwargs):

            
        """
        Constructor method
        """
    
        self.wf = np.copy(wf)
        if wf.ndim == 1:
            self.wf = np.expand_dims(wf, axis=0)
        self.ht = np.copy(ht)
        if wf.ndim == 1:
            self.wf = np.expand_dims(wf, axis=0)
        self.rh = rh
        self.c = cval
        self.refl_vg = np.array([1.0] * len(self.wf)) # init to 1
        self.dz = self.ht[0][0] - self.ht[0][1]
        
        if calc_refl_vg is True:
            self.utmx=utm_x
            self.utmy=utm_y
            self.refl_v = np.array([1.0] * len(self.wf))
            self.refl_g = np.array([1.0] * len(self.wf))
            
        self.calc_refl_vg = calc_refl_vg

        # Get the extra kwargs and save as attributes of same name
        # (e.g szn=[15,30,45,60])
        for attr in kwargs.keys():
            self.__dict__[attr] = kwargs[attr]

        # call the functin below to get gap, etc.
        self.lvisMainProcessFunc(c=cval)

    # Define properties for wf and ht data
    @property
    def wf(self):
        return self._wf
    @wf.setter
    def wf(self, value):
        if not isinstance(value, np.ndarray):
            raise TypeError('wf must be an ndarray, where each row is a seperate waveform.')
        if value.ndim == 1:
            value = np.expand_dims(value, axis=0)
        self._wf = value

    @property
    def ht(self):
        return self._ht
    @ht.setter
    def ht(self, value):
        if not isinstance(value, np.ndarray):
            raise TypeError('ht must be an ndarray of heights same shape as the waveforms.')
        if value.ndim == 1:
            value = np.expand_dims(value, axis=0)
        if value.shape != self.wf.shape:
            raise TypeError("wf and ht arrays are of different shapes.")
        self._ht = value

    @property
    def rh(self):
        return self._rh
    @rh.setter
    def rh(self, value):
        if (not isinstance(value, list)) and (not isinstance(value, np.ndarray)):
            raise TypeError('rh must be a list or an array of values that correspond to the relative' \
                        "heights for each waveform from which gap probability is calculated to the ground.")
        if len(value) != len(self.wf):
            raise TypeError("rh must be same length as the wf and ht arrays.")
        self._rh = value


    def lvisMainProcessFunc(self, c):
        
        ## First, seperate ground and vegetation return (use the function vegGroundSep())
        
        # Call function to seperate vegetation from ground
        self.vegGroundSep()
        
        if self.calc_refl_vg is True:
            # concat the utmx and utmy values
            utm_xy = np.concatenate([self.utmx.reshape(-1,1),self.utmy.reshape(-1,1)],axis=1)
            # calculate nearest neighbor using sklearn KDtree
            tree = KDTree(utm_xy)
            nearest_dist, nearest_ind = tree.query(utm_xy, k=2) 
            self.nn = nearest_ind[:, 1]     # drop id
        
        # call function to calculate gap and foliage
        self.gap_fol_calc()
        # Calculate biWF and biFP
        self.bioWF()
        self.bioFP()
        
    def vegGroundSep(self):
        # Get the first vegetation indices (RH100) for each wf
        in_bools = self.ht<=np.reshape(self.rh, (-1, 1))
        i_veg_first = np.argmax(in_bools,1)

        # Loop through the waveforms and seperate ground and vegetation
        # init lists to save data
        i_grd_list=[]
        i_grd_last_list=[]
        i_veg_last_list=[]
        waveNorm_list=[]
        dpdz_list=[]
        for i in range(len(self.wf)):
            # Save the wf and ht to variables
            wf = self.wf[i]
            ht = self.ht[i]
            # Get the ground idx
            grd = np.where(np.absolute(ht) == np.min(np.absolute(ht)))
            i_grd = grd[0][0]
            i_grd_list.append(i_grd)

            noise = 2 * np.std(wf[0:i_veg_first[i]])
            grnd_in = np.where(wf[i_grd:] < noise)

            try:
                i_grd_last = i_grd + np.min(grnd_in)
            except:
                noise = 2 * np.std(wf[0:i_grd-1])
                grnd_in = np.where(wf[i_grd:] < noise)
                i_grd_last = i_grd + np.min(grnd_in)
                
            i_grd_last_list.append(i_grd_last)
            # Last veg return index
            temp_veg = np.min([5, -ht[i_grd_last]])
            i_veg_last = np.max(np.where(ht >= temp_veg))            
            i_veg_last_list.append(i_veg_last)

            ## Update waveform and heights
            self.wf[i][0:i_veg_first[i]-1] = np.nan
            self.wf[i][i_grd_last+1:] = np.nan
            self.ht[i][0:i_veg_first[i]-1] = np.nan
            self.ht[i][i_grd_last+1:] = np.nan

            # Normalize waveform (wf/sum(wf))
            temp_wfnorm = np.divide(self.wf[i], np.nansum(self.wf[i]))
            waveNorm = temp_wfnorm
            waveNorm_list.append(waveNorm)
            # Add a variable dpdz
            dpdz = temp_wfnorm / self.dz
            # Correct for negative return values AR 4.12
            dpdz[dpdz<0]=0
            dpdz_list.append(dpdz)
            
            
        # Add data to class (as arrays for easier data manipulation)
        self.i_veg_first = i_veg_first
        self.i_grd = np.array(i_grd_list)
        self.i_grd_last = np.array(i_grd_last_list)
        self.i_veg_last = np.array(i_veg_last_list)
        self.waveNorm = np.array(waveNorm_list)
        self.dpdz = np.array(dpdz_list)
        
    
    def gap_fol_calc(self):
        
        ## Rho_vg calculation
        # cur_wf = self.dpdz
        cur_veg_first = self.i_veg_first
        cur_veg_last = self.i_veg_last
        cur_grd_last = self.i_grd_last
        
        ## Calculating sum of veg return and ground return
        cols = np.arange(self.dpdz.shape[1]).reshape(-1, 1)
        # Current
        tmask = (cur_veg_first <= cols) & (cols<cur_veg_last)
        R_cur_v = np.nansum(self.dpdz.T*tmask,axis=0)
        tmask = (cur_veg_last+1 <= cols) & (cols<cur_grd_last)
        R_cur_g = np.nansum(self.dpdz.T*tmask,axis=0)
        
        if self.calc_refl_vg is True:

            ## Calcualte Rv/Rg
            # Get the nn wf and veg and ground indices
            nearestn = self.nn
            neb_wf = self.dpdz[nearestn]
            nn_veg_first = self.i_veg_first[nearestn]
            nn_veg_last = self.i_veg_last[nearestn]
            nn_grd_last = self.i_grd_last[nearestn]
            # Neighbor calculate sum of veg and ground return
            tmask = (nn_veg_first <= cols) & (cols<nn_veg_last)
            R_neb_v = np.nansum(neb_wf.T*tmask,axis=0)
            tmask = (nn_veg_last+1 <= cols) & (cols<nn_grd_last)
            R_neb_g = np.nansum(neb_wf.T*tmask,axis=0)

            del neb_wf
            
            R_vg=(R_cur_v - R_neb_v)/(R_neb_g - R_cur_g)
            # Calculate R_v and R_g
            R_v = (R_neb_g*R_cur_v - R_cur_g*R_neb_v) / (R_neb_g - R_cur_g)
            R_g = (R_cur_g*R_neb_v - R_neb_g*R_cur_v) / (R_neb_v - R_cur_v)

            self.refl_v = R_v
            self.refl_g = R_g
            self.refl_vg = R_vg
            
        # Save Rv/Rg to variable
        R_vg=self.refl_vg

        # Calculate vegetation cover
        veg_cover = R_cur_v / (R_cur_v + (R_vg)*R_cur_g)
        veg_cover2 = R_cur_v / (R_cur_v + R_cur_g)

        # Vegetation return dz
        tmask = (cur_veg_first <= cols) & (cols<cur_veg_last)
        R_cur_vz = np.nancumsum(self.dpdz.T*tmask,axis=0)
        
        # Calculate gap probability
        pgap = 1-(R_cur_vz.T / (R_cur_v.reshape(-1,1) + R_vg.reshape(-1,1) * R_cur_g.reshape(-1,1)))
        pgap[~tmask.T]=np.nan

        # Foliage accum and density
        foliage_accum =  -(np.log(pgap)) / 0.5
        foliage_dens = (self.dpdz * (1/pgap)) / 0.5

        # Save vals to attributes
        self.cov = veg_cover
        self.covUncorr = veg_cover2
        self.gap = pgap
        self.fdp = foliage_dens
        self.lai = foliage_accum
        
    def bioWF(self):
        ## Waveform based biomass index
        dp = np.absolute(np.diff(self.gap, append=np.nan))
        # biowf = self.ht**self.c * dp
        self.biWF = np.nansum(self.ht**self.c * dp, axis=1)

    def bioFP(self):
        ## Foliage profile based biomass index
        dp = np.absolute(np.diff(self.gap, append=np.nan))
        # biofp = (self.ht**self.c * dp) / self.gap
        self.biFP = np.nansum((self.ht**self.c * dp) / self.gap, axis=1)

    # def biomassFP(self):

    #     ## Testing for tropical forests (Mondah Forest, Gabon)
    #     beta = 0.5
    #     b = np.exp(1.2)
    #     alpha = 3.854
    #     a = np.exp(2.346)
    #     G = 0.5
    #     gamma = 1
    #     F = 0.6
    #     rho = 0.55


    #     k_coef = (np.pi / 40) * ((b**(-2/beta) * F * rho) / (a * G * gamma * Fa)


#############
## FullWF Utilities
#############

def wf_smooth(wfarr,sigmean=None,sd=5):
    
    """
    Parameters
    ----------
    wfarr : ndarray
        Array of lvis data. 2d for multiple waveforms or 1d for single wf.
    sigmean : array_like
        Mean signal noise.
    sd : None or int
        Standard deviation for Gaussian kernel. (Default is 5)
        The larger the smoother the function.

    Returns
    ----------
    Waveforms with mean signal noise removed and
    smoothed using a gaussian 1D kernel.

    """
     
    # Read the waveforms and perform a gaussian smooth
    if sigmean is not None:
        if isinstance(sigmean, int):
             wfarr = wfarr-sigmean
        else:
            if isinstance(sigmean, list):
                sigmean = np.array(sigmean) 
            if wfarr.ndim != sigmean.ndim:
                sigmean = sigmean.reshape(-1,1)
            wfarr = wfarr - sigmean # 2
    smoothwfs = ndimage.gaussian_filter1d(wfarr,sd)

    # Return the smoothed wf
    return smoothwfs



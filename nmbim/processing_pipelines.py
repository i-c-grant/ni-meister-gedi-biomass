import nmbim.algorithms as algorithms

biwf_pipeline = {
    ### Pre-normalization processing ###
    # Calculate height array and dz value
    "height": {
        "alg_fun": algorithms.calc_height,
        "input_map": {
            "wf": "raw/wf",
            "elev_top": "raw/elev/top",
            "elev_bottom": "raw/elev/bottom",
            "elev_ground": "raw/elev/ground",
        },
        "output_path": "processed/ht",
        "params": {},
    },
    "calc_dz": {
        "alg_fun": algorithms.calc_dz,
        "input_map": {"ht": "processed/ht"},
        "output_path": "processed/dz",
        "params": {},
    },
    # Remove initial signal noise from the waveform
    "remove_initial_noise": {
        "alg_fun": algorithms.remove_noise,
        "input_map": {"wf": "raw/wf", "mean_noise": "raw/mean_noise"},
        "output_path": "processed/wf_noise_removed",
        "params": {},
    },
    # Smooth the waveform
    "smooth": {
        "alg_fun": algorithms.smooth_waveform,
        "input_map": {"wf": "processed/wf_noise_removed"},
        "output_path": "processed/wf_noise_removed_smooth",
        "params": {"sd": 8},
    },
    # Identify borders of vegetation and ground returns
    "segment": {
        "alg_fun": algorithms.separate_veg_ground,
        "input_map": {
            "wf": "processed/wf_noise_removed_smooth",
            "ht": "processed/ht",
            "dz": "processed/dz",
            "rh": "raw/rh",
        },
        "params": {"veg_floor": 5,
                   "veg_buffer": 5,},
        "output_path": "processed/veg_ground_sep",
    },
    # Calculate and remove residual noise from smoothed waveform
    "calc_resid_noise": {
        "alg_fun": algorithms.calc_noise,
        "input_map": {
            "wf": "processed/wf_noise_removed_smooth",
            "veg_top": "processed/veg_ground_sep/veg_top",
            "ground_bottom": "processed/veg_ground_sep/ground_bottom",
            "ht": "processed/ht",
        },
        "output_path": "processed/residual_noise",
        "params": {"noise_ratio": 2},
    },
    "remove_resid_noise": {
        "alg_fun": algorithms.remove_noise,
        "input_map": {
            "wf": "processed/wf_noise_removed_smooth",
            "mean_noise": "processed/residual_noise",
        },
        "output_path": "processed/wf_all_noise_removed",
        "params": {},
    },
    # Remove residual noise from raw waveform too for visualization
    "remove_resid_noise_raw": {
        "alg_fun": algorithms.remove_noise,
        "input_map": {
            "wf": "processed/wf_noise_removed",
            "mean_noise": "processed/residual_noise",
        },
        "output_path": "processed/wf_noise_removed_raw",
        "params": {},
    },
    # Scale the raw waveform for visualization
    "scale_raw": {
        "alg_fun": algorithms.scale_raw_wf,
        "input_map": {"wf_raw": "processed/wf_noise_removed_raw",
                      "wf_smooth": "processed/wf_all_noise_removed",
                      "dz": "processed/dz"},
        "output_path": "processed/wf_raw_scaled",
        "params": {},
    },
 
    ### Post-normalization processing ###
    # Normalize the smoothed, noise-removed waveform
    "normalize": {
        "alg_fun": algorithms.normalize_waveform,
        "input_map": {"wf": "processed/wf_all_noise_removed"},
        "output_path": "processed/wf_norm",
        "params": {},
    },
    # Divide the normalized waveform by the dz value
    "dp_dz": {
        "alg_fun": algorithms.calc_dp_dz,
        "input_map": {
            "wf": "processed/wf_norm",
            "dz": "processed/dz",
        },
        "output_path": "processed/dp_dz",
        "params": {},
    },
    # Calculate the ground return
    "ground_return": {
        "alg_fun": algorithms.create_ground_return,
        "input_map": {
            "wf": "processed/dp_dz",
            "ht": "processed/ht",
            "ground_return_max_height": ("processed/"
                                         "veg_ground_sep/"
                                         "ground_bottom"),
        },
        "output_path": "processed/ground_return",
        "params": {"sd_ratio": 0.25},
    },
    # Isolate the vegetation return by removing the ground return
    "isolate_veg": {
        "alg_fun": algorithms.isolate_vegetation,
        "input_map": {
            "wf": "processed/dp_dz",
            "ht": "processed/ht",
            "veg_top": "processed/veg_ground_sep/veg_top",
            "ground_return": "processed/ground_return",
        },
        "output_path": "processed/dp_dz_veg_only",
        "params": {},
    },
    # Calculate the biomass index using the isolated vegetation return
    "calc_biwf": {
        "alg_fun": algorithms.calc_biomass_index,
        "input_map": {
            "dp_dz": "processed/dp_dz_veg_only",
            "dz": "processed/dz",
            "ht": "processed/ht",
            "n_modes": "metadata/modes/num_modes",
        },
        "output_path": "results/biomass_index",
        "params": {"hse": 1.7},
    },
}

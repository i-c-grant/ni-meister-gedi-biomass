import nmbim.algorithms as algorithms

pipeline_test_veg_ground_sep = {
    "height": {
        "alg_fun" : algorithms.calc_height,
        "input_map" : {
            "wf": "raw/wf",
            "elev_top": "raw/elev/top",
            "elev_bottom": "raw/elev/bottom",
            "elev_ground": "raw/elev/ground"},
        "output_path" : "processed/ht",
        "params" : {}},
    "dz" : {"alg_fun" : algorithms.calc_dz,
            "input_map" : {"ht": "processed/ht"},
            "output_path" : "processed/dz",
            "params" : {}},
    "noise": {
        "alg_fun" : algorithms.remove_noise,
        "input_map" : {
            "wf": "raw/wf",
            "mean_noise": "raw/mean_noise"},
        "output_path" : "processed/wf_noise_removed",
        "params" : {}},
    "normalize": {"alg_fun" : algorithms.normalize_waveform,
                  "input_map" : {"wf": "processed/wf_noise_removed"},
                  "output_path" : "processed/wf_noise_norm",
                  "params" : {}},
    "smooth": {
        "alg_fun" : algorithms.smooth_waveform,
        "input_map" : {"wf": "processed/wf_noise_norm"},
        "output_path" : "processed/wf_noise_norm_smooth",
        "params" : {"sd": 8}},
    "dp_dz": {"alg_fun" : algorithms.calc_dp_dz,
              "input_map" : {"wf": "processed/wf_noise_norm_smooth",
                             "dz": "processed/dz"},
              "output_path" : "processed/dp_dz",
              "params" : {}},
    "segment": {
        "alg_fun" : algorithms.separate_veg_ground,
        "input_map" : {
            "wf": "processed/dp_dz",
            "ht": "processed/ht",
            "dz": "processed/dz",
            "rh": "raw/rh"},
        "params": {"veg_floor": 5},
        "output_path" : "processed/veg_ground_sep"},
    "ground_return": {
        "alg_fun" : algorithms.create_ground_return,
        "input_map" : {
            "wf": "processed/dp_dz",
            "ht": "processed/ht",
            "ground_bottom": "processed/veg_ground_sep/ground_bottom",
        },
        "output_path" : "processed/ground_return",
        "params" : {}},
    "isolate_veg": {
        "alg_fun" : algorithms.isolate_vegetation,
        "input_map" : {
            "wf": "processed/dp_dz",
            "ht": "processed/ht",
            "veg_top": "processed/veg_ground_sep/veg_top",
            "ground_return": "processed/ground_return"
        },
        "output_path" : "processed/dp_dz_veg_only",
        "params" : {}},
    "bi": {
        "alg_fun" : algorithms.calc_biomass_index,
        "input_map": {
            "dp_dz": "processed/dp_dz_veg_only",
            "dz": "processed/dz",
            "ht": "processed/ht"
        },
        "output_path" : "results/biomass_index",
        "params" : {"hse": 1.7}
    }
}

pipeline_biomass_index_simple = {
    "height": {"alg_fun" : algorithms.calc_height,
               "input_map" : {"wf": "raw/wf",
                              "elev_top": "raw/elev/top",
                              "elev_bottom": "raw/elev/bottom",
                              "elev_ground": "raw/elev/ground"},
               "output_path" : "processed/ht",
               "params" : {}},
    "dz" : {"alg_fun" : algorithms.calc_dz,
            "input_map" : {"ht": "processed/ht"},
            "output_path" : "processed/dz",
            "params" : {}},
    "noise": {"alg_fun" : algorithms.remove_noise,
              "input_map" : {"wf": "raw/wf",
                             "mean_noise": "raw/mean_noise"},
              "output_path" : "processed/wf_noise_removed",
              "params" : {}},
    "normalize": {"alg_fun" : algorithms.normalize_waveform,
                  "input_map" : {"wf": "processed/wf_noise_removed"},
                  "output_path" : "processed/wf_noise_norm",
                  "params" : {}},
    "smooth": {"alg_fun" : algorithms.smooth_waveform,
               "input_map" : {"wf": "processed/wf_noise_norm"},
               "output_path" : "processed/wf_noise_norm_smooth",
               "params" : {"sd": 5}},
    "dp_dz": {"alg_fun" : algorithms.calc_dp_dz,
              "input_map" : {"wf": "processed/wf_noise_norm_smooth",
                             "dz": "processed/dz"},
              "output_path" : "processed/dp_dz",
              "params" : {}},
    "biomass_index": {"alg_fun" : algorithms.calc_biomass_index,
                      "input_map" : {"dp_dz": "processed/dp_dz",
                                     "ht": "processed/ht",
                                     "dz": "processed/dz"},
                      "output_path" : "results/biomass_index",
                      "params" : {"hse": 1.7,
                                  "ceiling": 80,
                                  "floor": -5}}
}

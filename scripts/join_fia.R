library(sf)
library(tidyverse)
library(ggplot2)
library(cowplot)

df_evalidator <-
  read_csv("data/fia/evalidator/FIADB_API_Estimates_82152638.csv")

sf_menlove_healey <-
  st_read("data/fia/menlove_healey/2020_biohex_merged.gpkg")

## convert sf_menlove_healey EMAP_HEX to character, with leading zeros such taht length is 5
sf_menlove_healey <-
  sf_menlove_healey %>%
  mutate(EMAP_HEX = str_pad(as.character(EMAP_HEX), width = 5, side = "left", pad = "0"))

## filter to only Total land cover type
df_evalidator <-
  df_evalidator %>%
  filter(LAND_USE_MAJOR == "Total") 

## join the two dataframes on EMAP_HEX and EMAPHEX
joined <-
  left_join(sf_menlove_healey, df_evalidator,
            join_by(EMAP_HEX == EMAPHEX))



## dry short tons per acre to megagrams per hectare
## 1 lb  = 0.4535924 kg
## 1 short ton (2000 lb) = 907.1848 kg
## 1 short ton = .9071848 Mg

## 1 acre = 0.404686 hectares

## .9071848 / 0.404686

## 2.2417004789

hex_area_ha <- 64e3
megagrams_per_ton <- .9071848

joined <- joined %>%
  mutate(live_dry_mg_ha = NUMERATOR_ESTIMATE * megagrams_per_ton / hex_area_ha)

red_dashed_line <- geom_abline(intercept = 0, slope = 1, color = "red", linetype = "dashed")

points <- geom_point(aes(y = live_dry_mg_ha), size = .25, alpha = .5)

y_label <- ylab("EVALIDATOR biomass (Mg/ha)")

p_jenk <- joined %>%
  filter(LAND_USE_MAJOR == "Total") %>%
  ggplot(aes(x = JENK_LIVE)) +
  points +
  geom_abline(intercept = 0, slope = 1, color = "red", linetype = "dashed") +
  red_dashed_line +
  xlab("Jenkins biomass (Mg/ha; Menlove & Healey, 2020)") +
  y_label 

p_crm <-
  joined %>%
  filter(LAND_USE_MAJOR == "Total") %>%
  ggplot(aes(x = CRM_LIVE)) +
  points +
  geom_abline(intercept = 0, slope = 1, color = "red", linetype = "dashed") +
  red_dashed_line +
  xlab("CRM biomass (Mg/ha; Menlove & Healey, 2020)") +
  y_label

cowplot::plot_grid(p_jenk, p_crm, nrow = 2)

ggplot(joined) +
  geom_point(aes(x = live_dry_mg_ha,
                 y = JENK_LIVE,
                 color = DENOMINATOR_ESTIMATE),
             size = .5,
             alpha = .5)

plot(joined["NUMERATOR_ESTIMATE"])

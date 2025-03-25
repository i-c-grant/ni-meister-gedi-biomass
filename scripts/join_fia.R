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
  y_label +
  ggtitle("EVALIdator biomass vs Menlove & Healey (2020)")

p_crm <-
  joined %>%
  filter(LAND_USE_MAJOR == "Total") %>%
  ggplot(aes(x = CRM_LIVE)) +
  points +
  geom_abline(intercept = 0, slope = 1, color = "red", linetype = "dashed") +
  red_dashed_line +
  xlab("CRM biomass (Mg/ha; Menlove & Healey, 2020)") +
  y_label

p_scatterplots <- cowplot::plot_grid(p_jenk, p_crm, nrow = 2)
ggsave("reports/fia_comparison/scatterplots.png", plot = p_scatterplots, width = 8, height = 6, dpi = 300)

ggplot(joined) +
  geom_point(aes(x = live_dry_mg_ha,
                 y = JENK_LIVE,
                 color = DENOMINATOR_ESTIMATE),
             size = .5,
             alpha = .5)

plot(joined["NUMERATOR_ESTIMATE"])

# Reshape the data into a long format for easier plotting
joined_long <- joined %>%
  pivot_longer(
    cols = c("CRM_LIVE", "JENK_LIVE", "live_dry_mg_ha"),
    names_to = "source",
    values_to = "biomass"
  ) %>%
  mutate(source = case_match(source,
                             "CRM_LIVE" ~ "CRM",
                             "JENK_LIVE" ~ "Jenkins",
                             .default = source))

# --- Alternative mixed overlay histogram version ---
overlay_histogram_mixed <- ggplot() +
  # Plot CRM and Jenkins as filled histograms
  geom_histogram(
    data = dplyr::filter(joined_long, source %in% c("CRM", "Jenkins")),
    aes(x = biomass, fill = source),
    bins = 100,
    position = "identity",
    alpha = 0.4,  # keeps transparency for fill
    color = NA   # no outline
  ) +
  # Plot Evalidator distribution as a red line
  stat_bin(
    data = dplyr::filter(joined_long, source == "live_dry_mg_ha"),
    aes(x = biomass, y = after_stat(count)),
    bins = 100,
    geom = "line",
    color = "red",
    size = 1
  ) +
  labs(
    title = "EVALIDator vs Menlove & Healey (2020)",
    x = "Biomass (Mg/ha)",
    y = "Number of EMAP hexagons",
    fill = "Source"
  ) +
  theme_classic() +
  xlim(0, 400) +
  ylim(0, 2000)

# Render the mixed overlay histogram plot
print(overlay_histogram_mixed)

ggsave("reports/fia_comparison/overlay_histogram_mixed.png", plot = overlay_histogram_mixed, width = 8, height = 6, dpi = 300)

## Calculate differences between biomass estimates
joined <- joined %>%
  mutate(
    diff_evalidator_crm = live_dry_mg_ha - CRM_LIVE,
    diff_evalidator_jenk = live_dry_mg_ha - JENK_LIVE,
    diff_crm_jenk = CRM_LIVE - JENK_LIVE
  )

## Write to a geopackage in /home/ian/projects/ni-meister-gedi-biomass-global/data/fia/
st_write(joined, "/home/ian/projects/ni-meister-gedi-biomass-global/data/fia/joined_fia.gpkg", delete_dsn = TRUE)

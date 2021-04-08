#' Create the Dataset
#' @import dplyr
#' @return WP Success Rates
#' @export
add_wp_success_rate <- function(){

  # Message
  message("Constructing WP Success Rate")

  ### Load Data
  NFL_PBP <- dplyr::tbl(
    DBI::dbConnect(
      RSQLite::SQLite(), "/Users/jonathangoldberg/Google Drive/Random/Sports/Fantasy Football/Data/Raw NFL Seasons Data/pbp_db"),
    "nflfastR_pbp")

  ### Get
  starting_home_wp <- NFL_PBP %>%
    dplyr::filter(season >= 2007, vegas_home_wp < .95 & vegas_home_wp > .05, !is.na(vegas_home_wp)) %>%
    dplyr::collect() %>%
    dplyr::group_by(game_id) %>%
    dplyr:: mutate(play_number = row_number()) %>%
    #tidyr::fill(vegas_home_wp, .direction = "up") %>%
    dplyr::filter(play_number == 1) %>%
    dplyr::select(game_id, vegas_home_wp, play_number) %>%
    dplyr::rename(
      starting_home_wp = vegas_home_wp
    ) %>%
    dplyr:: select(-play_number)

  ### merge w/ PBP
  data <- starting_home_wp %>%
    dplyr::left_join(
      NFL_PBP %>% dplyr::filter(season >= 2007)  %>% dplyr::collect()
      ,
      by = "game_id"
    )

  ### Get Home_Team and Away_Team stats
  # home
  home_success_rate <- data %>%
    dplyr::mutate(wp_success_rate = ifelse(vegas_home_wp > starting_home_wp, 1, 0)) %>%
    dplyr::group_by(game_id, home_team) %>%
    dplyr::summarise(
      wp_success_rate = mean(wp_success_rate, na.rm = TRUE)
    ) %>%
    dplyr::rename(
      team = home_team
    ) %>%
    dplyr::ungroup() %>%
    dplyr::select(game_id, team, wp_success_rate)
  # away
  away_success_rate <- data %>%
    dplyr::mutate(wp_success_rate = ifelse(1-vegas_home_wp > 1-starting_home_wp, 1, 0)) %>%
    dplyr::group_by(game_id, away_team) %>%
    dplyr::summarise(
      wp_success_rate = mean(wp_success_rate, na.rm = TRUE)
      ) %>%
    dplyr::rename(
      team = away_team
    ) %>%
    dplyr::ungroup() %>%
    dplyr::select(game_id, team, wp_success_rate)
  # merge
  wp_success_rate <- dplyr::bind_rows(home_success_rate, away_success_rate)

  return(wp_success_rate)
}

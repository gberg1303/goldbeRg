#' Create the Dataset
#' @import dplyr
#' @return Quarterback Rating Dataset
#' @export
add_qb_rating <- function(keep_latest_performance = FALSE){

  ### Load Data
  NFL_PBP <- dplyr::tbl(
    DBI::dbConnect(
      RSQLite::SQLite(), "/Users/jonathangoldberg/Google Drive/Random/Sports/Fantasy Football/Data/Raw NFL Seasons Data/pbp_db"),
    "nflfastR_pbp")

  ### QB Data
  qb_data <- NFL_PBP %>%
    dplyr::filter(!is.na(cpoe), !is.na(epa), qb_dropback == 1) %>%
    dplyr::group_by(passer_player_name, passer_player_id, posteam, season, week) %>%
    # Get Dropbacks
    dplyr::summarise(
      cpoe = mean(cpoe),
      epa = mean(epa),
      dropbacks = sum(qb_dropback)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::collect() %>%
    # Group and Get Moving Average
    dplyr::arrange(passer_player_name, season, week) %>%
    dplyr::group_by(passer_player_name, passer_player_id) %>%
    dplyr::mutate(
      games = n(),
      dropbacks = cumsum(dropbacks)
    ) %>%
    dplyr::filter(games >= 6) %>%
    dplyr::mutate(
      cpoe = dplyr::lag(pracma::movavg(cpoe, n = 5, type = "s")),
      epa = dplyr::lag(pracma::movavg(epa, n = 5, type = "s")),
    ) %>%
    dplyr::ungroup() %>%
    dplyr::filter(dropbacks >= 100) %>%
    # Add Composite
    dplyr::mutate(Composite = cpoe*.009+epa*.21+.09) %>%
    # Select Data that Matters
    dplyr::select(passer_player_name, passer_player_id, season, week, games, Composite)



  ### Starters
  starters <- NFL_PBP %>%
    dplyr::filter(qtr < 3, !is.na(posteam), !is.na(passer_player_name)) %>%
    dplyr::group_by(game_id, passer_player_name, passer_player_id, posteam, season, week) %>%
    # Get Starters
    dplyr::summarise(
      dropbacks = sum(qb_dropback)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::group_by(game_id, posteam, season, week) %>%
    dplyr::filter(dropbacks == max(dropbacks)) %>%
    dplyr::select(-dropbacks) %>%
    dplyr::collect()

  ### Combine
  starters_dataset <- starters %>%
    dplyr::left_join(
      qb_data,
      by = c("season", "week", "passer_player_name", "passer_player_id")
    ) %>%
    dplyr::filter(season > 2006) %>%
    dplyr::mutate(Composite = ifelse(is.na(Composite) == TRUE, .01, Composite),
                  games = ifelse(is.na(games) == TRUE, 1, games)) %>%
    # remove more than one QB in a game
    dplyr::group_by(game_id, posteam, season, week) %>%
    dplyr::filter(games == max(games))

  ### Get Latest QB Performance
  latest_qb_performance <- starters_dataset %>%
    dplyr::group_by(passer_player_name, passer_player_id) %>%
    dplyr::arrange(season, week) %>%
    dplyr::mutate(number = dplyr::row_number()) %>%
    dplyr::filter(number == max(number)) %>%
    dplyr::ungroup() %>%
    dplyr::group_by(passer_player_name) %>%
    dplyr::filter(season == max(season)) %>%
    dplyr::ungroup() %>%
    dplyr::select(passer_player_name, passer_player_id, Composite)


  ifelse(keep_latest_performance == TRUE, return(latest_qb_performance), return(starters_dataset))

}




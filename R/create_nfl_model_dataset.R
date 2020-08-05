#' Create the Dataset
#' @import dplyr
#' @param keep_latest_performance: This will determine whether the dataset that is outputted will be the team's latest performance or the full model dataset
#' @return Model Dataset
#' @export
create_nfl_modeldataset <- function(keep_latest_performance = FALSE){


  ### Load Data
  message(paste("Grabbing NFL Data"))
  NFL_PBP <- dplyr::tbl(
    DBI::dbConnect(
      RSQLite::SQLite(), "/Users/jonathangoldberg/Google Drive/Random/Sports/Fantasy Football/Data/Raw NFL Seasons Data/pbp_db"),
                 "nflfastR_pbp")

  ### Get Game EPA Data
  message(paste("Creating EPA Data"))
  epa_data <- NFL_PBP %>%
    dplyr::filter(!is.na(epa), !is.na(ep), !is.na(posteam), play_type=="pass" | play_type=="run", season >= 2007) %>%
    dplyr::group_by(game_id, season, week, posteam, home_team) %>%
    dplyr::summarise(
      off_epa= mean(epa),
    ) %>%
    dplyr::left_join(NFL_PBP %>%
                filter(!is.na(epa), !is.na(ep), !is.na(posteam), play_type=="pass" | play_type=="run", season >= 2007) %>%
                dplyr::group_by(game_id, season, week, defteam, away_team) %>%
                  dplyr::summarise(def_epa=mean(epa)),
              by = c("game_id", "posteam" = "defteam", "season", "week"),
              all.x = TRUE) %>%
    dplyr::mutate(opponent = ifelse(posteam == home_team, away_team, home_team)) %>%
    dplyr::select(game_id, season, week, home_team, away_team, posteam, opponent, off_epa, def_epa) %>%
    dplyr::collect()
  # Merge Back Opponent
  epa_data <- epa_data %>%
    dplyr::left_join(epa_data %>%
                       dplyr::select(-opponent) %>%
                       dplyr::rename(
                         opp_off_epa = off_epa,
                         opp_def_epa = def_epa
                ) %>%
                  dplyr::group_by(posteam) %>%
                  dplyr::arrange(season, week) %>%
                  dplyr::mutate(
                    opp_def_epa = pracma::movavg(opp_def_epa, n = 10, type = "s"),
                    opp_def_epa = dplyr::lag(opp_def_epa),
                    opp_off_epa = pracma::movavg(opp_off_epa, n = 10, type = "s"),
                    opp_off_epa = dplyr::lag(opp_off_epa),
                ), by = c("game_id", "season", "week", "home_team", "away_team", "opponent" = "posteam"),
              all.x = TRUE)
  # Merge Back League Mean
  epa_data <- epa_data %>%
    dplyr::left_join(epa_data %>%
                       dplyr::filter(posteam == home_team) %>%
                       dplyr::group_by(season, week) %>%
                       dplyr::summarise(
                         league_mean = mean(off_epa + def_epa)
                         ) %>%
                       dplyr::ungroup() %>%
                       dplyr::group_by(season) %>%
                       dplyr::mutate(
                         league_mean = lag(cummean(league_mean))
                ),
              by = c("season", "week"),
              all.x = TRUE)
  #Adjust EPA
  epa_data <- epa_data %>%
    dplyr::mutate(
      off_adjustment_factor = ifelse(!is.na(league_mean), league_mean-opp_def_epa, 0),
      def_adjustment_factor = ifelse(!is.na(league_mean), league_mean-opp_off_epa, 0),
      adjusted_off_epa = off_epa + off_adjustment_factor,
      adjusted_def_epa = def_epa + def_adjustment_factor,
    )
  # Group and Get Moving Average
  epa_data <- epa_data %>%
    dplyr::group_by(posteam) %>%
    dplyr::arrange(season, week) %>%
    dplyr::mutate(
      def_epa = pracma::movavg(def_epa, n = 10, type = "s"),
      def_epa = dplyr::lag(def_epa),
      off_epa = pracma::movavg(off_epa, n = 10, type = "s"),
      off_epa = dplyr::lag(off_epa),
      adjusted_off_epa = dplyr::lag(pracma::movavg(adjusted_off_epa, n = 10, type = "s")),
      adjusted_def_epa = dplyr::lag(pracma::movavg(adjusted_def_epa, n = 10, type = "s"))
    ) %>%
    dplyr::ungroup() %>%
    dplyr::select(-home_team, -away_team) %>%
    dplyr::select(game_id, posteam, season, week, off_epa, def_epa, adjusted_off_epa, adjusted_def_epa)

  ### Get Schedule and Game Outcomes
  message(paste("Creating Schedule and Game Outcomes"))
  NFL_Outcomes_Weekly <- NFL_PBP %>%
    dplyr::filter(season_type == "REG" | season_type == "POST") %>%
    dplyr::group_by(season, week, game_date, game_id, home_team, away_team, game_date) %>%
    dplyr::summarise(home_score = max(total_home_score),
              away_score = max(total_away_score)) %>%
    dplyr::mutate(winner = ifelse(home_score > away_score, home_team, away_team),
           loser = ifelse(home_score > away_score, away_team, home_team)) %>%
    dplyr::mutate(team = home_team,
           opponent = away_team) %>%
    dplyr::mutate(win = ifelse(team == winner, 1, 0)) %>%
    dplyr::collect() %>%
    dplyr::bind_rows(x = .,
              y = NFL_PBP %>%
                dplyr::filter(season_type == "REG" | season_type == "POST") %>%
                dplyr::group_by(season, week, game_date, game_id, home_team, away_team, game_date) %>%
                dplyr::summarise(home_score = max(total_home_score),
                          away_score = max(total_away_score)) %>%
                dplyr::mutate(winner = ifelse(home_score > away_score, home_team, away_team),
                       loser = ifelse(home_score > away_score, away_team, home_team)) %>%
                dplyr::mutate(team = away_team,
                       opponent = home_team) %>%
                dplyr::mutate(win = ifelse(team == winner, 1, 0)) %>%
                dplyr::collect()) %>%
    dplyr::mutate(point_differential = ifelse(team == home_team, home_score-away_score, away_score-home_score),
           points_for = ifelse(team == home_team, home_score, away_score),
           points_against = ifelse(team == home_team, away_score, home_score)) %>%
    dplyr::ungroup()
  # Merge Back Opponent
  NFL_Outcomes_Weekly <- NFL_Outcomes_Weekly %>%
    dplyr::left_join(
      NFL_Outcomes_Weekly %>%
        dplyr::select(game_id, season, week, team, opponent, points_for, points_against) %>%
        dplyr::rename(
          opp_points_for = points_for,
          opp_points_against = points_against
        ) %>%
        dplyr::group_by(team) %>%
        dplyr::arrange(season, week) %>%
        dplyr::mutate(
          opp_points_for = lag(pracma::movavg(opp_points_for, n = 10, type = "s")),
          opp_points_against = lag(pracma::movavg(opp_points_against, n = 10, type = "s")),
        ) %>%
        dplyr::select(-season, -week),
      by = c("game_id", "team" = "opponent", "opponent" = "team"),
      all.x = TRUE
    )
  # Group and Create the Lagged Moving Average
  NFL_Outcomes_Weekly <- NFL_Outcomes_Weekly %>%
    dplyr::group_by(team) %>%
    dplyr::arrange(season, week) %>%
    dplyr::mutate(
      point_differential = dplyr::lag(pracma::movavg(point_differential, n = 10, type = "s"))
    ) %>%
    dplyr::ungroup()

  ### Create Model Dataset
  message(paste("Merging to Create Model Dataset"))
  Model_Dataset <- NFL_Outcomes_Weekly %>%
    # Add Opponent Box Score Statistics
    merge(y = NFL_Outcomes_Weekly %>%
            dplyr::select(game_id, team, point_differential) %>%
            dplyr::rename(
              opp_point_differential = point_differential
            ),
          by.x = c("opponent", "game_id"),
          by.y = c("team", "game_id")) %>%
    # Add EPA Statistics
    dplyr::left_join(epa_data, by = c("game_id", "season", "week",  "home_team" = "posteam")) %>%
    dplyr::left_join(epa_data %>%
                       dplyr::rename(
                  opp_off_epa = off_epa,
                  opp_def_epa = def_epa,
                  opp_adjusted_off_epa = adjusted_off_epa,
                  opp_adjusted_def_epa = adjusted_def_epa),
              by = c("game_id", "season", "week", "away_team" = "posteam")) %>%
    dplyr::filter(home_team == team) %>%
    # Add Home Margin and Playoff Indication
    dplyr::mutate(home_margin = home_score - away_score
    ) %>%
    dplyr::select(-opponent, -team, -winner, -loser) %>%
    # Merge out CPOEless statistics
    dplyr::filter(season >= 2007) %>%
    # Filter NAs
    dplyr::filter(!is.na(off_epa)) %>%
    # Add Numeric ID
    dplyr::mutate(numeric_id = row_number())

  ### Append Newest Season
  # Get Games
  games <- nflfastR::fast_scraper_schedules(max(NFL_Outcomes_Weekly$season)+1) %>% dplyr::select(game_id, season, week, home_team, away_team)
  # Get latest performance
  latest_team_performance <- NFL_Outcomes_Weekly %>%
    # Add Opponent Box Score Statistics
    merge(y = NFL_Outcomes_Weekly %>%
            dplyr::select(game_id, team, point_differential) %>%
            dplyr::rename(
              opp_point_differential = point_differential
            ),
          by.x = c("opponent", "game_id"),
          by.y = c("team", "game_id")) %>%
    # Add EPA Statistics
    dplyr::left_join(epa_data, by = c("game_id", "season", "week",  "home_team" = "posteam")) %>%
    dplyr::left_join(epa_data %>%
                       dplyr::rename(
                         opp_off_epa = off_epa,
                         opp_def_epa = def_epa,
                         opp_adjusted_off_epa = adjusted_off_epa,
                         opp_adjusted_def_epa = adjusted_def_epa),
                     by = c("game_id", "season", "week", "away_team" = "posteam")) %>%
    arrange(season, week) %>%
    dplyr::mutate(numeric_id = row_number()) %>%
    # Add Home Margin and Playoff Indication
    dplyr::mutate(home_margin = home_score - away_score
    ) %>%
    # Merge out CPOEless statistics
    dplyr::filter(season >= 2007) %>%
    # Filter NAs
    dplyr::filter(!is.na(off_epa)) %>%
    dplyr::group_by(team) %>%
    dplyr::filter(numeric_id == max(numeric_id)) %>%
    dplyr::select(-opponent, -team, -winner, -loser) %>%
    dplyr::select(team, point_differential, adjusted_off_epa, adjusted_def_epa)
  # Merge the player statistics over
  Model_Dataset_Append <- games %>%
    dplyr::filter(game_id %in% Model_Dataset$game_id == FALSE) %>%
    dplyr::left_join(latest_team_performance, by = c("home_team" = "team")) %>%
    dplyr::left_join(latest_team_performance %>%
                       dplyr::rename(
                         opp_point_differential = point_differential,
                         opp_adjusted_off_epa = adjusted_off_epa,
                         opp_adjusted_def_epa = adjusted_def_epa
                       ), by = c("away_team" = "team"))
  # Append Newest Season
  Model_Dataset <- dplyr::bind_rows(
    Model_Dataset %>%
      dplyr::mutate(game_completed = 1),
    Model_Dataset_Append %>%
      dplyr::mutate(game_completed = 0))

  ### Add Game Location
  # Scrape game Location
  game_location <- nflfastR::fast_scraper_schedules(min(Model_Dataset$season):max(Model_Dataset$season)) %>%
    dplyr::select(game_id, location)
  # Merge Game Location
  Model_Dataset <- Model_Dataset %>%
    dplyr::left_join(game_location, by = "game_id")

  ifelse(keep_latest_performance == TRUE, return(latest_team_performance), return(Model_Dataset))

}

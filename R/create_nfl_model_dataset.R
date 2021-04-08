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
              by = c("game_id", "posteam" = "defteam", "season", "week")) %>%
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
              #all.x = TRUE
              )
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
              #all.x = TRUE
              )
  #Adjust EPA
  epa_data <- epa_data %>%
    dplyr::mutate(
      off_adjustment_factor = ifelse(!is.na(league_mean), league_mean-opp_def_epa, 0),
      def_adjustment_factor = ifelse(!is.na(league_mean), league_mean-opp_off_epa, 0),
      adjusted_off_epa = off_epa + off_adjustment_factor,
      adjusted_def_epa = def_epa + def_adjustment_factor,
    )
  # merge WP Success Rate
  #wp_success <- add_wp_success_rate()
  #epa_data <- epa_data %>%
    # Home Team
  #  dplyr::left_join(
  #    wp_success,
  #    by = c("game_id", "posteam" = "team")
  #  )
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
      adjusted_def_epa = dplyr::lag(pracma::movavg(adjusted_def_epa, n = 10, type = "s")),
      adjusted_off_epa_sd = dplyr::lag(roll::roll_sd(adjusted_off_epa, width = 10)),
      adjusted_def_epa_sd = dplyr::lag(roll::roll_sd(adjusted_def_epa, width = 10)),
     # wp_success_rate = dplyr::lag(pracma::movavg(wp_success_rate, n = 3, type = "s"))
    ) %>%
    dplyr::ungroup() %>%
    dplyr::select(-home_team, -away_team) %>%
    dplyr::select(game_id, posteam, season, week, off_epa, def_epa, adjusted_off_epa, adjusted_def_epa, adjusted_off_epa_sd, adjusted_def_epa_sd,
                 # wp_success_rate
                  )

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
     # all.x = TRUE
    )
  # Group and Create the Lagged Moving Average
  NFL_Outcomes_Weekly <- NFL_Outcomes_Weekly %>%
    dplyr::group_by(team) %>%
    dplyr::arrange(season, week) %>%
    dplyr::mutate(
      point_differential = dplyr::lag(pracma::movavg(point_differential, n = 10, type = "s")),
      point_differential_sd = dplyr::lag(roll::roll_sd(point_differential, width = 10))
    ) %>%
    dplyr::ungroup()

  ### Create Model Dataset
  message(paste("Merging to Create Model Dataset"))
  Model_Dataset <- NFL_Outcomes_Weekly %>%
    # Add Opponent Box Score Statistics
    merge(y = NFL_Outcomes_Weekly %>%
            dplyr::select(game_id, team, point_differential, point_differential_sd) %>%
            dplyr::rename(
              opp_point_differential = point_differential,
              opp_point_differential_sd = point_differential_sd
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
                  opp_adjusted_def_epa = adjusted_def_epa,
                  opp_adjusted_off_epa_sd = adjusted_off_epa_sd,
                  opp_adjusted_def_epa_sd = adjusted_def_epa_sd,
                  #opp_wp_success_rate = wp_success_rate
                  ),
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

  ### Add QB Data
  message(paste("Adding QB Data"))
  # Load Data
  qbs <- add_qb_rating()
  # Merge Over
  Model_Dataset <- Model_Dataset %>%
    # Add Home QB
    dplyr::left_join(
      qbs %>% dplyr::ungroup() %>% dplyr::select(game_id, posteam, passer_player_name, Composite) %>% dplyr::rename(home_qb = Composite, qb1 = passer_player_name),
      by = c("home_team" = "posteam", "game_id")
    ) %>%
    # Add Away QB
    dplyr::left_join(
      qbs %>% dplyr::ungroup() %>% dplyr::select(game_id, posteam, passer_player_name, Composite) %>% dplyr::rename(away_qb = Composite, qb2 = passer_player_name),
      by = c("away_team" = "posteam", "game_id")
    )


  ### Append Newest Season
  # Get Games
  games <- tryCatch(nflfastR::fast_scraper_schedules(max(NFL_Outcomes_Weekly$season)+1) %>% dplyr::select(gameday, game_id, season, week, home_team, away_team), error = function(x){
    nflfastR::fast_scraper_schedules(max(NFL_Outcomes_Weekly$season)) %>% dplyr::select(gameday, game_id, season, week, home_team, away_team)
  })
  # Get latest performance
  latest_team_performance <- NFL_Outcomes_Weekly %>%
    # Add Opponent Box Score Statistics
    merge(y = NFL_Outcomes_Weekly %>%
            dplyr::select(game_id, team, point_differential, point_differential_sd) %>%
            dplyr::rename(
              opp_point_differential = point_differential,
              opp_point_differential_sd = point_differential_sd
            ),
          by.x = c("opponent", "game_id"),
          by.y = c("team", "game_id")) %>%
    # Add EPA Statistics
    dplyr::left_join(epa_data, by = c("game_id", "season", "week",  "team" = "posteam")) %>%
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
    dplyr::select(team, point_differential, adjusted_off_epa, adjusted_def_epa, point_differential_sd, adjusted_off_epa_sd, adjusted_def_epa_sd)
  # Add Latest QB Data
  message(paste("Adding QB Starters for Rest of Season"))
  # Get latest performance and future starters
  qb_latest <- add_qb_rating(keep_latest_performance = TRUE)
  qb_starters <- readr::read_csv("https://projects.fivethirtyeight.com/nfl-api/nfl_elo_latest.csv") %>%
    dplyr::mutate(
      team1 = gsub("WSH", "WAS", team1),
      team2 = gsub("WSH", "WAS", team2),
      team1 = gsub("LAR", "LA", team1),
      team2 = gsub("LAR", "LA", team2),
      team1 = gsub("OAK", "LV", team1),
      team2 = gsub("OAK", "LV", team2)
    ) %>%
    dplyr::select(date, season, team1, team2, qb1, qb2) %>%
    dplyr::mutate(qb1 = paste0(substr(qb1, 1, 1), ".", sub("^\\S+\\s+", '', qb1)),
                  qb2 = paste0(substr(qb2, 1, 1), ".", sub("^\\S+\\s+", '', qb2)),
    ) %>%
    dplyr::left_join(
      qb_latest %>% dplyr::ungroup() %>% dplyr::select(passer_player_name, Composite) %>% dplyr::rename(home_qb = Composite),
      by = c("qb1" = "passer_player_name")
    ) %>%
    dplyr::left_join(
      qb_latest %>% dplyr::ungroup() %>% dplyr::select(passer_player_name, Composite) %>% dplyr::rename(away_qb = Composite),
      by = c("qb2" = "passer_player_name")
    ) %>%
    dplyr::mutate(home_qb = ifelse(is.na(home_qb) == TRUE, .01, home_qb),
                  away_qb = ifelse(is.na(away_qb) == TRUE, .01, away_qb))
  # Merge into Schedule
  games <- games %>%
    dplyr::mutate(gameday = as.Date(gameday)) %>%
    dplyr::left_join(qb_starters,
              by = c("gameday" = "date", "season", "home_team" = "team1", "away_team" = "team2"))
  # Merge the player statistics over
  Model_Dataset_Append <- games %>%
    dplyr::filter(game_id %in% Model_Dataset$game_id == FALSE) %>%
    dplyr::left_join(latest_team_performance,
                     by = c("home_team" = "team")) %>%
    dplyr::left_join(latest_team_performance %>%
                       dplyr::rename(
                         opp_point_differential = point_differential,
                         opp_point_differential_sd = point_differential_sd,
                         opp_adjusted_off_epa = adjusted_off_epa,
                         opp_adjusted_def_epa = adjusted_def_epa,
                         opp_adjusted_off_epa_sd = adjusted_off_epa_sd,
                         opp_adjusted_def_epa_sd = adjusted_def_epa_sd
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
    dplyr::select(game_id, location, stadium)
  # Merge Game Location
  Model_Dataset <- Model_Dataset %>%
    dplyr::left_join(game_location, by = "game_id")

  ifelse(keep_latest_performance == TRUE, return(latest_team_performance), return(Model_Dataset))

}

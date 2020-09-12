#'Simulation Seasons
#'@param year: The season you wish to simulate
#' @param simulations: how many times do you wish to simulate the season?
#' @param seed: seed for the model. pick a random number.
#' @param preseason: this will simulate the season from scratch based on latest performance data that account for offseason regression to the mean.
#' @return dataset of results for season simulations
#' @export
simulate_season <- function(year, seed = 123, simulations = 1000, preseason = TRUE){

  ### Do Multiprocess
  message("Loading Multiprocess")
  future::plan(future::multiprocess)

  ### Generate Season Predictions, Get Latest Data, Load Model
  latest_performance_data <- create_nfl_modeldataset(keep_latest_performance = TRUE)
  model <- create_nfl_model(year = year, dataset = create_nfl_modeldataset(keep_latest_performance = FALSE), seed = seed)
  season_prediction <- generate_nfl_predictions(years = year)

  # For Active Season Predictions, ensure that completed games already count.
  if(preseason == FALSE){
  season_prediction <- season_prediction %>%
    dplyr::mutate(model_home_wp = ifelse(game_completed == 1, win, model_home_wp))
  }

  ### Run the Simulations
  simulation_outcomes <- furrr::future_map_dfr(1:simulations, function(simulations){

    ### Get Playoff Outcomes
    outcomes <- season_prediction %>%
      # Randomly choose outcome given probabilities
      dplyr::mutate(outcome = stats::rbinom(n = nrow(season_prediction), size = 1, prob=model_home_wp))

    # Double Teams
    team_wins <- outcomes %>%
      dplyr::mutate(team = home_team) %>%
      dplyr::rename(team_wp = model_home_wp) %>%
      dplyr::bind_rows(
        outcomes %>%
          dplyr::mutate(team = away_team) %>%
          dplyr::rename(team_wp = model_home_wp) %>%
          dplyr::mutate(team_wp = 1 - team_wp,
                        outcome = 1 - outcome)
      ) %>%

      # Group and standings
      dplyr::group_by(team) %>%
      dplyr::summarise(wins = sum(outcome)) %>%

      # Add Division and Conference
      dplyr::left_join(goldbeRg::nfl_division, by = c("team" = "team_abbr"), all.x = TRUE) %>%

       # Find Playoff Teams
      dplyr::group_by(division) %>%
      dplyr::mutate(
        division_rank = rank(-wins, ties.method = "random"),
        division_winner = ifelse(division_rank == 1, 1, 0)) %>%
      dplyr::group_by(conference, division_winner) %>%
      dplyr::mutate(
        conference_rank = rank(-wins, ties.method = "random"),
        ) %>%
      dplyr::ungroup() %>%
      dplyr::mutate(
        conference_rank = ifelse(division_winner == 0, conference_rank + 4, conference_rank),
        first_seed = ifelse(conference_rank == 1, 1, 0),
        wild_card = ifelse(conference_rank > 1 & conference_rank <= 7, 1, 0),
        playoff = ifelse(first_seed == 1 | wild_card == 1, 1, 0)
        ) %>%
      dplyr::select(-conference_rank, -division_rank) %>%

       # Add Sim Number for Mapping
      dplyr::mutate(sim_no = simulations)

    ### Wild Card Round
    # Seed Teams
    wild_card_seeds <- team_wins %>%
      dplyr::filter(wild_card == 1) %>%
      dplyr::group_by(conference, division_winner) %>%
      dplyr::mutate(seed = rank(-wins, ties.method = "random")) %>%
      dplyr::ungroup() %>%
      dplyr::mutate(seed = ifelse(division_winner == 0 & seed == 1, 4, seed),
                    seed = ifelse(division_winner == 0 & seed == 2, 5, seed),
                    seed = ifelse(division_winner == 0 & seed == 3, 6, seed))

    # Create Matchups
    wild_card_matchups <- dplyr::tibble(home_team_seed = c(1, 2, 3),
                                    away_team_seed = c(6, 5, 4)) %>%
      dplyr::left_join(wild_card_seeds %>% dplyr::select(team, seed, conference), by = c("home_team_seed" = "seed")) %>%
      dplyr::left_join(wild_card_seeds %>% dplyr::select(team, seed, conference), by = c("away_team_seed" = "seed")) %>%
      dplyr::rename(home_team = team.x,
                    away_team = team.y,
                    conference = conference.x) %>%
      dplyr::filter(conference == conference.y) %>%
      dplyr::select(-conference.y)

    # Merge Latest Performance Data
    wild_card_matchups <- wild_card_matchups %>%
      dplyr::left_join(latest_performance_data, by = c("home_team" = "team")) %>%
      dplyr::left_join(latest_performance_data %>%
                         dplyr::rename(
                           opp_point_differential = point_differential,
                           opp_adjusted_off_epa = adjusted_off_epa,
                           opp_adjusted_def_epa = adjusted_def_epa), by = c("away_team" = "team")) %>%
      dplyr::mutate(location = "Home")
    # Make Prediction Probability and Simulate
    wild_card_outcomes <- wild_card_matchups %>%
      dplyr::mutate(model_home_wp = caret::predict.train(newdata = wild_card_matchups, object = model, type = "prob")[,2]) %>%
      dplyr::mutate(outcome = stats::rbinom(n = nrow(wild_card_matchups), size = 1, prob=model_home_wp))

    ### Divisional Round
    # Grab Wild Card Winners and reseed
    wild_card_winners <- wild_card_outcomes %>%
      dplyr::mutate(
        team = ifelse(outcome == 1, home_team, away_team),
        seed = ifelse(outcome == 1, home_team_seed, away_team_seed)
      ) %>%
      dplyr::select(team, seed, conference) %>%
      dplyr::group_by(conference) %>%
      dplyr::mutate(seed = rank(seed)) %>%
      dplyr::mutate(seed = seed + 1) %>%
      dplyr::ungroup()

    # Create Divisional Matchup
    divisional_matchup <- dplyr::tibble(home_team_seed = c(1, 2),
                                        away_team_seed = c(3, 4)) %>%
      dplyr::left_join(team_wins %>%
                         dplyr::filter(first_seed == 1) %>%
                         dplyr::mutate(seed = 1) %>%
                         dplyr::select(team, conference, seed) %>%
                         dplyr::bind_rows(
                           wild_card_winners %>%
                             dplyr::filter(seed <= 3) %>%
                             dplyr::select(team, conference, seed)
                    ),
                by = c("home_team_seed" = "seed")) %>%
      dplyr::left_join(
        wild_card_winners %>%
          dplyr::filter(seed >= 3) %>%
          dplyr::select(team, conference, seed),
        by = c("away_team_seed" = "seed", "conference")) %>%
      dplyr::rename(
        home_team = team.x,
        away_team = team.y
      ) %>%
    # Add Performance Data
      dplyr::left_join(latest_performance_data, by = c("home_team" = "team")) %>%
      dplyr::left_join(latest_performance_data %>%
                         dplyr::rename(
                           opp_point_differential = point_differential,
                           opp_adjusted_off_epa = adjusted_off_epa,
                           opp_adjusted_def_epa = adjusted_def_epa), by = c("away_team" = "team")) %>%
      dplyr::mutate(location = "Home")

    # Predict and Simulate Divisional Round
    divisional_outcomes <- divisional_matchup %>%
      dplyr::mutate(model_home_wp = caret::predict.train(newdata = ., object = model, type = "prob")[,2]) %>%
      dplyr::mutate(outcome = stats::rbinom(n = nrow(.), size = 1, prob=model_home_wp))

    ### Conference Championship
    # Grab winners and reseed
    conference_matchup <- divisional_outcomes %>%
      dplyr::mutate(
        team = ifelse(outcome == 1, home_team, away_team),
        seed = ifelse(outcome == 1, home_team_seed, away_team_seed)
      ) %>%
      dplyr::select(team, seed, conference) %>%
      dplyr::group_by(conference) %>%
      dplyr::mutate(seed = rank(seed)) %>%
      dplyr::ungroup()

    # Create Conference Championship Matchup
    conference_matchup <- conference_matchup %>%
      dplyr::select(team, conference, seed) %>%
      dplyr::filter(seed == 1) %>%
      dplyr::mutate(away_team_seed = 2) %>%
      dplyr::left_join(conference_matchup %>% dplyr::filter(seed == 2), by = c("away_team_seed" = "seed", "conference")) %>%
      dplyr::rename(
        home_team = team.x,
        away_team = team.y,
        home_team_seed = seed
      ) %>%
      # Add Performance Data
      dplyr::left_join(latest_performance_data, by = c("home_team" = "team")) %>%
      dplyr::left_join(latest_performance_data %>%
                         dplyr::rename(
                           opp_point_differential = point_differential,
                           opp_adjusted_off_epa = adjusted_off_epa,
                           opp_adjusted_def_epa = adjusted_def_epa), by = c("away_team" = "team")) %>%
      dplyr::mutate(location = "Home")

    # Predict and Simulate Divisional Round
    conference_outcome <- conference_matchup %>%
      dplyr::mutate(model_home_wp = caret::predict.train(newdata = conference_matchup, object = model, type = "prob")[,2]) %>%
      dplyr::mutate(outcome = stats::rbinom(n = nrow(conference_matchup), size = 1, prob=model_home_wp))

    ### Super Bowl Simulation
    # Grab Wild Card Winners and reseed
    super_matchup <- conference_outcome %>%
      dplyr::mutate(
        team = ifelse(outcome == 1, home_team, away_team),
        seed = ifelse(outcome == 1, home_team_seed, away_team_seed)
      ) %>%
      dplyr::select(team, seed, conference) %>%
      dplyr::group_by(conference) %>%
      dplyr::mutate(seed = rank(seed)) %>%
      dplyr::ungroup()

    # Create Conference Championship Matchup
    super_matchup <- super_matchup %>%
      dplyr::select(team, conference, seed) %>%
      dplyr::mutate(away_team_seed = 1) %>%
      dplyr::left_join(super_matchup, by = c("away_team_seed" = "seed")) %>%
      dplyr::filter(conference.x != conference.y) %>%
      dplyr::select(-conference.y) %>%
      dplyr::rename(
        home_team = team.x,
        away_team = team.y,
        home_team_seed = seed,
        home_team_conference = conference.x
      ) %>%
      # Add Performance Data
      dplyr::left_join(latest_performance_data, by = c("home_team" = "team")) %>%
      dplyr::left_join(latest_performance_data %>%
                         dplyr::rename(
                           opp_point_differential = point_differential,
                           opp_adjusted_off_epa = adjusted_off_epa,
                           opp_adjusted_def_epa = adjusted_def_epa), by = c("away_team" = "team")) %>%
      dplyr::mutate(location = "Neutral")

    # Predict and Simulate Divisional Round
    super_outcome <- super_matchup %>%
      dplyr::mutate(model_home_wp = caret::predict.train(newdata = super_matchup, object = model, type = "prob")[,2],
                    alt_model_home_wp_1 = dplyr::lag(model_home_wp),
                    alt_model_home_wp_2 = dplyr::lead(model_home_wp),
                    model_home_wp = ifelse(is.na(alt_model_home_wp_1), (model_home_wp+(1-alt_model_home_wp_2))/2, (model_home_wp+(1-alt_model_home_wp_1))/2)) %>%
      dplyr::mutate(outcome = stats::rbinom(n = nrow(super_matchup), size = 1, prob=model_home_wp)) %>%
      dplyr::filter(home_team_conference == "AFC")

    # Get Super Bowl Winner
    super_winner <- super_outcome %>%
      dplyr::mutate(team = ifelse(outcome == 1, home_team, away_team)) %>%
      dplyr::select(team)


    ### Apply outcomes to the standings
    team_wins <- team_wins %>%
      dplyr::mutate(division_round = ifelse(team %in% divisional_matchup$home_team | team %in% divisional_matchup$away_team, 1, 0),
                    conference_championship = ifelse(team %in% conference_matchup$home_team | team %in% conference_matchup$away_team, 1, 0),
                    super_bowl = ifelse(team %in% super_matchup$home_team | team %in% super_matchup$away_team, 1, 0),
                    league_winner = ifelse(team %in% super_winner$team, 1, 0))


    ### Complete Simulation
    message(paste("Simulation Number", simulations))
    return(team_wins)
  })

  ### Get Probabilities
  probabilties <- simulation_outcomes %>%
    dplyr::group_by(team) %>%
    dplyr::summarise(
      wins = mean(wins),
      division_winner = mean(division_winner),
      wild_card = mean(wild_card),
      playoff = mean(playoff),
      division_round = mean(division_round),
      conference_championship = mean(conference_championship),
      super_bowl = mean(super_bowl),
      league_winner = mean(league_winner)
    )

  return(probabilties)
}

#' Create a table for simulated Seasons
#' @param season_simulation_data: the output of the season simulation function
#' @return makes the table pretty!
#' @export
simulated_season_table <- function(season_simulation_data){

  ### Make Reactable Table
  table <- season_simulation_data %>%
    dplyr::mutate(wins = round(wins, 2)) %>%
    dplyr::mutate_if(is.numeric, round, 4) %>%
    dplyr::arrange(-league_winner) %>%
    reactable::reactable(
      compact = TRUE,
      borderless = FALSE,
      striped = FALSE,
      fullWidth = FALSE,
      defaultPageSize = 100,
      defaultColDef = reactable::colDef(
        align = "center",
        minWidth = 100),
      theme = reactable::reactableTheme(
        headerStyle = list(
          "&:hover[aria-sort]" = list(background = "hsl(0, 0%, 96%)"),
          "&[aria-sort='ascending'], &[aria-sort='descending']" = list(background = "hsl(0, 0%, 96%)"),
          borderColor = "#555")
      ),
      columns = list(
        team = reactable::colDef(name = "Team",
                           align = "left",
                           minWidth = 110),
        wins = reactable::colDef(name = "Wins"),
        wild_card = reactable::colDef(name = "Make Wild Card",
                           class = "border-left",
                           style = function(value) {
                             normalized <- (value - min(season_simulation_data$wild_card)) / (max(season_simulation_data$wild_card) - min(season_simulation_data$wild_card))
                             color <- rgb(colorRamp(c("#FFFFFF","#9933FF"))(normalized), maxColorValue = 255)
                             list(background = color)}),
        division_winner = reactable::colDef(name = "Win Division",
                                 class = "border-left",
                                 style = function(value) {
                                   normalized <- (value - min(season_simulation_data$division_winner)) / (max(season_simulation_data$division_winner) - min(season_simulation_data$division_winner))
                                   color <- rgb(colorRamp(c("#FFFFFF","#9933FF"))(normalized), maxColorValue = 255)
                                   list(background = color)}),
        playoff = reactable::colDef(name = "Make Playoffs",
                                class = "border-left",
                                style = function(value) {
                                  normalized <- (value - min(season_simulation_data$playoff)) / (max(season_simulation_data$playoff) - min(season_simulation_data$playoff))
                                  color <- rgb(colorRamp(c("#FFFFFF","#9933FF"))(normalized), maxColorValue = 255)
                                  list(background = color)}
                         ),
        division_round = reactable::colDef(name = "Make Divisional Round",
                                 class = "border-left",
                                 style = function(value) {
                                   normalized <- (value - min(season_simulation_data$division_round)) / (max(season_simulation_data$division_round) - min(season_simulation_data$division_round))
                                   color <- rgb(colorRamp(c("#FFFFFF","#9933FF"))(normalized), maxColorValue = 255)
                                   list(background = color)}
                                 ),
        conference_championship = reactable::colDef(name = "Make Conference Champ.",
                                class = "border-left",
                                style = function(value) {
                                  normalized <- (value - min(season_simulation_data$conference_championship)) / (max(season_simulation_data$conference_championship) - min(season_simulation_data$conference_championship))
                                  color <- rgb(colorRamp(c("#FFFFFF","#9933FF"))(normalized), maxColorValue = 255)
                                  list(background = color)}
                                ),
        super_bowl = reactable::colDef(name = "Make Super Bowl",
                                         class = "border-left",
                                         style = function(value) {
                                           normalized <- (value - min(season_simulation_data$super_bowl)) / (max(season_simulation_data$super_bowl) - min(season_simulation_data$super_bowl))
                                           color <- rgb(colorRamp(c("#FFFFFF","#9933FF"))(normalized), maxColorValue = 255)
                                           list(background = color)}
                            ),
        league_winner = reactable::colDef(name = "Win Super Bowl",
                            class = "border-left",
                            style = function(value) {
                              normalized <- (value - min(season_simulation_data$league_winner)) / (max(season_simulation_data$league_winner) - min(season_simulation_data$league_winner))
                              color <- rgb(colorRamp(c("#FFFFFF","#9933FF"))(normalized), maxColorValue = 255)
                              list(background = color)}
                            )
        )
      )

  return(table)

}

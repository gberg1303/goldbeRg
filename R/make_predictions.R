#' Create the Model
#' @import dplyr
#' @param year: the year in which the model will test, meaning the model will be based on data prior to that season.
#' @param dataset: this should be set to the dataset that is outputted by the create model function.
#' @param seed: seed for the model. pick a random number.
#' @return trained glm model
#' @export
create_nfl_model <- function(year, dataset, seed = 123){

  ### Create Model
  message(paste("Setting Seed and Creating Model"))
  set.seed(seed)
  Goldberg_Model <- caret::train(win ~
                            point_differential + adjusted_off_epa + adjusted_def_epa +
                            opp_point_differential + opp_adjusted_off_epa + opp_adjusted_def_epa +
                           #location
                              + stadium #+ location*stadium
                             + home_qb + away_qb
                               + home_qb*adjusted_off_epa + away_qb*opp_adjusted_off_epa
                            ,
                          data = dataset %>% mutate(win = as.factor(win), stadium = as.factor(stadium)) %>% filter(season < year),
                          method = 'glm',
                          family = "binomial",
                          preProc = c("scale"),
                          trControl = caret::trainControl(method = "repeatedcv", number = 10, repeats = 3))
  return(Goldberg_Model)

}

#' Predict the NFL
#' @import dplyr
#' @param years: the years for which you want to get predictions from nfl games
#' @param seed: seed for the model. pick a random number.
#' @return datatable with predictions for games
#' @export
generate_nfl_predictions <- function(years, seed = 123){

  ### Grab Model Dataset
  message(paste("Getting Dataset"))
  dataset <- create_nfl_modeldataset()

  ### Create Predictions
  message(paste("Mapping Years"))
  results <- purrr::map_df(years, function(years){

    # Create Model
    model <- create_nfl_model(year = years, dataset = dataset, seed = seed)

    # Generate Predictions
    predictions <- dataset %>%
      dplyr::filter(season == years) %>%
      dplyr::mutate(model_home_wp = caret::predict.train(newdata = dataset %>% dplyr::mutate(win = as.factor(win)) %>% dplyr::filter(season == years), object = model, type = "prob")[,2]) %>%
      dplyr::select(game_id, game_completed, win, home_team, away_team, model_home_wp)

    return(predictions)
  })
  return(results)

}


#' Help Simulate the NFL
#' @import dplyr
#' @param years: the years for which you want to get predictions from nfl games
#' @param seed: seed for the model. pick a random number.
#' @return datatable with predictions for games. A team's epa is randomly generated from a normal distribution of their last ten games.
generate_simulation_predictions <- function(years, seed = 123){

  ### Grab Model Dataset
  message(paste("Getting Dataset"))
  dataset <- create_nfl_modeldataset()

  ### Create Predictions
  message(paste("Mapping Years"))
  results <- purrr::map_df(years, function(years){

    # Create Model
    model <- create_nfl_model(year = years, dataset = dataset, seed = seed)

    # Generate Predictions
    predictions <- dataset %>%
      dplyr::filter(season == years) %>%
      # Generate new statistics from normal distribution of their last ten games
      dplyr::mutate(
        point_differential = rnorm(1, mean = point_differential, sd = point_differential_sd),
        adjusted_off_epa = rnorm(1, mean = adjusted_off_epa, sd = adjusted_off_epa_sd),
        adjusted_def_epa = rnorm(1, mean = adjusted_def_epa, sd = adjusted_def_epa_sd)) %>%
      # Model predicts
      dplyr::mutate(model_home_wp = caret::predict.train(newdata = dataset %>% dplyr::mutate(win = as.factor(win)) %>% dplyr::filter(season == years), object = model, type = "prob")[,2]) %>%
      dplyr::select(game_id, game_completed, win, home_team, away_team, model_home_wp)

    return(predictions)
  })
  return(results)

}

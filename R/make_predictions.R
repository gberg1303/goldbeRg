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
                              location,
                          data = dataset %>% mutate(win = as.factor(win)) %>% filter(season < year),
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


  message(paste("Mapping Years"))
  results <- purrr::map_df(years, function(years){
  ### Create Model
  model <- create_nfl_model(year = years, dataset = dataset, seed = seed)

  ### Generate Predictions
  predictions <- dataset %>%
    dplyr::filter(season == years) %>%
    dplyr::mutate(model_home_wp = caret::predict.train(newdata = dataset %>% dplyr::mutate(win = as.factor(win)) %>% dplyr::filter(season == years), object = model, type = "prob")[,2]) %>%
    dplyr::select(game_id, home_team, away_team, model_home_wp)

  return(predictions)
  })
  return(results)

}

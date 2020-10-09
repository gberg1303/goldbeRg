### OLD Playoffs
dplyr::group_by(division) %>%
  dplyr::mutate(
    division_rank = rank(-wins, ties.method = "random"),
    division_winner = ifelse(division_rank == 1, 1, 0)) %>%
  dplyr::group_by(conference, division_winner) %>%
  dplyr::mutate(
    rest_conference_rank = rank(-wins, ties.method = "random")) %>%
  dplyr::ungroup() %>%
  dplyr::mutate(
    wild_card = ifelse(division_winner == 0 & rest_conference_rank == 1 | division_winner == 0 & rest_conference_rank == 2, 1, 0),
    playoff = ifelse(division_winner == 1 | wild_card == 1, 1, 0)
  ) %>%
  dplyr::select(-rest_conference_rank, -division_rank)



#####

### Add QB Ratings to Dataset
Model_Dataset_New <- Model_Dataset %>%
  # Add Home QB
  dplyr::left_join(
    starters %>% dplyr::ungroup() %>% dplyr::select(game_id, posteam, Composite) %>% dplyr::rename(home_qb = Composite),
    by = c("home_team" = "posteam", "game_id")
  ) %>%
  # Add Away QB
  dplyr::left_join(
    dplyr::starters %>% dplyr::ungroup() %>% dplyr::select(game_id, posteam, Composite) %>% dplyr::rename(away_qb = Composite),
    by = c("away_team" = "posteam", "game_id")
  )

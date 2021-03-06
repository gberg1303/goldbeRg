% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/make_predictions.R
\name{generate_simulation_predictions}
\alias{generate_simulation_predictions}
\title{Help Simulate the NFL}
\usage{
generate_simulation_predictions(years, seed = 123)
}
\arguments{
\item{years:}{the years for which you want to get predictions from nfl games}

\item{seed:}{seed for the model. pick a random number.}
}
\value{
datatable with predictions for games. A team's epa is randomly generated from a normal distribution of their last ten games.
}
\description{
Help Simulate the NFL
}

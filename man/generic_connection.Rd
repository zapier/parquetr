% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/generic_connection.R
\name{generic_connection}
\alias{generic_connection}
\title{A setter/builder for generic connections}
\usage{
generic_connection(connection_name, build_connection_fun, test_connection_fun)
}
\arguments{
\item{connection_name}{The name at which to store the connection in .package_env}

\item{build_connection_fun}{A function with no arguments to build the connection}

\item{test_connection_fun}{A function with the connection object
(as stored as an argument to test the connection).
Must return TRUE if the connection is valid and FALSE if it is invalid.
If it is invalid, the connection will be rebuilt.}
}
\value{
The connection object
}
\description{
A setter/builder for generic connections
}

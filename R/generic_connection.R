#' A setter/builder for generic connections
#'
#' @param connection_name The name at which to store the connection in .package_env
#' @param build_connection_fun A function with no arguments to build the connection
#' @param test_connection_fun A function with the connection object
#' (as stored as an argument to test the connection).
#' Must return TRUE if the connection is valid and FALSE if it is invalid.
#' If it is invalid, the connection will be rebuilt.
#'
#' @return The connection object
generic_connection <- function(connection_name, build_connection_fun, test_connection_fun) {
  function() {
    if (identical(try(test_connection_fun(get(connection_name, .package_env)), silent = TRUE), TRUE)) {
      connection_object <- get(connection_name, envir = .package_env)
    } else {
        connection_object <- build_connection_fun()
        assign(connection_name, connection_object, envir = .package_env)
    }
    return(connection_object)
  }
}
NULL

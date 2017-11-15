.package_env <- new.env(parent = emptyenv())

#' Log to stdout if set to verbose
#'
#' @param ... Args passed to message
#'
#' @return None, side-effect only
log_if_verbose <- function(...) {
  if (isTRUE(getOption("parquetr.verbose"))) {
    message("parquetr: ", ...)
  }
}
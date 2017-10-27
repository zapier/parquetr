#' Identify Corresponding Spark Types
#'
#' @param .data \code{data.frame}
#'
#' @return character vector
#' @export
#'
#' @importFrom glue glue
#' @importFrom dplyr coalesce recode
identify_spark_types <- function (.data)
{
  classes <- lapply(.data, class)
  classes_first_pass <- lapply(classes, function(x) {
    if (all(c("POSIXct", "POSIXt") %in% x)) {
      x <- "timestamp"
    }
    return(x)
  })
  if (any("factor" %in% classes_first_pass)) {
    warning("one of the columns is a factor")
  }
  # yes, the letter case does seem to be important
  data_types <- recode(unlist(classes_first_pass), factor = "character",
                       numeric = "numeric", integer = "integer", integer64 = "BIGINT", character = "character",
                       logical = "boolean", Date = "date")
  return(data_types)
}

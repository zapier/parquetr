#' R6 Object to wrap Spark connection
#'
#' @export
#' @importFrom R6 R6Class
#' @importFrom glue glue
#' @importFrom sparklyr spark_install_find spark_config spark_write_parquet spark_read_parquet spark_connect spark_read_csv connection_is_open
#' @importFrom magrittr "%>%"
#' @importFrom dplyr collect first pull select
#' @importFrom purrr map
#' @importFrom uuid UUIDgenerate
#' @importFrom readr write_csv
#' @importFrom rlang quo_text sym "!!"
#' @importFrom zapieR s3_accessibility_layer
#' @importFrom DBI dbRemoveTable
#'
#' @docType class
#' @keywords data
#' @return Object of \code{\link{R6Class}} with methods for reading/writing Parquet on S3.
#' @format \code{\link{R6Class}} object.
#' @examples \dontrun{
#' temp_table <- uuid::UUIDgenerate()
#' local_spark <- sparkConnection$new()
#' local_spark$write_parquet(iris, temp_table)
#' local_spark$read_parquet(temp_table)
#' local_spark$delete_parquet(temp_table)
#' }
#' @field bucket A R6S3 function that generates a target bucket
#' @field df A data.frame to process
#' @field location \code{character} Of the location relative to the bucket root (allowing for the root prefix of spark-storage) to interact with
#' @section Methods:
#' \describe{
#'   \item{\code{new(bucket)}}{This method is used to create object of this class with \code{bucket} as the target bucket.}
#'   \item{\code{write_parquet(df, location, mode = "overwrite", ...)}}{This method is used to create object of this class with \code{bucket} as the target bucket.}
#' }

Parquetr <- R6Class(
  "Parquetr",
  public = list(
    initialize = function(bucket, master = "local", config = NULL) {
      Sys.setenv(SPARK_HOME = sparklyr:::spark_install_find()$sparkVersionDir)
      if (is.null(config)) {
        config <- sparklyr::spark_config()
        config$`sparklyr.shell.driver-memory` <- paste0(trunc(systemRAMFree()-1), "G")
        #config$`sparklyr.shell.executor-memory` <- "4G"
        #config$`spark.yarn.executor.memoryOverhead` <- "1G"
        config[["sparklyr.defaultPackages"]] <- "org.apache.hadoop:hadoop-aws:2.7.3"
      }
      private$master <- master
      private$config <- config
      private$spark_connection <- private$connect()
      private$bucket <- generic_connection(
        connection_name = UUIDgenerate(),
        test_connection_fun = test_connection_s3,
        build_connection_fun =
          function() s3_accessibility_layer$new(
              bucket = bucket()$bucket,
              key_prefix = glue("{bucket()$key_prefix}spark-storage/")
            )
      )()
    },
    write_parquet = function(df, location, mode = "overwrite", ...) {
      log_if_verbose("Writing parquet to ", location)
      # @param location character ; just the name not the s3 addy
      # current issue with columns that have newlines in presence of date column
      # https://github.com/rstudio/sparklyr/issues/1020
      character_columns <- which(lapply(df, class) == "character")
      df[, character_columns] <- lapply(df[, character_columns], function(x) {
        gsub("\n", "", x)
      })
      # Date doesn't serialize correctly, other timestamp bits seem affected as well: https://github.com/rstudio/sparklyr/issues/941
      # datetime_columns <- which(lapply(df, function(x) {first(class(x))}) %in% c("Date", "POSIXct", "POSIXlt"))
      # df[, datetime_columns] <- lapply(df[, character_columns], function(x) {as.character(x)})
      temp_loc <- gsub("-", "", UUIDgenerate())
      self$write_csv(df, temp_loc)
      types <- identify_spark_types(df)
      names(types) <- names(df)
      log_if_verbose("Reading csv into Spark from ", self$s3a_url(paste0("csv/", temp_loc)))
      df_spark <- spark_read_csv(sc = self$sc, name = temp_loc, path = self$s3a_url(paste0("csv/", temp_loc)), columns = types, infer_schema = FALSE)
      log_if_verbose("Writing Parquet from Spark to S3 ", self$s3a_url(location))
      spark_write_parquet(df_spark, self$s3a_url(location), mode = mode, ...)

      # Clean-up
      log_if_verbose("Cleaning up from write_parquet()")
      sparklyr::tbl_uncache(self$sc, temp_loc) # uncache the table
      dbRemoveTable(self$sc, temp_loc) # remove temp table made for spark_read_csv from the database (might be on disk otherwise)
      rm(df_spark) # clear representation, just in case going out of scope isn't good enough
      self$delete_csv(temp_loc) # delete the temp record we made on S3
    },
    write_parquet_partition = function(df, location, partition) {
      unique_entries_for_partition <- private$get_partition_value(df, partition)
      self$write_parquet(df, self$partition_location(location, partition, unique_entries_for_partition), mode = "overwrite")
    },
    read_parquet = function(name, ...) {
      spark_read_parquet(self$sc, private$spark_name(name), self$s3a_url(name), ...) %>% collect(n = Inf)
    },
    read_csv = function(name, columns = NULL, ...) {
      # TODO: A read that immediately follows a write could set infer_schema to false to speed reading back into Spark
      name <- as.character(glue("csv/{name}"))
      spark_read_csv(sc = self$sc, name = private$spark_name(name), self$s3a_url(name), null_value = "", columns = columns, ...) %>% collect(n = Inf)
    },
    delete_parquet = function(name) {
      private$bucket$ls(name)$filename %>%
        map(~ private$bucket$rm(.x))
    },
    write_csv = function(d, location) {
      location <- glue("csv/{location}")
      temp_file <- tempfile()
      log_if_verbose("Writing local disk csv to ", temp_file)
      write_csv(x = d, path = temp_file, na = "")
      log_if_verbose("Uploading local disk csv to s3 ", location)
      private$bucket$set_file(location, temp_file)
      unlink(temp_file)
    },
    delete_csv = function(location) {
      location <- glue("csv/{location}")
      private$bucket$rm(location)
    },
    s3a_url = function(location) {
      private$bucket$s3a_url(location)
    },
    s3_url = function(location) {
      private$bucket$s3_url(location)
    },
    partition_location = function(location, partition, value) {
      # Build a text string to specify a partition location
      stopifnot(is.atomic(location))
      stopifnot(is.atomic(partition))
      stopifnot(is.atomic(value))
      glue("{location}/{partition}={value}")
    },
    # Object finalizer
    finalize = function() {
      try(spark_disconnect(private$spark_connection), silent = TRUE)
    }
  ),
  active = list(
    sc = function() {
      if (!is.null(private$sc) && spark_connection_is_open(private$sc)) {
        private$spark_connection
      } else {
        warning("Spark context lost, creating a new context")
        private$spark_connection <- private$connect()
      }

    }
  ),
  private = list(
    spark_connection = NULL,
    config = NULL,
    master = NULL,
    get_partition_value = function(df, partition) {
      df %>%
        select(!! partition) %>%
        pull(!! partition) %>%
        unique()
    },
    connect = function() {
      sparklyr::spark_connect(master = private$master, config = private$config)
    },
    bucket = NULL,
    spark_name = function(name) {
      name %>%
        # Spark 2.1 doesn't like hyphens in the object names
        # Spark 2.1 doesn't like forward slashes in the object names
        # ... it is kind of like R in that wake
        make.names() %>%
        gsub(".", "_", .)
    },
    csv_name = function(location) {
      paste0("csv/", location)
    }
  )
)

systemRAMFree <- function() {
  #in GB, platform dependent
  as.numeric(system('FREE_KB=$(($(echo `sed -n \'2p;3p;4p\' <  /proc/meminfo | sed "s/ \\+/ /g" | cut -d\' \' -f 2 ` | sed "s/ /+/g")));echo $FREE_KB', intern=TRUE))/1024/1024
}

context("Not otherwise specified tests")

unlink(list.files(".", pattern = "log4j.spark.log",recursive = TRUE, full.names = TRUE))

test_that("UUID can be generated; a failure of this test indicates someone forgot to install package:uuid", {
  temp_table <- uuid::UUIDgenerate()
  expect_type(temp_table, "character")
})

temp_table <- uuid::UUIDgenerate()

test_that("Spark connection can be generated", {
  local_spark <- Parquetr$new(zapieR::data_science_storage_s3)
  expect_true(is.environment(local_spark))
})

local_spark <- Parquetr$new(zapieR::data_science_storage_s3)

demo_loc <- paste0("deleteme", gsub("-", "", uuid::UUIDgenerate()))
d1 <- data.frame(id = 1:3, date = lubridate::today()-lubridate::days(1), value = rnorm(3))
d2 <- data.frame(id = 1:3, date = lubridate::today(), value = rnorm(3))
d3 <- data.frame(id = 1:3, date = lubridate::today()-lubridate::days(2), value = rnorm(3))

test_that("Everything works", {
  # I do one mega pass on everything to make sure that subsequent test passes aren't because the connection object was generated twice
  # clear the workstapce
  local_spark$delete_parquet(temp_table)
  # table shouldn't exist
  expect_error(local_spark$read_parquet(temp_table), "Path does not exist")
  # write the table
  suppressWarnings(local_spark$write_parquet(iris, temp_table))
  # read the table, should be about the same - we skip the column Species because we know Spark looses that as a factor
  expect_equivalent(iris[,1:4], local_spark$read_parquet(temp_table)[, 1:4])
  # delete the table
  local_spark$delete_parquet(temp_table)$result
  # expect that table no longer exists
  expect_error(local_spark$read_parquet(temp_table), "Path does not exist")
})

test_that("Workspace clearing for additional testing", {
  # clear the workstapce
  local_spark$delete_parquet(temp_table)
  # table shouldn't exist
  expect_error(local_spark$read_parquet(temp_table), "Path does not exist")
})

test_that("Written Parquet table can be written and read", {
  suppressWarnings(local_spark$write_parquet(iris, temp_table))
  # read the table, should be about the same - we skip the column Species because we know Spark looses that as a factor
  expect_equivalent(iris[,1:4], local_spark$read_parquet(temp_table)[, 1:4])
})

test_that("A deleted table can't be read", {
  # delete the table
  local_spark$delete_parquet(temp_table)$result
  # expect that table no longer exists
  expect_error(local_spark$read_parquet(temp_table), "Path does not exist")
})

test_that("csv handling", {
  test_location <- paste0("deleteme/", uuid::UUIDgenerate())
  local_spark$delete_csv(test_location)
  expect_error(local_spark$read_csv(test_location, "Path does not exist"))
  local_spark$write_csv(d = cars, location = test_location)
  expect_equivalent(cars, local_spark$read_csv(test_location))
})

# Speed testing at various sizes always yielded an advantage to copy_to
# However, copy_to has some serialization bugs for date/timestamps.
# library(zapieR)
# library(sparklyr)
# library(dplyr)
# library(lubridate)
# local_spark <- sparkConnection$new()
# test_location <- paste0("deleteme/", uuid::UUIDgenerate())
# small_dat <- cars
# small_dat$date <- now()
# #bigger_dat <- replicate(1000000, iris, simplify = FALSE) %>% bind_rows
#
# tic()
# local_spark$write_csv(d = small_dat, location = test_location)
# spark_csv_file <- spark_read_csv(local_spark$sc, "moo", local_spark$.__enclos_env__$private$s3a(paste0("csv/",test_location)))
# toc()
# tbl(local_spark$sc, "moo")
#
# tic()
# test_location <- paste0("deleteme/", uuid::UUIDgenerate())
# copy_to(local_spark$sc, small_dat, name = "cars", overwrite = TRUE)
# spark_df <- tbl(local_spark$sc, "cars")
# toc()

unlink(list.files(".", pattern = "log4j.spark.log",recursive = TRUE, full.names = TRUE))
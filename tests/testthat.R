library(testthat)
library(parquetr)
library(lubridate)

test_check("parquetr")
unlink(list.files(".", pattern = "log4j.spark.log",recursive = TRUE, full.names = TRUE))

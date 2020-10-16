if (file.exists("/tmp/rzapierci.tar.gz")) {
  # if we were able to download the CI bootstrapper, install it
  install.packages("/tmp/rzapierci.tar.gz", repos = NULL, type = 'source')
}

if (read.dcf("DESCRIPTION")[, "Package"] == "rzapierci") {
  built_file <- devtools::build()
  # We don't need to install our newly built package because this was part of devtools::check()
  # but, if we're rzapierci we want to do it anyway to be sure we're bootstrapping with this version of things
  install.packages(built_file, repos = NULL, type = 'source')
}
library(rzapierci)
build_and_release_package()

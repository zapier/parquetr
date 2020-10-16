# Anything that needs to be run as a bash script prior to doing R CMD CHECK
# You are already 'root' on the target image

# R needs the timezone for {devtools}
echo TZ=Etc/UTC >> ~/.Renviron # set time zone

curl "https://s3.amazonaws.com/aws-cli/awscli-bundle.zip" -o "awscli-bundle.zip"
unzip awscli-bundle.zip
./awscli-bundle/install -i /usr/local/aws -b /usr/local/bin/aws

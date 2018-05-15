#!/usr/bin/env bash

target="$1"
## this script will sync the project to the remote server
rsync -i -rtuv $PWD/../ $target:~/projects/nocc  --exclude ./pre-data/
#rsync -e "ssh -i ../aws/tp.pem"  -rtuv $PWD/../ $target:~/nocc  --exclude ./pre-data/

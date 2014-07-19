#!/bin/bash

FILES=~/workspace/scrapy/nhldata/data/skaters/*.csv
for f in $FILES
do
echo "file = $f"
sudo -u postgres psql -d nhldata -c "copy skaters from '$f' csv header delimiter as ',';"
done

exit

#!/bin/bash
if [ $# -eq 0 ]; then
    echo "No date argument given. Format: YYYYMMDD"
    exit 1
fi
arg1=$1
year=${arg1:0:4}
month=${arg1:4:2}
day=${arg1:6:2}
curl https://dumps.wikimedia.org/other/pageview_complete/${year}/${year}-${month}/pageviews-${arg1}-user.bz2 -o pageview_${arg1}.bz2 && bzip2 -d pageview_${arg1}.bz2

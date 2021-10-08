#!/usr/bin/bash
if [ $# -eq 0 ]; then
    echo "No date argument given. Format: YYYYMMDD"
    echo "This data is not updated as often. Here are the possible values:"
    echo "Sorry for the bad formatting"
    curl -s https://dumps.wikimedia.org/nowiki/ | grep href
    exit 1
fi
arg1=$1
curl https://dumps.wikimedia.org/nowiki/${arg1}/nowiki-${arg1}-page.sql.gz -o page_${arg1}.sql.gz && gzip -d page_${arg1}

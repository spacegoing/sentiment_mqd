#!/bin/bash
root=`pwd`
UtilsDir=$root/Utils
ExClassDir=$root/ExchangeClass
ParentSpiderDir=$root/market_events_spiders
SpiderDir=$root/market_events_spiders/spiders

find . -type l -delete

ln -s $UtilsDir/market_id.csv $root/market_id.csv
ln -s $UtilsDir $ExClassDir
ln -s $UtilsDir $ParentSpiderDir
ln -s $UtilsDir $SpiderDir
ln -s $UtilsDir/market_id.csv $SpiderDir
ln -s $ExClassDir $SpiderDir
ln -s $root/csv_upload_template.csv $SpiderDir

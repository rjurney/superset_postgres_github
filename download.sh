#!/usr/bin/env bash

# Fetch one year of Github Archive events, starting in  and going to June 2019
wget -nc -P data/ http://data.gharchive.org/2011-{01..12}-{01..31}-{0..23}.json.gz
wget -nc -P data/ http://data.gharchive.org/2012-{01..12}-{01..31}-{0..23}.json.gz
wget -nc -P data/ http://data.gharchive.org/2013-{01..12}-{01..31}-{0..23}.json.gz
wget -nc -P data/ http://data.gharchive.org/2014-{01..12}-{01..31}-{0..23}.json.gz
wget -nc -P data/ http://data.gharchive.org/2015-{01..12}-{01..31}-{0..23}.json.gz
wget -nc -P data/ http://data.gharchive.org/2016-{01..12}-{01..31}-{0..23}.json.gz
wget -nc -P data/ http://data.gharchive.org/2017-{01..12}-{01..31}-{0..23}.json.gz
wget -nc -P data/ http://data.gharchive.org/2018-{01..12}-{01..31}-{0..23}.json.gz
wget -nc -P data/ http://data.gharchive.org/2019-{01..06}-{01..31}-{0..23}.json.gz

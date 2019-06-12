#!/usr/bin/env bash

# Fetch one year of Github Archive events, starting in March 2018 and going to March 2019
wget -P data/ http://data.gharchive.org/2018-{04..08}-{01..31}-{0..23}.json.gz
wget -P data/ http://data.gharchive.org/2018-{09..12}-{01..31}-{0..23}.json.gz
wget -P data/ http://data.gharchive.org/2019-{01..03}-{01..31}-{0..23}.json.gz

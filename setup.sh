#!/bin/sh
rm *.json
rm -rf db_folder
python3 nba_data.py
mkdir db_folder
mongod --port $1 --dbpath ./db_folder --quiet &
python3 dataCollection.py $1
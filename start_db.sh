#!/bin/sh
mysqld --datadir=$HOME/mysql_data --socket=$HOME/mysql.sock --local-infile=1 --port=3307 &
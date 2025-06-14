#!/bin/sh
mysqld --datadir=$HOME/mysql/data --socket=$HOME/mysql.sock --local-infile=1 --port=3307 --innodb-buffer-pool-size=1G &
#!/bin/sh
export PATH=$HOME/local/mysql/bin:$PATH
export MYSQL_HOME=$HOME/local/mysql
mysqld --initialize-insecure --datadir=$HOME/mysql/data --socket=$HOME/mysql.sock --local-infile=1 --port=3307

#!bin/bash

DATADIR="$HOME/mysql/data"
SOCKET="$HOME/mysql.sock"
SQLUSER="root"
DB_NAME="ustca"
TABLE_NAME="observation"
FOLDER="$HOME/data/observations"


case "$1" in
  startdb)
    echo 'startdb'
    mysqld --datadir=$DATADIR --socket=$SOCKET --local-infile=1 --port=3307 --innodb-buffer-pool-size=1G &
    ;;
  stopdb)
    echo 'stopdb'
    mysqladmin --socket="$SOCKET" -u $SQLUSER shutdown
    ;;
  load)
    echo 'load'
    echo 'Create tables'
    echo -n "Proceed? [y/n]: "
    read -r ans
    if [[ "$ans" == 'y' ]] || [[ "$ans" = 'Y' ]]; then
        echo 'ARE YOU SURE?'
        echo -n 'WARNING: DOING THIS WILL DELETE ALL EXISTING DATA [y\n]: '
        read -r ans
        if [[ "$ans" == 'y' ]] || [[ "$ans" = 'Y' ]]; then
            mysql --local-infile=1 < sql/load_data.sql
            echo 'created tables'
        fi
    fi
    
    echo 'Load observations'
    echo -n "Could take up to 1hr. Proceed? [y/n]: "
    read -r ans
    if [[ "$ans" == 'y' ]] || [[ "$ans" = 'Y' ]]; then
        echo -n 'Confirm? [y\n]: '
        read -r ans
        if [[ "$ans" == 'y' ]] || [[ "$ans" = 'Y' ]]; then

            for file in "$FOLDER"/*.csv; do
                echo "Loading $file..."
                mysql --local-infile=1 $DB_NAME -e "
                LOAD DATA LOCAL INFILE '$file'
                INTO TABLE $TABLE_NAME
                FIELDS TERMINATED BY ',' 
                OPTIONALLY ENCLOSED BY '\"'
                LINES TERMINATED BY '\n'
                IGNORE 1 LINES
                (@id, @element, @value, @date)
                SET
                id      = @id,
                element = @element,
                data    = NULLIF(@value, ''),
                date    = @date;
                "
            done

            mysql $DB_NAME -e "
            DELETE FROM $TABLE_NAME where date = 0;
            "

            echo 'loaded observations'
        fi
    fi
    ;;
  *)
    echo "Usage: source dbinterface.sh {startdb|stopdb|load}"
    ;;
esac


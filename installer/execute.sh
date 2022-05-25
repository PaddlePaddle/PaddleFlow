#!/bin/bash
set -ex

work_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

echo $work_dir

source $work_dir/config.sh

source tools/functions


function echo_input_tips() {
    red_echo "ERROR: You should input params as: "
    echo -e "\tinstall"
    echo -e "\tuninstall"
}


function execute_component() {
    cd $work_dir/helm
    if [ $1 = "uninstall" ]; then
      green_echo "Begin to execute uninstall"
      sh execute_uninstall.sh
    elif [ $1 = "install" ]; then
      green_echo "Begin to execute install server"
      sh $work_dir/charts/execute.sh
      sh execute.sh
    fi
}

function db_init() {
  if [ $DATABASE_SQL == "true" ];then
    green_echo "Initialize the database······"
    cd database/
    sh execute.sh
  fi
}

function db_dump() {
  time=`date "+%Y%m%d%H%M%S"`
  if [ $DB_DRIVER == "mysql" ];then
    mysqldump -u$DB_USER -h$DB_HOST -p$DB_PW -P$DB_PORT --databases $DB_DATABASE --set-gtid-purged=off>mysql_dump_${time}.sql
  elif [ $DB_DRIVER == "postgres" ];then
    echo "not implement"
  fi
}

function db_uninstall() {
  if [ $DB_DRIVER == "mysql" ];then
    db_dump
    mysql -u$DB_USER -h$DB_HOST -p$DB_PW -P$DB_PORT -e "drop database if exists ${DB_DATABASE};"
  elif [ $DB_DRIVER == "postgres" ];then
    echo "not implement"
  fi
}

# parse flag
if [ $1 = "install" ]; then
  db_init
  execute_component $1
elif [ $1 = "uninstall" ];then
  db_uninstall
  execute_component $1
else
  echo_input_tips
  exit -1
fi

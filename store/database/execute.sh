#bin/bash
set -ex

if [ $DB_DRIVER == "mysql" ];then
  mysql -u$DB_USER -h$DB_HOST -p$DB_PW -P$DB_PORT -e "use $DB_DATABASE" &>/dev/null
  if [ $? -ne 0 ]
  then
   echo "MySQL database $DB_DATABASE is not exist"
  else
   echo "MySQL database $DB_DATABASE is exist, starting backup."
   mysqldump -u$DB_USER -h$DB_HOST -p$DB_PW -P$DB_PORT --databases $DB_DATABASE >  $DB_DATABASE.bak_`date +%Y%m%d`.sql
   drop database if exists DB_DATABASE;
  fi
  echo "creating database $DB_DATABASE."
  mysql -u$DB_USER -h$DB_HOST -p$DB_PW -P$DB_PORT $DB_DATABASE -e "source paddleflow.sql"
  echo "creating database $DB_DATABASE completed."

elif [ $DB_DRIVER == "postgres" ];then
  echo "not implement"
fi
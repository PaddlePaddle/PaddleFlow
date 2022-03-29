#bin/bash
set -ex

if [ $DB_DRIVER == "mysql" ];then
  sed -i "s/DB_DATABASE/$DB_DATABASE/g" init.sql
  mysql -u$DB_USER -h$DB_HOST -p$DB_PW -P$DB_PORT -e "source init.sql"
  mysql -u$DB_USER -h$DB_HOST -p$DB_PW -P$DB_PORT $DB_DATABASE -e "source apiserver.sql"
  mysql -u$DB_USER -h$DB_HOST -p$DB_PW -P$DB_PORT $DB_DATABASE -e "source filesystem.sql"
elif [ $DB_DRIVER == "postgres" ];then
  echo "not implement"
fi
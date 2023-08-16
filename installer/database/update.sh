#!/bin/bash
set -x
export DB_HOST="主机名"
export DB_DRIVER=mysql #数据库类型
export DB_PORT= #端口
export DB_USER=postgres #用户
export DB_PW= #密码
export DB_DATABASE=paddleflow #数据库名称
export PATH=/usr/pgsql-10/bin:/usr/bin; #二进制文件地址
OLD_PF_VERSION=143 # 原来的PaddleFlow版本
NEW_PF_VERSION=145 # 升级后的PaddleFlow版本

# 数据库升级sql，支持的版本 1.4.3-1.4.6
cat <<EOF > update_143_to_144.sql
ALTER TABLE job MODIFY extension_template mediumtext DEFAULT NULL;
EOF
cat <<EOF > update_144_to_145.sql
ALTER TABLE run MODIFY name varchar(128) NOT NULL;
ALTER TABLE run ADD COLUMN failure_options_json text NOT NULL after disabled;
ALTER TABLE pipeline MODIFY  name varchar(128) NOT NULL;
ALTER TABLE fs_cache_config ADD COLUMN clean_cache tinyint(1) NOT NULL default 0 COMMENT 'whether clean cache after mount pod vanishes' after debug;
ALTER TABLE fs_cache_config ADD COLUMN resource text COMMENT 'resource limit for mount pod' after clean_cache;
EOF
cat <<EOF > update_145_to_146.sql
ALTER TABLE job_task ADD COLUMN annotations text DEFAULT NULL after message;
EOF


if [[ $DB_DRIVER == "mysql" ]];then
  mysql -u$DB_USER -h$DB_HOST -p$DB_PW -P$DB_PORT -e "use $DB_DATABASE" &>/dev/null
  if [ $? -eq 0 ];then
   echo "MySQL database $DB_DATABASE is exist, starting update."
   mysqldump -u$DB_USER -h$DB_HOST -p$DB_PW -P$DB_PORT --databases $DB_DATABASE >  $DB_DATABASE.bak_`date +%Y%m%d`.sql
   echo "" > update.sql
   if [[ $OLD_PF_VERSION -eq 143 &&  $NEW_PF_VERSION -gt 143 ]];then
      [[ $NEW_PF_VERSION -ge 144 ]] && cat update_143_to_144.sql > update.sql
      [[ $NEW_PF_VERSION -ge 145 ]] && cat update_144_to_145.sql >> update.sql
      [[ $NEW_PF_VERSION -ge 146 ]] && cat update_145_to_146.sql  >> update.sql
   fi
   if [[ $OLD_PF_VERSION -eq 144 && $NEW_PF_VERSION -gt 144 ]];then
       [[ $NEW_PF_VERSION -ge 145 ]] && cat update_144_to_145.sql > update.sql
       [[ $NEW_PF_VERSION -ge 146 ]] && cat update_145_to_146.sql >> update.sql
   fi
   if [[ $OLD_PF_VERSION -eq 145 && $NEW_PF_VERSION -gt 146 ]];then
       cat update_145_to_146.sql > update.sql
   fi
   mysql -u$DB_USER -h$DB_HOST -p$DB_PW -P$DB_PORT $DB_DATABASE -e "source update.sql"
  fi
elif [[ $DB_DRIVER == "postgresql" ]];then
  echo "not implement postgresql update script"
fi
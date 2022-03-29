CREATE DATABASE IF NOT EXISTS paddleflow;
CREATE TABLE IF NOT EXISTS `filesystem` (
  `pk` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'pk',
  `id` varchar(36) NOT NULL COMMENT 'id',
  `name` varchar(200) NOT NULL,
  `server_address` varchar(1024) NOT NULL,
  `type` varchar(50) NOT NULL COMMENT 'file system type',
  `subpath` varchar(1024) NOT NULL COMMENT 'subpath',
  `user_name` varchar(256) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_at` datetime NOT NULL,
  `properties` TEXT,
  PRIMARY KEY (`pk`),
  UNIQUE KEY (`id`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `client` (
  `pk` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'pk',
  `id` varchar(36) NOT NULL COMMENT 'id',
  `fs_id` varchar(36) NOT NULL,
  `address` varchar(1024) NOT NULL,
  `state` varchar(36) NOT NULL Default 'active' COMMENT 'state',
  `created_at` datetime NOT NULL,
  `updated_at` datetime NOT NULL,
  PRIMARY KEY (`pk`),
  UNIQUE KEY (`id`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `link` (
    `pk` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'pk',
    `id` varchar(36) NOT NULL COMMENT 'id',
    `fs_id` varchar(36) NOT NULL,
    `fs_path` varchar(1024) NOT NULL,
    `server_address` varchar(1024) NOT NULL,
    `type` varchar(50) NOT NULL COMMENT 'file system type',
    `subpath` varchar(1024) NOT NULL COMMENT 'subpath',
    `user_name` varchar(256),
    `created_at` datetime NOT NULL,
    `updated_at` datetime NOT NULL,
    `properties` TEXT,
    PRIMARY KEY (`pk`),
    UNIQUE KEY (`id`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8 COMMENT='file system';
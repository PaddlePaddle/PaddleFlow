CREATE DATABASE IF NOT EXISTS paddleflow;

CREATE TABLE IF NOT EXISTS `queue` (
    `pk` bigint(20) NOT NULL AUTO_INCREMENT,
    `id` varchar(60) NOT NULL UNIQUE,
    `name` varchar(60) NOT NULL UNIQUE,
    `namespace` varchar(50) NOT NULL,
    `cluster_id` varchar(255) NOT NULL DEFAULT '',
    `cpu` varchar(20) NOT NULL,
    `mem` varchar(20) NOT NULL,
    `scalar_resources` varchar(255) DEFAULT NULL,
    `min_resources` varchar(255) DEFAULT NULL,
    `max_resources` varchar(255) DEFAULT NULL,
    `location` text DEFAULT '',
    `quota_type` varchar(20) DEFAULT NULL,
    `status` varchar(20) DEFAULT NULL,
    `scheduling_policy` varchar(2048) DEFAULT NULL,
    `created_at` datetime(3) DEFAULT NULL,
    `updated_at` datetime(3) DEFAULT NULL,
    `deleted_at` datetime(3) DEFAULT NULL,
    PRIMARY KEY (`pk`),
    UNIQUE KEY `queue_name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `job` (
    `pk` bigint(20) NOT NULL AUTO_INCREMENT,
    `id` varchar(60) NOT NULL UNIQUE,
    `user_name` varchar(60) NOT NULL,
    `type` varchar(20) NOT NULL,
    `config` mediumtext NOT NULL,
    `queue_id` varchar(36) NOT NULL,
    `runtime_info` mediumtext DEFAULT NULL,
    `status` varchar(32) DEFAULT NULL,
    `message` text DEFAULT NULL,
    `created_at` datetime(3) DEFAULT NULL,
    `activated_at` datetime(3) DEFAULT NULL,
    `updated_at` datetime(3) DEFAULT NULL,
    `deleted_at` datetime(3) DEFAULT NULL,
    PRIMARY KEY (`pk`),
    UNIQUE KEY `job_id` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `user` (
    `pk` bigint(20) NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(60) NOT NULL UNIQUE COMMENT 'unique identify',
    `password` VARCHAR(256) NOT NULL COMMENT 'encode password',
    `created_at` datetime DEFAULT NULL COMMENT 'create time',
    `updated_at` datetime DEFAULT NULL COMMENT 'update time',
    `deleted_at` datetime DEFAULT NULL COMMENT 'delete time',
    PRIMARY KEY (`pk`),
    UNIQUE KEY (`name`),
    INDEX user_name (`name`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='user info table';

-- root user with initial password 'paddleflow'
TRUNCATE `paddleflow`.`user`;
insert into user(name, password) values('root','$2a$10$1qdSQN5wMl3FtXoxw7mKpuxBqIuP0eYXTBM9CBn5H4KubM/g5Hrb6%');

CREATE TABLE IF NOT EXISTS `grant` (
    `pk` bigint(20) NOT NULL AUTO_INCREMENT,
    `id` VARCHAR(60) NOT NULL,
    `created_at` datetime DEFAULT NULL,
    `updated_at` datetime DEFAULT NULL,
    `deleted_at` datetime DEFAULT NULL,
    `user_name` VARCHAR(128) NOT NULL,
    `resource_type` VARCHAR(36) NOT NULL,
    `resource_id`   VARCHAR(36) NOT NULL,
    PRIMARY KEY (`pk`),
    UNIQUE KEY (`id`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `run` (
    `pk` bigint(20) NOT NULL AUTO_INCREMENT,
    `id` varchar(60) NOT NULL,
    `source` varchar(256) NOT NULL,
    `name` varchar(60) NOT NULL,
    `user_name` varchar(60) NOT NULL,
    `fs_id` varchar(60) NOT NULL,
    `fs_name` varchar(60) NOT NULL,
    `image_url` varchar(128),
    `description` text,
    `param_raw` text,
    `run_yaml` text,
    `runtime_raw` text,
    `entry` varchar(256),
    `message` text,
    `status` varchar(32) DEFAULT NULL,
    `run_cache_ids` text,
    `created_at` datetime(3) DEFAULT NULL,
    `activated_at` datetime(3) DEFAULT NULL,
    `updated_at` datetime(3) DEFAULT NULL,
    `deleted_at` datetime(3) DEFAULT NULL,
    PRIMARY KEY (`pk`),
    UNIQUE KEY (`id`),
    INDEX (`fs_id`),
    INDEX (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `image` (
    `pk` bigint(20) NOT NULL AUTO_INCREMENT,
    `id` varchar(128) NOT NULL UNIQUE,
    `image_id` varchar(64),
    `fs_id` varchar(60) NOT NULL,
    `source` varchar(256) NOT NULL,
    `md5` varchar(60),
    `url` varchar(256),
    `created_at` datetime(3) DEFAULT NULL,
    `updated_at` datetime(3) DEFAULT NULL,
    `deleted_at` datetime(3) DEFAULT NULL,
    PRIMARY KEY (`pk`),
    UNIQUE KEY (`id`),
    INDEX (`fs_id`),
    INDEX (`image_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `pipeline` (
    `pk` bigint(20) NOT NULL AUTO_INCREMENT,
    `id` varchar(60) NOT NULL UNIQUE,
    `name` varchar(60) NOT NULL,
    `fs_id` varchar(60) NOT NULL,
    `fs_name` varchar(60) NOT NULL,
    `user_name` varchar(60) NOT NULL,
    `pipeline_yaml` text NOT NULL,
    `pipeline_md5` varchar(32) NOT NULL,
    `created_at` datetime(3) DEFAULT NULL,
    `updated_at` datetime(3) DEFAULT NULL,
    `deleted_at` datetime(3) DEFAULT NULL,
    PRIMARY KEY (`pk`),
    UNIQUE KEY (`id`),
    UNIQUE INDEX idx_fs_path (`fs_id`, `name`),
    UNIQUE INDEX idx_fs_md5 (`fs_id`, `pipeline_md5`),
    INDEX (`fs_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `run_cache` (
    `pk` bigint(20) NOT NULL AUTO_INCREMENT,
    `id` varchar(60) NOT NULL UNIQUE,
    `step` varchar(256) NOT NULL,
    `first_fp` varchar(256),
    `second_fp` varchar(256),
    `source` varchar(256) NOT NULL,
    `fs_id` varchar(60) NOT NULL,
    `run_id` varchar(60) NOT NULL,
    `fs_name` varchar(60) NOT NULL,
    `user_name` varchar(60) NOT NULL,
    `expired_time` varchar(64) NOT NULL DEFAULT '-1',
    `strategy` varchar(16) NOT NULL DEFAULT 'conservative',
    `custom` text,
    `created_at` datetime(3) DEFAULT NULL,
    `updated_at` datetime(3) DEFAULT NULL,
    `deleted_at` datetime(3) DEFAULT NULL,
    PRIMARY KEY (`pk`),
    UNIQUE KEY (`id`),
    INDEX (`step`),
    INDEX (`fs_id`),
    INDEX (`strategy`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `artifact_event` (
    `pk` bigint(20) NOT NULL AUTO_INCREMENT,
    `md5` varchar(32) NOT NULL,
    `run_id` varchar(60) NOT NULL,
    `fs_id` varchar(60) NOT NULL,
    `user_name` varchar(60) NOT NULL,
    `fs_name` varchar(60) NOT NULL,
    `artifact_path` varchar(256) NOT NULL,
    `step` varchar(256) Not Null,
    `artifact_name` varchar(32) Not Null,
    `type` varchar(16) Not Null,
    `meta` text,
    `created_at` datetime(3) DEFAULT NULL,
    `updated_at` datetime(3) DEFAULT NULL,
    `deleted_at` datetime(3) DEFAULT NULL,
    PRIMARY KEY (`pk`),
    INDEX (`fs_id`),
    INDEX (`type`),
    INDEX (`run_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `cluster_info` (
    `pk` bigint(20) NOT NULL AUTO_INCREMENT,
    `id` varchar(60) NOT NULL UNIQUE COMMENT 'cluster id',
    `name` varchar(255) NOT NULL COMMENT 'cluster name',
    `description` varchar(255) NOT NULL DEFAULT '' COMMENT 'cluster description',
    `endpoint` varchar(255) NOT NULL DEFAULT '' COMMENT 'cluster endpoint, e.g. http://10.11.11.47:8080',
    `source` varchar(64) NOT NULL DEFAULT 'OnPremise' COMMENT 'cluter source, e.g. OnPremise/AWS/CCE',
    `cluster_type` varchar(32) NOT NULL DEFAULT '' COMMENT 'cluster type, e.g. Kubernetes/Local',
    `version` varchar(32) DEFAULT NULL COMMENT 'cluster version, e.g. v1.16',
    `status` varchar(32) NOT NULL DEFAULT 'online' COMMENT 'status in {online, offline}',
    `credential` text DEFAULT NULL COMMENT 'cluster credential, e.g. kube config in k8s',
    `setting` text DEFAULT NULL COMMENT 'extra settings',
    `namespace_list` text DEFAULT NULL COMMENT 'json type，e.g. ["ns1", "ns2"]',
    `created_at` datetime DEFAULT NULL COMMENT 'create time',
    `updated_at` datetime DEFAULT NULL COMMENT 'update time',
    `deleted_at` char(32) NOT NULL DEFAULT '' COMMENT 'deleted flag, not null means deleted',
    PRIMARY KEY (`pk`),
    UNIQUE (`name`, `deleted_at`),
    UNIQUE (`id`, `deleted_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `flavour` (
    `pk` bigint(20) NOT NULL AUTO_INCREMENT,
    `id` varchar(60) NOT NULL UNIQUE COMMENT 'id',
    `name` varchar(60) NOT NULL UNIQUE COMMENT 'unique flavour name',
    `cluster_id` varchar(68) DEFAULT '' COMMENT 'cluster id',
    `cpu` varchar(20) NOT NULL COMMENT 'cpu',
    `mem` varchar(20) NOT NULL COMMENT 'memory',
    `scalar_resources` varchar(255) DEFAULT NULL COMMENT 'scalar resource e.g. GPU',
    `user_name` varchar(60) DEFAULT NULL COMMENT 'creator name',
    `created_at` datetime(3) DEFAULT NULL,
    `updated_at` datetime(3) DEFAULT NULL,
    `deleted_at` datetime(3) DEFAULT NULL,
    PRIMARY KEY (`pk`)
    UNIQUE KEY `name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

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
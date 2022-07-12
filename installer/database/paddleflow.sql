CREATE DATABASE IF NOT EXISTS `paddleflow_db` DEFAULT CHARACTER SET utf8  COLLATE utf8_bin;
USE `paddleflow_db`;

CREATE TABLE IF NOT EXISTS `cluster_info` (
    `pk` bigint(20) NOT NULL AUTO_INCREMENT,
    `id` varchar(60) NOT NULL UNIQUE COMMENT 'cluster id',
    `name` varchar(255) NOT NULL COMMENT 'cluster name',
    `description` varchar(2048) NOT NULL DEFAULT '' COMMENT 'cluster description',
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
    UNIQUE KEY idx_name (`name`, `deleted_at`),
    UNIQUE KEY idx_id (`id`, `deleted_at`)
    ) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_bin;

CREATE TABLE IF NOT EXISTS `flavour` (
    `pk` bigint(20) NOT NULL AUTO_INCREMENT,
    `id` varchar(60) NOT NULL UNIQUE COMMENT 'id',
    `name` varchar(60) NOT NULL UNIQUE COMMENT 'unique flavour name',
    `cluster_id` varchar(60) DEFAULT '' COMMENT 'cluster id',
    `cpu` varchar(20) NOT NULL COMMENT 'cpu',
    `mem` varchar(20) NOT NULL COMMENT 'memory',
    `scalar_resources` varchar(255) DEFAULT NULL COMMENT 'scalar resource e.g. GPU',
    `user_name` varchar(60) DEFAULT NULL COMMENT 'creator name',
    `created_at` datetime(3) DEFAULT NULL,
    `updated_at` datetime(3) DEFAULT NULL,
    `deleted_at` datetime(3) DEFAULT NULL,
    PRIMARY KEY (`pk`),
    UNIQUE KEY idx_name (`name`)
    ) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_bin;

CREATE TABLE IF NOT EXISTS `queue` (
    `pk` bigint(20) NOT NULL AUTO_INCREMENT,
    `id` varchar(60) NOT NULL UNIQUE,
    `name` varchar(255) NOT NULL UNIQUE,
    `namespace` varchar(64) NOT NULL,
    `cluster_id` varchar(60) NOT NULL DEFAULT '',
    `quota_type` varchar(255) DEFAULT NULL,
    `min_resources` text DEFAULT NULL,
    `max_resources` text DEFAULT NULL,
    `location` text DEFAULT NULL,
    `status` varchar(20) DEFAULT NULL,
    `scheduling_policy` varchar(2048) DEFAULT NULL,
    `created_at` datetime(3) DEFAULT NULL,
    `updated_at` datetime(3) DEFAULT NULL,
    `deleted_at` datetime(3) DEFAULT NULL,
    PRIMARY KEY (`pk`),
    UNIQUE KEY `queue_name` (`name`)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_bin;

CREATE TABLE IF NOT EXISTS `job` (
    `pk` bigint(20) NOT NULL AUTO_INCREMENT,
    `id` varchar(60) NOT NULL UNIQUE,
    `name` varchar(512) DEFAULT '',
    `user_name` varchar(60) NOT NULL,
    `queue_id` varchar(60) NOT NULL,
    `type` varchar(20) NOT NULL,
    `config` mediumtext NOT NULL,
    `runtime_info` mediumtext DEFAULT NULL,
    `status` varchar(32) DEFAULT NULL,
    `message` text DEFAULT NULL,
    `resource` text DEFAULT NULL,
    `framework` varchar(30) DEFAULT NULL,
    `members` mediumtext DEFAULT NULL,
    `extension_template` text DEFAULT NULL,
    `parent_job` varchar(60) DEFAULT NULL,
    `created_at` datetime(3) NULL DEFAULT CURRENT_TIMESTAMP(3),
    `activated_at` datetime(3) DEFAULT NULL,
    `updated_at` datetime(3) NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    `deleted_at` varchar(64) DEFAULT '',
    PRIMARY KEY (`pk`),
    UNIQUE KEY `job_id` (`id`, `deleted_at`)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_bin;

CREATE TABLE IF NOT EXISTS `job_label` (
    `pk` bigint(20) NOT NULL AUTO_INCREMENT,
    `id` varchar(36) NOT NULL UNIQUE,
    `label` varchar(255) NOT NULL,
    `job_id` varchar(60) NOT NULL,
    `created_at` datetime(3) DEFAULT NULL,
    `deleted_at` datetime(3) DEFAULT NULL,
    PRIMARY KEY (`pk`),
    UNIQUE KEY `idx_id` (`id`)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_bin;

CREATE TABLE IF NOT EXISTS `job_task` (
    `pk` bigint(20) NOT NULL AUTO_INCREMENT,
    `id` varchar(64) NOT NULL UNIQUE,
    `job_id` varchar(60) NOT NULL,
    `namespace` varchar(64) NOT NULL,
    `name` varchar(512) NOT NULL,
    `node_name` varchar(512) DEFAULT NULL,
    `member_role` varchar(64) DEFAULT NULL,
    `status` varchar(32) DEFAULT NULL,
    `message` text DEFAULT NULL,
    `log_url` varchar(4096) DEFAULT NULL,
    `ext_runtime_status` mediumtext DEFAULT NULL,
    `created_at` datetime(3) DEFAULT NULL,
    `started_at` datetime(3) DEFAULT NULL,
    `updated_at` datetime(3) DEFAULT NULL,
    `deleted_at` datetime(3) DEFAULT NULL,
    PRIMARY KEY (`pk`),
    UNIQUE KEY `idx_id` (`id`)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_bin;

CREATE TABLE IF NOT EXISTS `user` (
    `pk` bigint(20) NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(60) NOT NULL UNIQUE COMMENT 'unique identify',
    `password` VARCHAR(256) NOT NULL COMMENT 'encode password',
    `created_at` datetime DEFAULT NULL COMMENT 'create time',
    `updated_at` datetime DEFAULT NULL COMMENT 'update time',
    `deleted_at` datetime DEFAULT NULL COMMENT 'delete time',
    PRIMARY KEY (`pk`),
    UNIQUE KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_bin COMMENT='user info table';

-- root user with initial password 'paddleflow'
TRUNCATE `paddleflow_db`.`user`;
insert into user(name, password) values('root','$2a$10$1qdSQN5wMl3FtXoxw7mKpuxBqIuP0eYXTBM9CBn5H4KubM/g5Hrb6%');
insert into flavour(id, name, cpu, mem, scalar_resources) values('1','flavour1', 1, '1Gi', null);
insert into flavour(id, name, cpu, mem, scalar_resources) values('2','flavour2', 4, '8Gi', '{"nvidia.com/gpu":"1"}');
insert into flavour(id, name, cpu, mem, scalar_resources) values('3','flavour3', 4, '8Gi', '{"nvidia.com/gpu":"2"}');

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
)ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_bin;

CREATE TABLE IF NOT EXISTS `run` (
    `pk` bigint(20) NOT NULL AUTO_INCREMENT,
    `id` varchar(60) NOT NULL,
    `name` varchar(60) NOT NULL,
    `source` varchar(256) NOT NULL,
    `user_name` varchar(60) NOT NULL,
    `global_fs_id` varchar(60) NOT NULL,
    `global_fs_name` varchar(60) NOT NULL,
    `description` text NOT NULL,
    `parameters_json` text NOT NULL,
    `run_yaml` text NOT NULL,
    `docker_env` varchar(128) NOT NULL,
    `disabled` text NOT NULL,
    `schedule_id` varchar(60) NOT NULL,
    `message` text NOT NULL,
    `status` varchar(32) DEFAULT NULL,
    `run_cached_ids` text NOT NULL,
    `scheduled_at` datetime(3) DEFAULT NULL,
    `created_at` datetime(3) DEFAULT NULL,
    `activated_at` datetime(3) DEFAULT NULL,
    `updated_at` datetime(3) DEFAULT NULL,
    `deleted_at` datetime(3) DEFAULT NULL,
    PRIMARY KEY (`pk`),
    UNIQUE KEY (`id`),
    INDEX (`global_fs_id`),
    INDEX (`status`)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_bin;

CREATE TABLE IF NOT EXISTS `run_job` (
    `pk` bigint(20) NOT NULL AUTO_INCREMENT,
    `id` varchar(60) NOT NULL,
    `run_id` varchar(60) NOT NULL,
    `parent_dag_id` varchar(60) NOT NULL,
    `name` varchar(60) NOT NULL,
    `step_name` varchar(60) NOT NULL,
    `command` text,
    `parameters_json` text,
    `artifacts_json` text,
    `env_json` text,
    `docker_env` varchar(128),
    `loop_seq` int NOT NULL,
    `status` varchar(32) DEFAULT NULL,
    `message` text,
    `cache_json` text,
    `cache_run_id` varchar(60),
    `cache_job_id` varchar(60),
    `fs_mount_json` text,
    `created_at` datetime(3) DEFAULT NULL,
    `activated_at` datetime(3) DEFAULT NULL,
    `updated_at` datetime(3) DEFAULT NULL,
    `deleted_at` datetime(3) DEFAULT NULL,
    PRIMARY KEY (`pk`),
    INDEX (`run_id`),
    INDEX (`status`)
)ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_bin;

CREATE TABLE IF NOT EXISTS `run_dag` (
    `pk` bigint(20) NOT NULL AUTO_INCREMENT,
    `id` varchar(60) NOT NULL,
    `run_id` varchar(60) NOT NULL,
    `parent_dag_id` varchar(60) NOT NULL,
    `name` varchar(60) NOT NULL,
    `dag_name` varchar(60) NOT NULL,
    `parameters_json` text,
    `artifacts_json` text,
    `loop_seq` int NOT NULL,
    `status` varchar(32) DEFAULT NULL,
    `message` text,
    `created_at` datetime(3) DEFAULT NULL,
    `activated_at` datetime(3) DEFAULT NULL,
    `updated_at` datetime(3) DEFAULT NULL,
    `deleted_at` datetime(3) DEFAULT NULL,
    PRIMARY KEY (`pk`),
    INDEX (`run_id`),
    INDEX (`status`)
)ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_bin;

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
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_bin;

CREATE TABLE IF NOT EXISTS `pipeline` (
    `pk` bigint(20) NOT NULL AUTO_INCREMENT,
    `id` varchar(60) NOT NULL UNIQUE,
    `name` varchar(60) NOT NULL,
    `desc` varchar(256) NOT NULL,
    `user_name` varchar(60) NOT NULL,
    `created_at` datetime(3) DEFAULT NULL,
    `updated_at` datetime(3) DEFAULT NULL,
    `deleted_at` datetime(3) DEFAULT NULL,
    PRIMARY KEY (`pk`),
    UNIQUE KEY (`id`),
    INDEX idx_fs_name (`user_name`, `name`)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_bin;

CREATE TABLE IF NOT EXISTS `pipeline_detail` (
    `pk` bigint(20) NOT NULL AUTO_INCREMENT,
    `id` varchar(60) NOT NULL,
    `pipeline_id` varchar(60) NOT NULL,
    `fs_id` varchar(60) NOT NULL,
    `fs_name` varchar(60) NOT NULL,
    `yaml_path` text NOT NULL,
    `pipeline_yaml` text NOT NULL,
    `pipeline_md5` varchar(32) NOT NULL,
    `user_name` varchar(60) NOT NULL,
    `created_at` datetime(3) DEFAULT NULL,
    `updated_at` datetime(3) DEFAULT NULL,
    `deleted_at` datetime(3) DEFAULT NULL,
    PRIMARY KEY (`pk`)
    ) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_bin;

CREATE TABLE IF NOT EXISTS `schedule` (
    `pk` bigint(20) NOT NULL AUTO_INCREMENT,
    `id` varchar(60) NOT NULL,
    `name` varchar(60) NOT NULL,
    `desc` varchar(256) NOT NULL,
    `pipeline_id` varchar(60) NOT NULL,
    `pipeline_detail_id` varchar(60) NOT NULL,
    `user_name` varchar(60) NOT NULL,
    `fs_config` varchar(1024) NOT NULL,
    `crontab` varchar(60) NOT NULL,
    `options` text,
    `message` text,
    `status` varchar(32) DEFAULT NULL,
    `start_at` datetime(3) DEFAULT NULL,
    `end_at` datetime(3) DEFAULT NULL,
    `next_run_at` datetime(3) DEFAULT NULL,
    `created_at` datetime(3) DEFAULT NULL,
    `updated_at` datetime(3) DEFAULT NULL,
    `deleted_at` datetime(3) DEFAULT NULL,
    PRIMARY KEY (`pk`)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_bin;

CREATE TABLE IF NOT EXISTS `run_cache` (
    `pk` bigint(20) NOT NULL AUTO_INCREMENT,
    `id` varchar(60) NOT NULL UNIQUE,
    `job_id` varchar(60) NOT NULL,
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
    INDEX (`job_id`),
    INDEX (`fs_id`),
    INDEX (`strategy`)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_bin;

CREATE TABLE IF NOT EXISTS `artifact_event` (
    `pk` bigint(20) NOT NULL AUTO_INCREMENT,
    `md5` varchar(32) NOT NULL,
    `run_id` varchar(60) NOT NULL,
    `fs_id` varchar(60) NOT NULL,
    `user_name` varchar(60) NOT NULL,
    `fs_name` varchar(60) NOT NULL,
    `artifact_path` varchar(256) NOT NULL,
    `step` varchar(256) Not Null,
    `job_id` varchar(60) NOT NULL,
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
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_bin;

CREATE TABLE IF NOT EXISTS `filesystem` (
    `pk` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'pk',
    `id` varchar(36) NOT NULL COMMENT 'id',
    `name` varchar(200) NOT NULL,
    `server_address` varchar(1024) NOT NULL,
    `type` varchar(50) NOT NULL COMMENT 'file system type',
    `subpath` varchar(1024) NOT NULL COMMENT 'subpath',
    `user_name` varchar(256) NOT NULL,
    `independent_mount_process` tinyint(1) NOT NULL default 0 COMMENT 'csi mount use independent mount process',
    `created_at` datetime NOT NULL,
    `updated_at` datetime NOT NULL,
    `properties` TEXT,
    PRIMARY KEY (`pk`),
    UNIQUE KEY (`id`)
    )ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_bin;

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
    )ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_bin ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8 COMMENT='file system';

CREATE TABLE IF NOT EXISTS `fs_cache_config` (
    `pk` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'pk',
    `fs_id` varchar(36) NOT NULL COMMENT 'file system id',
    `cache_dir` varchar(4096) NOT NULL COMMENT 'cache dir, e.g. /var/pfs_cache',
    `quota` bigint(20) NOT NULL COMMENT 'cache quota',
    `block_size` int(5) NOT NULL COMMENT 'cache block size',
    `meta_driver` varchar(32) NOT NULL COMMENT 'meta_driver，e.g. default/mem/leveldb/nutsdb',
    `debug` tinyint(1) NOT NULL COMMENT 'turn on debug log',
    `extra_config` text  COMMENT 'extra cache config',
    `node_affinity` text  COMMENT 'node affinity，e.g. node affinity in k8s',
    `node_tainttoleration` text COMMENT 'node taints',
    `created_at` datetime NOT NULL COMMENT 'create time',
    `updated_at` datetime NOT NULL COMMENT 'update time',
    `deleted_at` datetime(3) DEFAULT NULL COMMENT 'delete time',
    PRIMARY KEY (`pk`),
    UNIQUE KEY (`fs_id`)
    )ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_bin ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8 COMMENT='file system cache config';

CREATE TABLE IF NOT EXISTS `fs_cache` (
    `pk` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'pk',
    `cache_id` varchar(36) NOT NULL COMMENT 'unique fs cache id',
    `cache_hash_id` varchar(36) COMMENT 'fs cache unique hashid for judging the same fscache or not',
    `fs_id` varchar(36) NOT NULL COMMENT 'file system id',
    `cluster_id` varchar(60) DEFAULT '' COMMENT 'cluster id',
    `cache_dir` varchar(4096) NOT NULL COMMENT 'cache dir, e.g. /var/pfs_cache',
    `nodename` varchar(255) NOT NULL COMMENT 'node name',
    `usedsize` bigint(20) NOT NULL COMMENT 'cache used size on cache dir',
    `created_at` datetime NOT NULL COMMENT 'create time',
    `updated_at` datetime NOT NULL COMMENT 'update time',
    `deleted_at` datetime(3) DEFAULT NULL  COMMENT 'delete time',
    PRIMARY KEY (`pk`),
    UNIQUE KEY (`cache_id`),
    INDEX idx_fs_id (`fs_id`),
    INDEX idx_fs_id_nodename (`fs_id`,`nodename`)
    )ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_bin ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8 COMMENT='manage file system cache ';

CREATE TABLE IF NOT EXISTS `paddleflow_node_info` (
    `pk` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'pk',
    `cluster_id` varchar(255) NOT NULL DEFAULT '',
    `nodename` varchar(255) NOT NULL COMMENT 'node name',
    `total_disk_size` bigint(20) NOT NULL COMMENT 'the total disk size can be used for cache of the node ',
    `disk_io_ratio` bigint(20) NOT NULL COMMENT 'the disk io ratio of the node',
    `net_io_ratio` bigint(20) NOT NULL COMMENT 'the net io ratio of the node',
    `created_at` datetime NOT NULL COMMENT 'create time',
    `updated_at` datetime NOT NULL COMMENT 'update time',
    `deleted_at` datetime(3) DEFAULT NULL  COMMENT 'delete time',
    PRIMARY KEY (`pk`),
    UNIQUE INDEX idx_cluster_node (`cluster_id`,`nodename`)
    )ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_bin ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8 COMMENT='all node info for compute node score for schedule or location awareness in the future';
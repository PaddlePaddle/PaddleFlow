
CREATE TABLE cluster_info (
    pk bigserial NOT NULL,
    id varchar(60) NOT NULL ,
    name varchar(255) NOT NULL ,
    description varchar(2048) NOT NULL DEFAULT '' ,
    endpoint varchar(255) NOT NULL DEFAULT '' ,
    source varchar(64) NOT NULL DEFAULT 'OnPremise' ,
    cluster_type varchar(32) NOT NULL DEFAULT '' ,
    version varchar(32) DEFAULT NULL ,
    status varchar(32) NOT NULL DEFAULT 'online' ,
    credential text DEFAULT NULL ,
    setting text DEFAULT NULL ,
    namespace_list text DEFAULT NULL ,
    created_at timestamptz DEFAULT NULL ,
    updated_at timestamptz DEFAULT NULL ,
    deleted_at char(32) NOT NULL DEFAULT '' ,
    PRIMARY KEY (pk));
CREATE UNIQUE INDEX idx_name ON cluster_info (name, deleted_at);
CREATE UNIQUE INDEX idx_id ON cluster_info (id, deleted_at);
  

CREATE TABLE resource_pool (
    pk bigserial NOT NULL,
    id varchar(60) NOT NULL ,
    name varchar(255) NOT NULL ,
    namespace varchar(64) DEFAULT NULL ,
    resource_type varchar(32) DEFAULT NULL ,
    resource_provider varchar(32) DEFAULT NULL ,
    org_id varchar(64) DEFAULT NULL ,
    creator_id varchar(64) DEFAULT NULL ,
    description varchar(2048) NOT NULL DEFAULT '' ,
    cluster_id varchar(60) NOT NULL DEFAULT '' ,
    quota_type varchar(255) DEFAULT NULL,
    total_resources text DEFAULT NULL ,
    min_resources text DEFAULT NULL,
    labels text DEFAULT NULL  ,
    annotations text DEFAULT NULL  ,
    status varchar(32) NOT NULL DEFAULT 'open' ,
    is_hybrid int DEFAULT 0 ,
    created_at timestamptz DEFAULT NULL ,
    updated_at timestamptz DEFAULT NULL ,
    deleted_at char(32) NOT NULL DEFAULT '' ,
    PRIMARY KEY (pk));
CREATE UNIQUE INDEX rp_idx_name ON resource_pool (name, deleted_at);
CREATE UNIQUE INDEX rp_idx_id ON resource_pool (id, deleted_at);


CREATE TABLE "flavour" (
    pk bigserial NOT NULL,
    id varchar(60) NOT NULL ,
    name varchar(60) NOT NULL ,
    cluster_id varchar(60) DEFAULT '' ,
    cpu varchar(20) NOT NULL ,
    mem varchar(20) NOT NULL ,
    scalar_resources varchar(255) DEFAULT NULL ,
    user_name varchar(60) DEFAULT NULL ,
    created_at timestamptz DEFAULT NULL,
    updated_at timestamptz DEFAULT NULL,
    deleted_at timestamptz DEFAULT NULL,
    PRIMARY KEY (pk));
CREATE UNIQUE INDEX flavour_idx_name ON flavour (name);
CREATE UNIQUE INDEX flavour_idx_id ON flavour (id);
  

CREATE TABLE queue (
    pk bigserial NOT NULL,
    id varchar(60) NOT NULL,
    name varchar(255) NOT NULL,
    namespace varchar(64) NOT NULL,
    cluster_id varchar(60) NOT NULL DEFAULT '',
    quota_type varchar(255) DEFAULT NULL,
    min_resources text DEFAULT NULL,
    max_resources text DEFAULT NULL,
    location text DEFAULT NULL,
    status varchar(20) DEFAULT NULL,
    scheduling_policy varchar(2048) DEFAULT NULL,
    created_at timestamptz DEFAULT NULL,
    updated_at timestamptz DEFAULT NULL,
    deleted_at timestamptz DEFAULT NULL,
    PRIMARY KEY (pk));
CREATE UNIQUE INDEX queue_id ON queue (id);
CREATE UNIQUE INDEX queue_name ON queue (name);
CREATE INDEX cluster_id ON queue (cluster_id);

CREATE TABLE job (
    pk bigserial NOT NULL,
    id varchar(60) NOT NULL,
    name varchar(512) DEFAULT '',
    user_name varchar(60) NOT NULL,
    queue_id varchar(60) NOT NULL,
    type varchar(20) NOT NULL,
    config text NOT NULL,
    runtime_info text DEFAULT NULL,
    runtime_status text DEFAULT NULL,
    status varchar(32) NOT NULL,
    message text DEFAULT NULL,
    resource text DEFAULT NULL,
    framework varchar(30) DEFAULT NULL,
    members text DEFAULT NULL,
    extension_template text DEFAULT NULL,
    parent_job varchar(60) DEFAULT NULL,
    created_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP(3),
    activated_at timestamptz DEFAULT NULL,
    updated_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP(3),
    deleted_at varchar(64) DEFAULT '',
    PRIMARY KEY (pk));
CREATE UNIQUE INDEX job_id ON job (id, deleted_at);
CREATE INDEX status_queue_deleted ON job (queue_id, status, deleted_at);
CREATE INDEX status_deleted ON job (status, deleted_at);

CREATE TABLE job_label (
    pk bigserial NOT NULL,
    id varchar(36) NOT NULL,
    label varchar(255) NOT NULL,
    job_id varchar(60) NOT NULL,
    created_at timestamptz DEFAULT NULL,
    deleted_at timestamptz DEFAULT NULL,
    PRIMARY KEY (pk));
CREATE UNIQUE INDEX job_label_idx_id ON job_label (id);


CREATE TABLE job_task (
    pk bigserial NOT NULL,
    id varchar(64) NOT NULL,
    job_id varchar(60) NOT NULL,
    namespace varchar(64) NOT NULL,
    name varchar(512) NOT NULL,
    node_name varchar(512) DEFAULT NULL,
    member_role varchar(64) DEFAULT NULL,
    status varchar(32) DEFAULT NULL,
    message text DEFAULT NULL,
    annotations text DEFAULT NULL,
    log_url varchar(4096) DEFAULT NULL,
    ext_runtime_status text DEFAULT NULL,
    created_at timestamptz DEFAULT NULL,
    started_at timestamptz DEFAULT NULL,
    updated_at timestamptz DEFAULT NULL,
    deleted_at timestamptz DEFAULT NULL,
    PRIMARY KEY (pk));
CREATE UNIQUE INDEX job_task_idx_id ON job_task (id);


CREATE TABLE "user" (
    pk bigserial NOT NULL,
    name VARCHAR(60) NOT NULL ,
    password VARCHAR(256) NOT NULL ,
    created_at timestamptz DEFAULT NULL ,
    updated_at timestamptz DEFAULT NULL ,
    deleted_at timestamptz DEFAULT NULL ,
    PRIMARY KEY (pk));
CREATE UNIQUE INDEX username ON "user" (name);


-- root user with initial password 'paddleflow'
-- TRUNCATE paddleflow_db.user;
insert into "user"(name, password) values('root','$2a$10$1qdSQN5wMl3FtXoxw7mKpuxBqIuP0eYXTBM9CBn5H4KubM/g5Hrb6%');
insert into "flavour"(id, name, cpu, mem, scalar_resources) values('1','flavour1', 1, '1Gi', null);
insert into "flavour"(id, name, cpu, mem, scalar_resources) values('2','flavour2', 4, '8Gi', '{"nvidia.com/gpu":"1"}');
insert into "flavour"(id, name, cpu, mem, scalar_resources) values('3','flavour3', 4, '8Gi', '{"nvidia.com/gpu":"2"}');

CREATE TABLE "grant" (
    pk bigserial NOT NULL,
    id VARCHAR(60) NOT NULL,
    created_at timestamptz DEFAULT NULL,
    updated_at timestamptz DEFAULT NULL,
    deleted_at timestamptz DEFAULT NULL,
    user_name VARCHAR(128) NOT NULL,
    resource_type VARCHAR(36) NOT NULL,
    resource_id   VARCHAR(36) NOT NULL,
    PRIMARY KEY (pk));
CREATE UNIQUE INDEX grant_id ON "grant" (id);

CREATE TABLE run (
    pk bigserial NOT NULL,
    id varchar(60) NOT NULL,
    name varchar(128) NOT NULL,
    source varchar(256) NOT NULL,
    user_name varchar(60) NOT NULL,
    fs_id varchar(200) NOT NULL,
    fs_name varchar(60) NOT NULL,
    description text NOT NULL,
    parameters_json text NOT NULL,
    run_yaml text NOT NULL,
    docker_env varchar(128) NOT NULL,
    disabled text NOT NULL,
    failure_options_json text NOT NULL,
    schedule_id varchar(60) NOT NULL,
    message text NOT NULL,
    status varchar(32) DEFAULT NULL,
    run_options_json text NOT NULL,
    run_cached_ids text NOT NULL,
    scheduled_at timestamptz DEFAULT NULL,
    created_at timestamptz DEFAULT NULL,
    activated_at timestamptz DEFAULT NULL,
    updated_at timestamptz DEFAULT NULL,
    deleted_at timestamptz DEFAULT NULL,
    PRIMARY KEY (pk));
CREATE INDEX run_id ON run (id);
CREATE INDEX run_fs_name ON run (fs_name);
CREATE INDEX run_status ON run (status);


CREATE TABLE run_job (
    pk bigserial NOT NULL,
    id varchar(60) NOT NULL,
    run_id varchar(60) NOT NULL,
    parent_dag_id varchar(60) NOT NULL,
    name varchar(60) NOT NULL,
    step_name varchar(60) NOT NULL,
    command text,
    parameters_json text,
    artifacts_json text,
    env_json text,
    docker_env varchar(128),
    loop_seq int NOT NULL,
    status varchar(32) DEFAULT NULL,
    message text,
    cache_json text,
    cache_run_id varchar(60),
    cache_job_id varchar(60),
    extra_fs_json text,
    created_at timestamptz DEFAULT NULL,
    activated_at timestamptz DEFAULT NULL,
    updated_at timestamptz DEFAULT NULL,
    deleted_at timestamptz DEFAULT NULL,
    PRIMARY KEY (pk));
CREATE INDEX run_job_id ON run_job (run_id);
CREATE INDEX run_job_status ON run_job (status);

CREATE TABLE run_dag (
    pk bigserial NOT NULL,
    id varchar(60) NOT NULL,
    run_id varchar(60) NOT NULL,
    parent_dag_id varchar(60) NOT NULL,
    name varchar(60) NOT NULL,
    dag_name varchar(60) NOT NULL,
    parameters_json text,
    artifacts_json text,
    loop_seq int NOT NULL,
    status varchar(32) DEFAULT NULL,
    message text,
    created_at timestamptz DEFAULT NULL,
    activated_at timestamptz DEFAULT NULL,
    updated_at timestamptz DEFAULT NULL,
    deleted_at timestamptz DEFAULT NULL,
    PRIMARY KEY (pk));
CREATE INDEX run_dag_id ON run_dag (run_id);
CREATE INDEX run_dag_status ON run_dag(status);

CREATE TABLE image (
    pk bigserial NOT NULL,
    id varchar(128) NOT NULL,
    image_id varchar(64),
    fs_id varchar(200) NOT NULL,
    source varchar(256) NOT NULL,
    md5 varchar(60),
    url varchar(256),
    created_at timestamptz DEFAULT NULL,
    updated_at timestamptz DEFAULT NULL,
    deleted_at timestamptz DEFAULT NULL,
    PRIMARY KEY (pk));
CREATE UNIQUE INDEX image_name_id ON image (id);
CREATE INDEX image_fs_id ON image (fs_id);
CREATE INDEX image_id ON image (image_id);


CREATE TABLE pipeline (
    pk bigserial NOT NULL,
    id varchar(60) NOT NULL,
    name varchar(128) NOT NULL,
    "desc" varchar(256) NOT NULL,
    user_name varchar(60) NOT NULL,
    created_at timestamptz DEFAULT NULL,
    updated_at timestamptz DEFAULT NULL,
    deleted_at timestamptz DEFAULT NULL,
    PRIMARY KEY (pk));
CREATE INDEX pipline_id ON pipeline (id);
CREATE INDEX idx_fs_name ON pipeline (user_name, name);


CREATE TABLE pipeline_version (
    pk bigserial NOT NULL,
    id varchar(60) NOT NULL,
    pipeline_id varchar(60) NOT NULL,
    fs_id varchar(200) NOT NULL,
    fs_name varchar(60) NOT NULL,
    yaml_path text NOT NULL,
    pipeline_yaml text NOT NULL,
    pipeline_md5 varchar(32) NOT NULL,
    user_name varchar(60) NOT NULL,
    created_at timestamptz DEFAULT NULL,
    updated_at timestamptz DEFAULT NULL,
    deleted_at timestamptz DEFAULT NULL,
    PRIMARY KEY (pk));


CREATE TABLE schedule (
    pk bigserial NOT NULL,
    id varchar(60) NOT NULL,
    name varchar(60) NOT NULL,
    "desc" varchar(256) NOT NULL,
    pipeline_id varchar(60) NOT NULL,
    pipeline_version_id varchar(60) NOT NULL,
    user_name varchar(60) NOT NULL,
    crontab varchar(60) NOT NULL,
    fs_config varchar(1024) NOT NULL,
    options text,
    message text,
    status varchar(32) DEFAULT NULL,
    start_at timestamptz DEFAULT NULL,
    end_at timestamptz DEFAULT NULL,
    next_run_at timestamptz DEFAULT NULL,
    created_at timestamptz DEFAULT NULL,
    updated_at timestamptz DEFAULT NULL,
    deleted_at timestamptz DEFAULT NULL,
    PRIMARY KEY (pk));


CREATE TABLE run_cache (
    pk bigserial NOT NULL,
    id varchar(60) NOT NULL,
    job_id varchar(60) NOT NULL,
    first_fp varchar(256),
    second_fp varchar(256),
    source varchar(256) NOT NULL,
    fs_id varchar(200) NOT NULL,
    run_id varchar(60) NOT NULL,
    fs_name varchar(60) NOT NULL,
    user_name varchar(60) NOT NULL,
    expired_time varchar(64) NOT NULL DEFAULT '-1',
    strategy varchar(16) NOT NULL DEFAULT 'conservative',
    custom text,
    created_at timestamptz DEFAULT NULL,
    updated_at timestamptz DEFAULT NULL,
    deleted_at timestamptz DEFAULT NULL,
    PRIMARY KEY (pk));
CREATE INDEX run_cache_id ON run_cache (id);
CREATE INDEX cache_job_id ON run_cache (job_id);
CREATE INDEX cache_fs_id ON run_cache (fs_id);
CREATE INDEX cache_strategy ON run_cache (strategy);


CREATE TABLE artifact_event (
    pk bigserial NOT NULL,
    md5 varchar(32) NOT NULL,
    run_id varchar(60) NOT NULL,
    fs_id varchar(200) NOT NULL,
    user_name varchar(60) NOT NULL,
    fs_name varchar(60) NOT NULL,
    artifact_path varchar(256) NOT NULL,
    step varchar(256) Not Null,
    job_id varchar(60) NOT NULL,
    artifact_name varchar(32) Not Null,
    type varchar(16) Not Null,
    meta text,
    created_at timestamptz DEFAULT NULL,
    updated_at timestamptz DEFAULT NULL,
    deleted_at timestamptz DEFAULT NULL,
    PRIMARY KEY (pk));
CREATE INDEX event_id ON artifact_event (fs_id);
CREATE INDEX event_type ON artifact_event (type);
CREATE INDEX event_run_id ON artifact_event (run_id);


CREATE TABLE filesystem (
    pk bigserial NOT NULL ,
    id varchar(200) NOT NULL ,
    name varchar(200) NOT NULL,
    server_address varchar(1024) NOT NULL,
    type varchar(50) NOT NULL ,
    subpath varchar(1024) NOT NULL ,
    user_name varchar(256) NOT NULL,
    independent_mount_process int NOT NULL default 0 ,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    properties TEXT,
    PRIMARY KEY (pk));
CREATE UNIQUE INDEX filesystem_id ON filesystem (id);
  

CREATE TABLE link (
    pk bigserial NOT NULL ,
    id varchar(36) NOT NULL ,
    fs_id varchar(200) NOT NULL,
    fs_path varchar(1024) NOT NULL,
    server_address varchar(1024) NOT NULL,
    type varchar(50) NOT NULL ,
    subpath varchar(1024) NOT NULL ,
    user_name varchar(256),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    properties TEXT,
    PRIMARY KEY (pk));
CREATE UNIQUE INDEX link_id ON link (id);
  

CREATE TABLE fs_cache_config (
    pk bigserial NOT NULL ,
    fs_id varchar(200) NOT NULL ,
    cache_dir varchar(4096) NOT NULL ,
    quota bigserial NOT NULL ,
    block_size int NOT NULL ,
    meta_driver varchar(32) NOT NULL ,
    debug int NOT NULL ,
    clean_cache int NOT NULL default 0 ,
    "resource" text ,
    extra_config text  ,
    node_affinity text  ,
    node_tainttoleration text ,
    created_at timestamptz NOT NULL ,
    updated_at timestamptz NOT NULL ,
    deleted_at timestamptz DEFAULT NULL ,
    PRIMARY KEY (pk));
CREATE UNIQUE INDEX fs_cache_config_id ON fs_cache_config (fs_id);
  

CREATE TABLE fs_cache (
    pk bigserial NOT NULL ,
    cache_id varchar(36) NOT NULL ,
    cache_hash_id varchar(36) ,
    fs_id varchar(200) NOT NULL ,
    cluster_id varchar(60) DEFAULT '' ,
    cache_dir varchar(4096) NOT NULL ,
    nodename varchar(255) NOT NULL ,
    usedsize bigserial NOT NULL ,
    created_at timestamptz NOT NULL ,
    updated_at timestamptz NOT NULL ,
    deleted_at timestamptz DEFAULT NULL  ,
    PRIMARY KEY (pk));
CREATE UNIQUE INDEX cache_id ON fs_cache (cache_id);
CREATE INDEX  idx_fs_id ON fs_cache (fs_id);
CREATE INDEX idx_fs_id_nodename ON fs_cache (fs_id,nodename);
  

CREATE TABLE paddleflow_node_info (
    pk bigserial NOT NULL ,
    cluster_id varchar(255) NOT NULL DEFAULT '',
    nodename varchar(255) NOT NULL ,
    total_disk_size bigserial NOT NULL ,
    disk_io_ratio bigserial NOT NULL ,
    net_io_ratio bigserial NOT NULL ,
    created_at timestamptz NOT NULL ,
    updated_at timestamptz NOT NULL ,
    deleted_at timestamptz DEFAULT NULL  ,
    PRIMARY KEY (pk));
CREATE UNIQUE INDEX idx_cluster_node ON paddleflow_node_info (cluster_id,nodename);
  

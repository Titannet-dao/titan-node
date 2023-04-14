-- Node information table
CREATE TABLE if not exists `node_info` (
    `node_id`            VARCHAR(128) NOT NULL UNIQUE,
    `online_duration`    INT          DEFAULT 0,
    `profit`             FLOAT        DEFAULT 0,
    `download_traffic`   FLOAT        DEFAULT 0,
    `upload_traffic`     FLOAT        DEFAULT 0,
    `download_blocks`    BIGINT       DEFAULT 0,
    `last_seen`          DATETIME     DEFAULT CURRENT_TIMESTAMP,
    `quitted`            BOOLEAN      DEFAULT 0,
    `port_mapping`       VARCHAR(8)   DEFAULT '',
    `mac_location`       VARCHAR(32)  DEFAULT '',
    `product_type`       VARCHAR(32)  DEFAULT '',
    `cpu_cores`          INT          DEFAULT 0,
    `memory`             FLOAT        DEFAULT 0,
    `node_name`          VARCHAR(64)  DEFAULT '',
    `latitude`           FLOAT        DEFAULT 0,
    `longitude`          FLOAT        DEFAULT 0,
    `disk_type`          VARCHAR(64)  DEFAULT '',
    `io_system`          VARCHAR(64)  DEFAULT '',
    `system_version`     VARCHAR(32)  DEFAULT '',
    `nat_type`           VARCHAR(32)  DEFAULT '',
    `disk_space`         FLOAT        DEFAULT 0,
    `bandwidth_up`       FLOAT        DEFAULT 0,
    `bandwidth_down`     FLOAT        DEFAULT 0,
    `blocks`             BIGINT       DEFAULT 0,
    `disk_usage`         FLOAT        DEFAULT 0,
    `scheduler_sid`      VARCHAR(128) NOT NULL,
    PRIMARY KEY (`node_id`)
) ENGINE=InnoDB COMMENT='Node information';

-- Validation results table
CREATE TABLE if not exists `validation_result` (
    `round_id`      VARCHAR(128) NOT NULL,
    `node_id`       VARCHAR(128) NOT NULL,
    `validator_id`  VARCHAR(128) NOT NULL,
    `cid`           VARCHAR(128) NOT NULL,
    `block_number`  BIGINT       DEFAULT 0,
    `status`        TINYINT      DEFAULT 0,
    `duration`      BIGINT       DEFAULT 0,
    `bandwidth`     FLOAT        DEFAULT 0,
    `start_time`    DATETIME     DEFAULT NULL,
    `end_time`      DATETIME     DEFAULT NULL,
    KEY `round_node` (`round_id`, `node_id`)
) ENGINE=InnoDB COMMENT='Validation results';

-- Block download information table
CREATE TABLE if not exists `block_download_info` (
    `id`             VARCHAR(64)  NOT NULL UNIQUE,
    `block_cid`      VARCHAR(128) NOT NULL,
    `node_id`        VARCHAR(128) NOT NULL,
    `carfile_cid`    VARCHAR(128) NOT NULL,
    `block_size`     INT(20)      DEFAULT 0,
    `speed`          INT(20)      DEFAULT 0,
    `reward`         INT(20)      DEFAULT 0,
    `status`         TINYINT      DEFAULT 0,
    `failed_reason`  VARCHAR(128) DEFAULT '',
    `client_ip`      VARCHAR(18)  NOT NULL,
    `created_time`   DATETIME     DEFAULT CURRENT_TIMESTAMP,
    `complete_time`  DATETIME,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB COMMENT='Block download information';

-- Node register information table
CREATE TABLE if not exists `node_register_info` (
	`node_id`     VARCHAR(128)  NOT NULL UNIQUE,
	`public_key`  VARCHAR(1024) DEFAULT '' ,
    `create_time` VARCHAR(64)   DEFAULT '' ,
	`node_type`   VARCHAR(64)   DEFAULT '' ,
	PRIMARY KEY (`node_id`)
) ENGINE=InnoDB COMMENT='node register info';

-- Asset replica information table
CREATE TABLE if not exists `replica_info` (
	`hash`         VARCHAR(128) NOT NULL,
    `status`       TINYINT      DEFAULT 0 ,
    `node_id`      VARCHAR(128) NOT NULL ,
    `done_size`    BIGINT       DEFAULT 0 ,
    `is_candidate` BOOLEAN,
	`end_time`     DATETIME     DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY (`hash`,`node_id`),
    KEY `idx_node_id` (`node_id`)
) ENGINE=InnoDB COMMENT='replica info';

-- Asset record information table
CREATE TABLE if not exists `asset_record` (
	`hash`               VARCHAR(128) NOT NULL UNIQUE,
	`cid`                VARCHAR(128) NOT NULL UNIQUE,
    `total_size`         BIGINT       DEFAULT 0 ,
    `total_blocks`       INT          DEFAULT 0 ,
	`edge_replicas`      TINYINT      DEFAULT 0 ,
	`candidate_replicas` TINYINT      DEFAULT 0 ,
	`state`              VARCHAR(128) NOT NULL DEFAULT '',
    `expiration`         DATETIME     NOT NULL,
    `created_time`       DATETIME     DEFAULT CURRENT_TIMESTAMP,
	`end_time`           DATETIME     DEFAULT CURRENT_TIMESTAMP,
    `scheduler_sid`      VARCHAR(128) NOT NULL,
	PRIMARY KEY (`hash`),
    KEY `idx_sid` (`scheduler_sid`)
) ENGINE=InnoDB COMMENT='asset record';

-- Edge update information table
CREATE TABLE if not exists `edge_update_info` (
	`node_type`    INT          NOT NULL UNIQUE,
	`app_name`     VARCHAR(64)  NOT NULL,
    `version`      VARCHAR(32)  NOT NULL,
    `hash`         VARCHAR(128) NOT NULL,
	`download_url` VARCHAR(128) NOT NULL,
    `update_time`  DATETIME     DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (`node_type`)
) ENGINE=InnoDB COMMENT='edge update info';

-- Validators information table
CREATE TABLE if not exists `validators` (
    `node_id`       VARCHAR(128) NOT NULL,
    `scheduler_sid` VARCHAR(128) NOT NULL,
    PRIMARY KEY (`node_id`)
) ENGINE=InnoDB COMMENT='validators';

CREATE TABLE if not exists `asset_view` (
    `node_id`       VARCHAR(128) NOT NULL UNIQUE,
    `top_hash`      VARCHAR(128) NOT NULL,
    `bucket_hashes` BLOB         NOT NULL,
    PRIMARY KEY (`node_id`)
) ENGINE=InnoDB COMMENT='asset view';

CREATE TABLE if not exists `bucket` (
    `bucket_id`    VARCHAR(128) NOT NULL UNIQUE,
    `asset_hashes` BLOB         NOT NULL,
    PRIMARY KEY (`bucket_id`)
) ENGINE=InnoDB COMMENT='bucket';

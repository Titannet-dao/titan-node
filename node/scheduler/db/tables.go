package db

// Table creation SQL statements.
var cAssetStateTable = `
    CREATE TABLE if not exists %s (
		hash                VARCHAR(128) NOT NULL UNIQUE,
		state               VARCHAR(128) DEFAULT '',
		retry_count         INT          DEFAULT 0,
		replenish_replicas  INT          DEFAULT 0,
		PRIMARY KEY (hash)
	) ENGINE=InnoDB COMMENT='asset state info';`

var cReplicaInfoTable = `
    CREATE TABLE if not exists %s (
		hash          VARCHAR(128) NOT NULL,
		status        TINYINT      DEFAULT 0,
		node_id       VARCHAR(128) NOT NULL,
		done_size     BIGINT       DEFAULT 0,
		is_candidate  BOOLEAN,
		end_time      DATETIME     DEFAULT CURRENT_TIMESTAMP,
		UNIQUE KEY (hash,node_id),
		KEY idx_node_id (node_id)
	) ENGINE=InnoDB COMMENT='asset replica info';`

var cNodeInfoTable = `
    CREATE TABLE if not exists %s (
	    node_id            VARCHAR(128) NOT NULL UNIQUE,
	    online_duration    INT          DEFAULT 0,
	    profit             FLOAT        DEFAULT 0,
	    download_traffic   FLOAT        DEFAULT 0,
	    upload_traffic     FLOAT        DEFAULT 0,
	    download_blocks    BIGINT       DEFAULT 0,
	    last_seen          DATETIME     DEFAULT CURRENT_TIMESTAMP,
	    port_mapping       VARCHAR(8)   DEFAULT '',
	    mac_location       VARCHAR(32)  DEFAULT '',
	    product_type       VARCHAR(32)  DEFAULT '',
	    cpu_cores          INT          DEFAULT 0,
	    memory             FLOAT        DEFAULT 0,
	    node_name          VARCHAR(64)  DEFAULT '',
	    latitude           FLOAT        DEFAULT 0,
	    longitude          FLOAT        DEFAULT 0,
	    disk_type          VARCHAR(64)  DEFAULT '',
	    io_system          VARCHAR(64)  DEFAULT '',
	    system_version     VARCHAR(32)  DEFAULT '',
	    nat_type           VARCHAR(32)  DEFAULT '',
	    disk_space         FLOAT        DEFAULT 0,
    	bandwidth_up       FLOAT        DEFAULT 0,
    	bandwidth_down     FLOAT        DEFAULT 0,
	    blocks             BIGINT       DEFAULT 0,
	    disk_usage         FLOAT        DEFAULT 0,
	    scheduler_sid      VARCHAR(128) NOT NULL,
	    PRIMARY KEY (node_id)
	) ENGINE=InnoDB COMMENT='node info';`

var cValidationResultsTable = `
    CREATE TABLE if not exists %s (
		id            INT UNSIGNED AUTO_INCREMENT,
	    round_id      VARCHAR(128) NOT NULL,
	    node_id       VARCHAR(128) NOT NULL,
	    validator_id  VARCHAR(128) NOT NULL,
	    cid           VARCHAR(128) NOT NULL,
	    block_number  BIGINT       DEFAULT 0,
	    status        TINYINT      DEFAULT 0,
	    duration      BIGINT       DEFAULT 0,
	    bandwidth     FLOAT        DEFAULT 0,
	    start_time    DATETIME     DEFAULT NULL,
	    end_time      DATETIME     DEFAULT NULL,
		profit        FLOAT        DEFAULT 0,
		processed     BOOLEAN,
		PRIMARY KEY (id),
	    KEY round_node (round_id, node_id)
    ) ENGINE=InnoDB COMMENT='Validation result records';`

var cNodeRegisterTable = `
	CREATE TABLE if not exists %s (
		node_id     VARCHAR(128)  NOT NULL UNIQUE,
		public_key  VARCHAR(1024) DEFAULT '' ,
		create_time VARCHAR(64)   DEFAULT '' ,
		node_type   VARCHAR(64)   DEFAULT '' ,
		PRIMARY KEY (node_id)
	) ENGINE=InnoDB COMMENT='node register info';`

var cAssetRecordTable = `
	CREATE TABLE if not exists %s (
		hash               VARCHAR(128) NOT NULL UNIQUE,
		scheduler_sid      VARCHAR(128) NOT NULL,    
		cid                VARCHAR(128) NOT NULL,
		total_size         BIGINT       DEFAULT 0,
		total_blocks       INT          DEFAULT 0,
		edge_replicas      TINYINT      DEFAULT 0,
		candidate_replicas TINYINT      DEFAULT 0,
		expiration         DATETIME     NOT NULL,
		created_time       DATETIME     DEFAULT CURRENT_TIMESTAMP,
		end_time           DATETIME     DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (hash)
	) ENGINE=InnoDB COMMENT='asset record';`

var cEdgeUpdateTable = `
	CREATE TABLE if not exists %s (
		node_type    INT          NOT NULL UNIQUE,
		app_name     VARCHAR(64)  NOT NULL,
		version      VARCHAR(32)  NOT NULL,
		hash         VARCHAR(128) NOT NULL,
		download_url VARCHAR(128) NOT NULL,
		update_time  DATETIME     DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (node_type)
	) ENGINE=InnoDB COMMENT='edge update info';`

var cValidatorsTable = `
	CREATE TABLE if not exists %s (
		node_id       VARCHAR(128) NOT NULL,
		scheduler_sid VARCHAR(128) NOT NULL,
		PRIMARY KEY (node_id)
	) ENGINE=InnoDB COMMENT='validators';`

var cAssetViewTable = `
	CREATE TABLE if not exists %s (
		node_id       VARCHAR(128) NOT NULL UNIQUE,
		top_hash      VARCHAR(128) NOT NULL,
		bucket_hashes BLOB         NOT NULL,
		PRIMARY KEY (node_id)
	) ENGINE=InnoDB COMMENT='asset view';`

var cBucketTable = `
	CREATE TABLE if not exists %s (
		bucket_id    VARCHAR(128) NOT NULL UNIQUE,
		asset_hashes BLOB         NOT NULL,
		PRIMARY KEY (bucket_id)
	) ENGINE=InnoDB COMMENT='bucket';`

var cWorkloadTable = `
	CREATE TABLE if not exists %s (
		token_id        VARCHAR(128) NOT NULL UNIQUE,
		node_id         VARCHAR(128) NOT NULL,
		client_id       VARCHAR(128) NOT NULL,
		asset_id        VARCHAR(128) NOT NULL,
		limit_rate      INT          DEFAULT 0,
		create_time     DATETIME     NOT NULL,
		expiration      DATETIME     NOT NULL,
		client_workload MEDIUMBLOB ,
		node_workload   MEDIUMBLOB ,
		PRIMARY KEY (token_id)
	) ENGINE=InnoDB COMMENT='workload report';`

var cAssetEventTable = `
	CREATE TABLE if not exists %s (
		id            INT UNSIGNED AUTO_INCREMENT,
		hash          VARCHAR(128) NOT NULL,
		event         VARCHAR(50)  NOT NULL,
		created_time  DATETIME     DEFAULT CURRENT_TIMESTAMP,
		requester     VARCHAR(128),
		details       VARCHAR(128),
		PRIMARY KEY (id)
	) ENGINE=InnoDB COMMENT='asset events';`

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
		PRIMARY KEY (hash,node_id),
		KEY idx_node_id (node_id)
	) ENGINE=InnoDB COMMENT='asset replica info';`

var cNodeInfoTable = `
    CREATE TABLE if not exists %s (
	    node_id            VARCHAR(128) NOT NULL UNIQUE,
	    port_mapping       VARCHAR(8)   DEFAULT '',
	    mac_location       VARCHAR(32)  DEFAULT '',
	    cpu_cores          INT          DEFAULT 0,
	    memory             FLOAT        DEFAULT 0,
	    node_name          VARCHAR(64)  DEFAULT '',
	    disk_type          VARCHAR(64)  DEFAULT '',
	    io_system          VARCHAR(64)  DEFAULT '',
	    system_version     VARCHAR(64)  DEFAULT '',
	    nat_type           VARCHAR(32)  DEFAULT '',
	    disk_space         FLOAT        DEFAULT 0,
    	bandwidth_up       INT          DEFAULT 0,
    	bandwidth_down     INT          DEFAULT 0,
	    scheduler_sid      VARCHAR(128) NOT NULL,
		first_login_time   DATETIME     DEFAULT CURRENT_TIMESTAMP,
	    online_duration    INT          DEFAULT 0,
	    profit             FLOAT        DEFAULT 0,
	    last_seen          DATETIME     DEFAULT CURRENT_TIMESTAMP,
	    disk_usage         FLOAT        DEFAULT 0,
    	upload_traffic     BIGINT       DEFAULT 0,
    	download_traffic   BIGINT       DEFAULT 0,		
    	retrieve_count     INT          DEFAULT 0,	
    	asset_count        INT          DEFAULT 0,
		deactivate_time    INT          DEFAULT 0,
	    PRIMARY KEY (node_id)
	) ENGINE=InnoDB COMMENT='node info';`

var cValidationResultsTable = `
    CREATE TABLE if not exists %s (
		id                INT UNSIGNED AUTO_INCREMENT,
	    round_id          VARCHAR(128) NOT NULL,
	    node_id           VARCHAR(128) NOT NULL,
	    validator_id      VARCHAR(128) NOT NULL,
	    cid               VARCHAR(128) NOT NULL,
	    block_number      BIGINT       DEFAULT 0,
	    status            TINYINT      DEFAULT 0,
	    duration          BIGINT       DEFAULT 0,
	    bandwidth         FLOAT        DEFAULT 0,
	    start_time        DATETIME     DEFAULT NULL,
	    end_time          DATETIME     DEFAULT NULL,
		profit            FLOAT        DEFAULT 0,
		calculated_profit BOOLEAN,
		token_id          VARCHAR(128) DEFAULT '',
		file_saved        BOOLEAN,
		PRIMARY KEY (id),
	    KEY round_node (round_id, node_id),
		KEY idx_profit (calculated_profit),
		KEY idx_file  (file_saved)
    ) ENGINE=InnoDB COMMENT='Validation result records';`

var cNodeRegisterTable = `
	CREATE TABLE if not exists %s (
		node_id         VARCHAR(128)  NOT NULL UNIQUE,
		public_key      VARCHAR(1024) DEFAULT '' ,
		created_time    VARCHAR(64)   DEFAULT '' ,
		node_type       VARCHAR(64)   DEFAULT '' ,
		activation_key  VARCHAR(128)  DEFAULT '' ,
		ip 				VARCHAR(16)  DEFAULT '' ,
		PRIMARY KEY (node_id)
	) ENGINE=InnoDB COMMENT='node register info';`

var cAssetRecordTable = `
	CREATE TABLE if not exists %s (
		hash               VARCHAR(128) NOT NULL UNIQUE,
		scheduler_sid      VARCHAR(128) NOT NULL,    
		cid                VARCHAR(128) NOT NULL,
		total_size         BIGINT       DEFAULT 0,
		total_blocks       INT          DEFAULT 0,
		edge_replicas      INT          DEFAULT 0,
		candidate_replicas INT          DEFAULT 0,
		expiration         DATETIME     NOT NULL,
		created_time       DATETIME     DEFAULT CURRENT_TIMESTAMP,
		end_time           DATETIME     DEFAULT CURRENT_TIMESTAMP,
		bandwidth          INT          DEFAULT 0,
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
		sync_time     DATETIME     DEFAULT CURRENT_TIMESTAMP,
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
		created_time    DATETIME     NOT NULL,
		expiration      DATETIME     NOT NULL,
		client_workload BLOB ,
		node_workload   BLOB ,
		status          TINYINT      DEFAULT 0,
		client_end_time INT          DEFAULT 0,
		PRIMARY KEY (token_id),
		KEY idx_node_id (node_id),
		KEY idx_status (status)
	) ENGINE=InnoDB COMMENT='workload report';`

var cUserAssetTable = `
    CREATE TABLE if not exists %s (
	    hash              VARCHAR(128) NOT NULL,
	    user_id           VARCHAR(128) NOT NULL,
	    asset_name        VARCHAR(128) DEFAULT '' ,
		asset_type        VARCHAR(128) DEFAULT '' ,
		share_status      TINYINT      DEFAULT 0,
	    created_time      DATETIME     DEFAULT CURRENT_TIMESTAMP,
		total_size        BIGINT       DEFAULT 0,
		expiration        DATETIME     DEFAULT CURRENT_TIMESTAMP,
		password          VARCHAR(128) DEFAULT '' ,		
		group_id          INT          DEFAULT 0,
		PRIMARY KEY (hash,user_id),
		KEY idx_user_id (user_id),
		KEY idx_group_id (group_id)
    ) ENGINE=InnoDB COMMENT='user asset';`

var cUserInfoTable = `
    CREATE TABLE if not exists %s (
	    user_id             VARCHAR(128) NOT NULL,
		total_storage_size 	BIGINT      DEFAULT 0,
		used_storage_size 	BIGINT      DEFAULT 0,
		api_keys		    BLOB,
		total_traffic       BIGINT      DEFAULT 0,
		peak_bandwidth 	    INT         DEFAULT 0,
		download_count 	    INT         DEFAULT 0,
		enable_vip  	    BOOLEAN 	DEFAULT false,
		update_peak_time    DATETIME    DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (user_id)
    ) ENGINE=InnoDB COMMENT='user infos';`

var cReplicaEventTable = `
    CREATE TABLE if not exists %s (
		hash          VARCHAR(128) NOT NULL,
		event         TINYINT      DEFAULT 0,
		node_id       VARCHAR(128) NOT NULL,
		cid           VARCHAR(128) DEFAULT '',
		total_size    BIGINT       DEFAULT 0,
	    end_time      DATETIME     DEFAULT CURRENT_TIMESTAMP,
		expiration    DATETIME     DEFAULT CURRENT_TIMESTAMP,
		KEY idx_hash (hash),
		KEY idx_node_id (node_id)
	) ENGINE=InnoDB COMMENT='asset replica event';`

var cRetrieveEventTable = `
    CREATE TABLE if not exists %s (
		token_id        VARCHAR(128) NOT NULL UNIQUE,
		node_id         VARCHAR(128) NOT NULL,
		client_id       VARCHAR(128) NOT NULL,
		cid             VARCHAR(128) NOT NULL,
		size            INT          DEFAULT 0,
		created_time    INT          DEFAULT 0,
		end_time        INT          DEFAULT 0,
	    profit          FLOAT        DEFAULT 0,
		PRIMARY KEY (token_id),
		KEY idx_node_id (node_id)
	) ENGINE=InnoDB COMMENT='asset retrieve event';`

var cAssetVisitCountTable = `
    CREATE TABLE if not exists %s (
	    hash        VARCHAR(128) NOT NULL,
		count       INT 		 DEFAULT 0,
		PRIMARY KEY (hash)
    ) ENGINE=InnoDB COMMENT='user asset visit count';`

var cReplenishBackupTable = `
    CREATE TABLE if not exists %s (
	    hash        VARCHAR(128) NOT NULL,
		PRIMARY KEY (hash)
    ) ENGINE=InnoDB COMMENT='Assets that need to be replenish backed up to candidate nodes';`

var cUserAssetGroupTable = `
    CREATE TABLE if not exists %s (
		id            INT UNSIGNED AUTO_INCREMENT,
	    user_id       VARCHAR(128) NOT NULL,
		name          VARCHAR(32)  DEFAULT '',
		parent        INT          DEFAULT 0,
	    created_time  DATETIME     DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (id),
	    KEY idx_user_id (user_id),
	    KEY idx_parent (parent)
    ) ENGINE=InnoDB COMMENT='user asset group';`

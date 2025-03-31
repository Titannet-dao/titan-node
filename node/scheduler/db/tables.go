package db

// Table creation SQL statements.
var cAssetStateTable = `
    CREATE TABLE if not exists %s (
		hash                VARCHAR(128) NOT NULL UNIQUE,
		state               VARCHAR(32)  DEFAULT '',
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
		start_time    DATETIME     DEFAULT CURRENT_TIMESTAMP,
		total_size    BIGINT       DEFAULT 0,
		client_id     VARCHAR(128) DEFAULT '',
		speed         BIGINT       DEFAULT 0,
		workload_id   VARCHAR(128) DEFAULT '',
		PRIMARY KEY (hash,node_id),
		KEY idx_node_id (node_id),
		KEY idx_status (status),
		KEY idx_hash (hash)
	) ENGINE=InnoDB COMMENT='asset replica info';`

var cNodeInfoTable = `
    CREATE TABLE if not exists %s (
	    node_id              VARCHAR(128)    NOT NULL UNIQUE,
	    port_mapping         VARCHAR(8)      DEFAULT '',
	    mac_location         VARCHAR(32)     DEFAULT '',
	    cpu_cores            INT             DEFAULT 0,
	    cpu_info             VARCHAR(128)    DEFAULT '',
	    gpu_info             VARCHAR(128)    DEFAULT '',
	    memory               FLOAT           DEFAULT 0,
	    node_name            VARCHAR(64)     DEFAULT '',
	    disk_type            VARCHAR(64)     DEFAULT '',
	    io_system            VARCHAR(64)     DEFAULT '',
	    system_version       VARCHAR(64)     DEFAULT '',
		version              INT             DEFAULT 0,
	    disk_space           FLOAT           DEFAULT 0,
		available_disk_space FLOAT           DEFAULT 0,
		titan_disk_usage     DOUBLE          DEFAULT 0,
    	bandwidth_up         BIGINT          DEFAULT 0,
    	bandwidth_down       BIGINT          DEFAULT 0,
		netflow_up           BIGINT          DEFAULT 0,
		netflow_down         BIGINT          DEFAULT 0,
	    scheduler_sid        VARCHAR(128)    NOT NULL,
		first_login_time     DATETIME        DEFAULT CURRENT_TIMESTAMP,
	    online_duration      INT             DEFAULT 0,
	    offline_duration     INT             DEFAULT 0,
	    profit               DECIMAL(20, 6)  DEFAULT 0,
		penalty_profit       DECIMAL(20, 6)  DEFAULT 0,
	    last_seen            DATETIME        DEFAULT CURRENT_TIMESTAMP,
	    disk_usage           FLOAT           DEFAULT 0,
    	upload_traffic       BIGINT          DEFAULT 0,
    	download_traffic     BIGINT          DEFAULT 0,		
		deactivate_time      INT             DEFAULT 0,		
		free_up_disk_time    DATETIME        DEFAULT '2024-04-20 12:10:15',
		ws_server_id         VARCHAR(128)    DEFAULT '',
		force_offline        BOOLEAN         DEFAULT false,
	    nat_type             VARCHAR(32)     DEFAULT 'UnknowNAT',
	    PRIMARY KEY (node_id),
		KEY idx_last_seen (last_seen)
	) ENGINE=InnoDB COMMENT='node info';`

var cNodeStatisticsTable = `
    CREATE TABLE if not exists %s (
	    node_id                  VARCHAR(128)   NOT NULL UNIQUE,
		retrieve_count           INT            DEFAULT 0,
		retrieve_succeeded_count INT            DEFAULT 0,
		retrieve_failed_count    INT            DEFAULT 0,
		asset_count              INT            DEFAULT 0,
		asset_succeeded_count    INT            DEFAULT 0,
		asset_failed_count       INT            DEFAULT 0,
    	project_count            INT            DEFAULT 0,	
    	project_succeeded_count  INT            DEFAULT 0,	
    	project_failed_count     INT            DEFAULT 0,
		update_time              DATETIME       DEFAULT CURRENT_TIMESTAMP,
	    PRIMARY KEY (node_id)
	) ENGINE=InnoDB COMMENT='node statistics';`

var cNodeRetrieveTable = `
    CREATE TABLE if not exists %s (
	    trace_id      VARCHAR(128)   DEFAULT '',
 	    node_id       VARCHAR(128)   NOT NULL,
	    client_id     VARCHAR(128)   DEFAULT '',
		hash          VARCHAR(128)   NOT NULL,
		speed         BIGINT         DEFAULT 0,
		size          BIGINT         DEFAULT 0,		
	    status        TINYINT        DEFAULT 0,
		created_time  DATETIME       DEFAULT CURRENT_TIMESTAMP,
		KEY idx_trace_id  (trace_id),
		KEY idx_node_id  (node_id),
		KEY idx_hash_id  (hash),
		KEY idx_client_id (client_id),
		KEY idx_created_time (created_time)
	) ENGINE=InnoDB COMMENT='node retrieve record';`

var cValidationResultsTable = `
    CREATE TABLE if not exists %s (
		id                INT UNSIGNED   AUTO_INCREMENT,
	    round_id          VARCHAR(128)   NOT NULL,
	    node_id           VARCHAR(128)   NOT NULL,
	    validator_id      VARCHAR(128)   NOT NULL,
	    cid               VARCHAR(128)   DEFAULT '',
	    block_number      BIGINT         DEFAULT 0,
	    status            TINYINT        DEFAULT 0,
	    duration          BIGINT         DEFAULT 0,
	    bandwidth         FLOAT          DEFAULT 0,
	    start_time        DATETIME       DEFAULT CURRENT_TIMESTAMP,
	    end_time          DATETIME       DEFAULT CURRENT_TIMESTAMP,
		profit            DECIMAL(14, 6) DEFAULT 0,
		calculated_profit BOOLEAN,
		token_id          VARCHAR(128)   DEFAULT '',
		file_saved        BOOLEAN,
		node_count        INT            DEFAULT 0,
		PRIMARY KEY (id),
	    KEY round_node (round_id, node_id),
		KEY idx_profit (calculated_profit),
		KEY idx_round_id  (round_id),
		KEY idx_node_id  (node_id),
		KEY idx_file  (file_saved),
		KEY idx_start_time  (start_time)
    ) ENGINE=InnoDB COMMENT='Validation result records';`

var cNodeRegisterTable = `
	CREATE TABLE if not exists %s (
		node_id         VARCHAR(128)  NOT NULL UNIQUE,
		public_key      VARCHAR(1024) DEFAULT '' ,
		created_time    VARCHAR(64)   DEFAULT '' ,
		node_type       VARCHAR(64)   DEFAULT '' ,
		activation_key  VARCHAR(128)  DEFAULT '' ,
		ip 				VARCHAR(16)   DEFAULT '' ,
		migrate_key     VARCHAR(128)  DEFAULT '' ,		
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
		bandwidth          BIGINT       DEFAULT 0,
		note               VARCHAR(128) DEFAULT '',
		source             TINYINT      DEFAULT 0,
		owner              VARCHAR(128) DEFAULT '',
		PRIMARY KEY (hash),
		KEY idx_end_time (end_time)
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
		workload_id     VARCHAR(128) NOT NULL UNIQUE,
		client_id       VARCHAR(128) DEFAULT '',
		asset_cid       VARCHAR(128) NOT NULL,
		created_time    DATETIME     DEFAULT CURRENT_TIMESTAMP,
		workloads       BLOB ,
		client_end_time DATETIME     DEFAULT CURRENT_TIMESTAMP,
		asset_size      BIGINT       DEFAULT 0,
		status          TINYINT      DEFAULT 0,
		event           TINYINT      DEFAULT 0,
		PRIMARY KEY (workload_id),
		KEY idx_client_id (client_id),
		KEY idx_end_time (client_end_time)
	) ENGINE=InnoDB COMMENT='workload report';`

var cReplicaEventTable = `
    CREATE TABLE if not exists %s (
		hash          VARCHAR(128)  NOT NULL,
		event         TINYINT       DEFAULT 0,
		node_id       VARCHAR(128)  NOT NULL,
		cid           VARCHAR(128)  DEFAULT '',
		total_size    BIGINT        DEFAULT 0,
		done_size     BIGINT        DEFAULT 0,
	    created_time  DATETIME      DEFAULT CURRENT_TIMESTAMP,
		source        TINYINT       DEFAULT 0,
	    client_id     VARCHAR(128)  DEFAULT '',
		speed         BIGINT        DEFAULT 0,
		trace_id      VARCHAR(128)  DEFAULT '', 
		msg           VARCHAR(1024) DEFAULT '', 
		KEY idx_hash (hash),
		KEY idx_node_id (node_id),
		KEY idx_client_id (client_id),
		KEY idx_trace_id (trace_id),
		KEY idx_event (event),
		KEY idx_created_time (created_time)
	) ENGINE=InnoDB COMMENT='asset replica event';`

var cReplenishBackupTable = `
    CREATE TABLE if not exists %s (
	    hash        VARCHAR(128) NOT NULL,
		PRIMARY KEY (hash)
    ) ENGINE=InnoDB COMMENT='Assets that need to be replenish backed up to candidate nodes';`

var cAWSDataTable = `
    CREATE TABLE if not exists %s (
	    bucket          VARCHAR(128) NOT NULL,
		cid             VARCHAR(128) DEFAULT '',
		replicas        INT          DEFAULT 0,
		is_distribute   BOOLEAN      DEFAULT false,
		distribute_time DATETIME     DEFAULT CURRENT_TIMESTAMP,
		size            FLOAT        DEFAULT 0,
		PRIMARY KEY (bucket)
    ) ENGINE=InnoDB COMMENT='aws data';`

var cAssetDataTable = `
    CREATE TABLE if not exists %s (
		cid             VARCHAR(128) NOT NULL,
		hash            VARCHAR(128) NOT NULL,
		replicas        INT          DEFAULT 0,
		status          TINYINT      DEFAULT 0,
		distribute_time DATETIME     DEFAULT CURRENT_TIMESTAMP,
		owner           VARCHAR(128) DEFAULT '',
		expiration      DATETIME     NOT NULL,
		PRIMARY KEY (cid)
    ) ENGINE=InnoDB COMMENT='asset data';`

var cProfitDetailsTable = `
    CREATE TABLE if not exists %s (
		id            INT UNSIGNED   AUTO_INCREMENT,
		node_id       VARCHAR(128)   DEFAULT '',
		profit        DECIMAL(14, 6) DEFAULT 0,
		created_time  DATETIME       DEFAULT CURRENT_TIMESTAMP,
		size          BIGINT         DEFAULT 0,
		profit_type   INT            NOT NULL,
		note          VARCHAR(1024)  DEFAULT '', 
		cid           VARCHAR(128)   DEFAULT '',
		rate          DECIMAL(5, 4)  DEFAULT 0,
		PRIMARY KEY (id),
	    KEY idx_node_id (node_id),
	    KEY profit_type_id (profit_type),
	    KEY idx_time (created_time)
    ) ENGINE=InnoDB COMMENT='profit details';`

var cCandidateCodeTable = `
    CREATE TABLE if not exists %s (
	    code          VARCHAR(128)   NOT NULL,		
		expiration    DATETIME       DEFAULT CURRENT_TIMESTAMP,
		node_type     INT            NOT NULL,
		node_id       VARCHAR(128)   DEFAULT '',
		is_test 	  BOOLEAN        DEFAULT false,
		PRIMARY KEY (code)
    ) ENGINE=InnoDB COMMENT='candidate code';`

// Table creation SQL statements.
var cProjectStateTable = `
    CREATE TABLE if not exists %s (
		id                  VARCHAR(128) NOT NULL UNIQUE,	
		state               VARCHAR(32)  DEFAULT 0,		
		retry_count         INT          DEFAULT 0,
		replenish_replicas  INT          DEFAULT 0,
		PRIMARY KEY (id)
	) ENGINE=InnoDB COMMENT='project state info';`

var cProjectInfosTable = `
    CREATE TABLE if not exists %s (
		id            VARCHAR(128)   NOT NULL UNIQUE,
		user_id       VARCHAR(128)   DEFAULT '',
		bundle_url    VARCHAR(128)   DEFAULT '',
		name          VARCHAR(128)   DEFAULT '',	
		created_time  DATETIME       DEFAULT CURRENT_TIMESTAMP,		
		replicas      INT            DEFAULT 0,
		type          TINYINT        DEFAULT 0,
		scheduler_sid VARCHAR(128)   NOT NULL,   
		expiration    DATETIME       DEFAULT CURRENT_TIMESTAMP,	
		requirement   BLOB ,
		PRIMARY KEY (id),
	    KEY idx_user_id (user_id),
	    KEY idx_time (created_time),
	    KEY idx_expiration (expiration)
    ) ENGINE=InnoDB COMMENT='project info';`

var cProjectReplicasTable = `
    CREATE TABLE if not exists %s (
		id                 VARCHAR(128)  NOT NULL,
		node_id            VARCHAR(128)  NOT NULL,	
		status             TINYINT       DEFAULT 0,		
		created_time       DATETIME      DEFAULT CURRENT_TIMESTAMP,
		end_time           DATETIME      DEFAULT CURRENT_TIMESTAMP,
    	upload_traffic     BIGINT        DEFAULT 0,
    	download_traffic   BIGINT        DEFAULT 0,	
		type               TINYINT       DEFAULT 0,
		time               INT           DEFAULT 0,
		max_delay          INT           DEFAULT 0,
		min_delay          INT           DEFAULT 0,
		avg_delay          INT           DEFAULT 0,
		PRIMARY KEY (id,node_id),
	    KEY idx_time (created_time),
		KEY idx_node_id (node_id),
		KEY idx_type (type),
		KEY idx_id (id)
    ) ENGINE=InnoDB COMMENT='project replicas';`

var cProjectEventTable = `
    CREATE TABLE if not exists %s (
		id            VARCHAR(128)  NOT NULL,
		event         TINYINT       DEFAULT 0,
		node_id       VARCHAR(128)  NOT NULL,
		created_time  DATETIME      DEFAULT CURRENT_TIMESTAMP,
		KEY idx_id (id),
		KEY idx_node_id (node_id),
		KEY idx_time (created_time)
	) ENGINE=InnoDB COMMENT='project replica event';`

var cOnlineCountTable = `
	CREATE TABLE if not exists %s (
		node_id         VARCHAR(128)  NOT NULL,
		created_time    DATETIME      NOT NULL,
		online_count    INT           DEFAULT 0,
		PRIMARY KEY (node_id,created_time)
	) ENGINE=InnoDB COMMENT='node and server online count';`

var cAssetDownloadTable = `
	CREATE TABLE if not exists %s (
		node_id         VARCHAR(128)  NOT NULL,
		hash            VARCHAR(128)  NOT NULL,
		created_time    DATETIME      DEFAULT CURRENT_TIMESTAMP,
 		total_traffic   BIGINT        DEFAULT 0,
		peak_bandwidth 	BIGINT        DEFAULT 0,
		user_id         VARCHAR(128)  DEFAULT '',
 		KEY idx_hash_id (hash),
 		KEY idx_node_id (node_id)
	) ENGINE=InnoDB COMMENT='node and server online count';`

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

var cBandwidthEventTable = `
    CREATE TABLE if not exists %s (
	    node_id             VARCHAR(128) NOT NULL,
		b_up_peak           BIGINT       DEFAULT 0,
		b_down_peak         BIGINT       DEFAULT 0,
		b_up_free           BIGINT       DEFAULT 0,
		b_down_free         BIGINT       DEFAULT 0,
		b_up_load           BIGINT       DEFAULT 0,
		b_down_load         BIGINT       DEFAULT 0,
		size                BIGINT       DEFAULT 0,
		task_success        INT          DEFAULT 0,
		task_total          INT          DEFAULT 0,
		score               INT          DEFAULT 0,
	    created_time        DATETIME     DEFAULT CURRENT_TIMESTAMP,
	    KEY idx_node_id (node_id),
	    KEY idx_created_time (created_time)
    ) ENGINE=InnoDB COMMENT='node bandwidth event';`

var cServiceEventTable = `
    CREATE TABLE if not exists %s (
	    trace_id      VARCHAR(128) DEFAULT '',
	    node_id       VARCHAR(128) NOT NULL,
		type          TINYINT      DEFAULT 0,
		size          BIGINT       DEFAULT 0,
		status        TINYINT      DEFAULT 0,
		peak          BIGINT       DEFAULT 0,
		speed         BIGINT       DEFAULT 0,
		score         INT          DEFAULT 0,
		end_time      DATETIME     DEFAULT CURRENT_TIMESTAMP,
		start_time    DATETIME     DEFAULT CURRENT_TIMESTAMP,
	    KEY idx_node_id (node_id),
	    KEY idx_type (type),
	    KEY idx_status (status),
	    KEY idx_start_time (start_time),
	    KEY idx_end_time (end_time),
	    KEY idx_trace_id (trace_id)
    ) ENGINE=InnoDB COMMENT='node service event';`

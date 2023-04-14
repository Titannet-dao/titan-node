## 1 Build and install Titan
	cd ~
    git clone https://github.com/Filecoin-Titan/titan.git titan
	cd titan
	make
	make install
## 2 Install and start Vitess (this is a simple example, if it is a production environment, please refer to the official documentation to deploy https://vitess.io/docs/16.0 )

Please refer to the official sample example documentation (https://vitess.io/docs/16.0/get-started/local/)
Operate up to the 'Start a Single Keyspace Cluster' step
Next, create a Keyspace for titan in Cluster, and initialize the data tables required by titan in the Keyspace, set mysql user and password for titan

### 2.1 Copy the vitess example folder to the titan vitess-titan directory
    cp -r examples ~/titan/vitess-titan
    cp -r web ~/titan/vitess-titan
    cd ~/titan/vitess-titan
### 2.2 Create a mysql user and password for titan
    The mysql user and password are configured in mysql_auth_server_static_creds.json
    You need to add mysql_auth_server_static_creds to the vtgate-up.sh script

    sed -i "s/--mysql_auth_server_impl none/--mysql_auth_server_impl static --mysql_auth_server_static_file mysql_auth_server_static_creds.json /g" examples/common/scripts/vtgate-up.sh
### 2.3 Start the initialization cluster script
    bash titan_initial_cluster.sh

## 3 Install and start etcd(this is a simple example, if it is a production environment, please refer to the official documentation to deploy https://etcd.io/docs/v3.4/op-guide/clustering/)
    cd ~
    version=3.4.24
    file=etcd-v${version}-linux-amd64.tar.gz
    wget https://github.com/etcd-io/etcd/releases/download/v${version}/${file}
    tar -xzf ${file}
    cd ${file/.tar.gz/}
    nohup ./etcd -listen-client-urls="my_etcd_cluster_addresses" --advertise-client-urls="my_etcd_cluster_addresses" > etcd.log 2>&1 &

## 4 Run Scheduler
###  4.1 Execute the initialization command, this command will generate the config file in the root directory
    titan-scheduler init
###  4.2 Modify the config file
    You need to change the mysql address, etcd address, and scheduler extranet url in the config
    
    vi ~/.titanscheduler/config.toml
    DatabaseAddress = "mysql_user:mysql_password@tcp(mysql_address)/mysql_database"
    EtcdAddresses = ["my_etcd_cluster_addresses"]
    ExternalURL = "https://my-scheduler-external-ip:3456/rpc/v0"
### 4.3 Run
    titan-scheduler run

## 5 Run Locator
###  5.1 Download geodb
    Download geodb from [GeoLite2 City](http://dev.maxmind.com/geoip/geoip2/geolite2/), 
    we are currently using geodb to get ip location information for now. This has the drawback that the geodb is local and cannot be dynamically updated.
###  5.2 Run
    titan-locator run --geodb-path ./city.mmdb --etcd-addresses "my_etcd_cluster_addresses"
    Note: city.mmdb is a separate file downloaded from GeoLite2 City, --etcd-addresses is the address of the etcd cluster

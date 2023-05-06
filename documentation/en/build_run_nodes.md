## 1 Registering nodes from the web 
[Login to titannet](https://www.titannet.io/) 
And Register Node
When registration is complete, you will get the node-id and private-key and area-id

## 2 Import the private key to the node
Both Candidate and Edge need to import the private key to join the titan network, here is the example of Candidate

    titan-candidate key import --path /path/to/private.key

## 3 Modify the config
Both Candidate and Edge need to configure node-id and area-id to join titan network , here is the example of Candidate

    titan-candidate config set --node-id="your_node_id" --area-id="your_area_id" 

    For more configurations see below:
        --listen-address       The local address that the node listens to, Edge defaults to 0.0.0.0:1234, Candidate defaults to 0.0.0.0:2345
        --timeout              Network connection timeout, default 30 seconds
        --metadata-path        Metadata Storage Path
        --assets-paths         Asset data storage paths, can be configured multiple, for example: --assets-paths="/path/to/assets1, /path/to/assets2"
        --bandwidth-up         Uplink bandwidth
        --bandwidth-down       Downlink bandwidth
        --locator              Whether to get the url of the scheduler by locator, default is true
        --certificate-path     Https certificate
        --private-key-path     Private key for https certificate
        --ca-certificate-path  Ca certificate, if the scheduler uses self-signed certificate, then this item must be configured
        --fetch-block-timeout  Pull block timeout
        --fetch-block-retry    Number of failed retries to pull block
        --fetch-batch          Batch pull quantity
    If you want to specify the data storage path, you can configure --metadata-path , --assets-paths.

## 4 Set locator or scheduler address
If --locator=true is in step 3, you need to set the locator address:

    export LOCATOR_API_INFO=https://your_locator_server_ip:port

If --locator=false is in step 3, you need to set the scheduler address:

    export SCHEDULER_API_INFO=https://your_scheduler_server_ip:port

## 5 Set file descriptor limit
    ulimit -n 100000

## 6 Run Candidate or Edge
    titan-candidate run
    titan-edge run


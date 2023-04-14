#  Titan CDN+
Titan is a Filecoin extension, backed by Filecoin and focused on last-mile **CDN+** networks.
Currently, Titan's goals are:
 - Focus on the last mile of transmission, aggregating the storage and bandwidth resources of a large number of home devices
 - Accelerated IPFS/Filecoin/normal data retrieving
 - Accelerated IPFS/Filecoin/normal data storing
 - Trusted computing based on Filecoin blockchain/FVM
 
 Similar to traditional CDNs, Titan will pull data from the source, to a cluster of nodes in the Titan network, and the user retrieves the data from the nearest cluster of nodes in the Titan network.
 
We refer to Titan as **CDN+** because:
 - Unlike traditional CDNs, which generally store data in IDCs, users in neighboring cities need to pull data from the same IDC. As a contrast, Titan Network provides data to users in the same city by clustering nodes in each city, so the latency is lower and the bandwidth is larger.
 - Unlike traditional CDNs, which generally only support users to pull data but not to put data,  the Titan network supports users to store private data through **S3 APIs** and provides a superior data storing experience: excellent writing speed and good reliability, and the private data is regularly backed up to Filecoin storage providers.
 - Titan plans to build trusted computing based on the Filecoin block chain/FVM by running containers in the L1 node cluster of the Titan network and utilizing the computing power of the L1 node cluster.

#  Filecoin and Titan
Titan is an extension of Filecoin, backed by Filecoin:
 - Accept CDN deals via FVM contract
 - Titan regularly backs up to Filecion storage provider via Storage deals
 - The proof of work and contribution calculations for nodes in the Titan network, will be published to the Filecoin blockchain via the FVM contract
 - Via FVM DataDAO, Titan backup data to and restore data from Filecoin storage providers

##  Saturn and Titan
As a last-mile focused CDN+, Titan can also serve as the outermost courier of Saturn CDN, using a cluster of Titan network nodes to accelerate the content distribution of Saturn CDN.

#  Titan Next
 - Titan wants to leverage the computing power (CPU/GPU) of the Titan node cluster by running trusted containers and other methods to build trusted computing services.
 - Build sidechain capability, and as a sidechain of Filecoin to improve the TPS of Filecoin main chain.  As a **CDN+** focusing on the last mile, Titan is naturally close to the user, so if it has sidechain capability, Titan can aggregate user requests and use methods such as zk-rollups to improve the throughput of Filecion main chain.

## Building & Documentation

[Build & Run scheduler](documentation/en/build_run_scheduler.md)

[Build & Run nodes](documentation/en/build_run_nodes.md)

[Documentation](documentation/)
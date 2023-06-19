## 1 Apply activation code from the web 
[Login to titannet](https://www.titannet.io/) 
And apply for a private key
When apply is complete, you will get the private key

## 2 Set locator address
    export LOCATOR_API_INFO=https://your_locator_server_ip:port

## 3 Register node
Both Candidate and Edge need to register nodes with private keys to join the titan network, here is the example of Candidate

    titan-candidate register --key your_private_key

## 4 Set file descriptor limit
    ulimit -n 100000

## 5 Run Candidate or Edge
    titan-candidate run
    titan-edge run


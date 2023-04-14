#!/bin/bash

# Copyright 2019 The Vitess Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# this script brings up zookeeper and all the vitess components
# required for a single shard deployment.

source examples/common/env.sh

# start topo server
if [ "${TOPO}" = "zk2" ]; then
	CELL=zone1 examples/common/scripts/zk-up.sh
elif [ "${TOPO}" = "k8s" ]; then
	CELL=zone1 examples/common/scripts/k3s-up.sh
elif [ "${TOPO}" = "consul" ]; then
	CELL=zone1 examples/common/scripts/consul-up.sh
else
	CELL=zone1 examples/common/scripts/etcd-up.sh
fi

# start vtctld
CELL=zone1 examples/common/scripts/vtctld-up.sh

# start vttablets for keyspace commerce
for i in 100 101 102; do
	CELL=zone1 TABLET_UID=$i examples/common/scripts/mysqlctl-up.sh
	CELL=zone1 KEYSPACE=titan TABLET_UID=$i examples/common/scripts/vttablet-up.sh
done

# set the correct durability policy for the keyspace
vtctldclient --server localhost:15999 SetKeyspaceDurabilityPolicy --durability-policy=semi_sync titan || fail "Failed to set keyspace durability policy on the titan keyspace"

# start vtorc
examples/common/scripts/vtorc-up.sh

# Wait for all the tablets to be up and registered in the topology server
# and for a primary tablet to be elected in the shard and become healthy/serving.
wait_for_healthy_shard titan 0 || exit 1

# create the schema
vtctldclient ApplySchema --sql-file create_titan_schema.sql titan || fail "Failed to apply schema for the titan keyspace"

# create the vschema
vtctldclient ApplyVSchema --vschema-file vschema_titan_initial.json titan || fail "Failed to apply vschema for the titan keyspace"

# start vtgate
CELL=zone1 examples/common/scripts/vtgate-up.sh

# start vtadmin
examples/common/scripts/vtadmin-up.sh

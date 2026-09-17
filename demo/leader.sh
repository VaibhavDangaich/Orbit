#!/usr/bin/env bash
# Prints the container name of the scheduler currently holding leadership,
# read from etcd rather than from logs.
#
# Logs can't answer this question reliably: after one failover, BOTH
# schedulers have logged "elected leader" at some point, so grepping for it
# tells you who has ever led, not who leads now. etcd holds the single
# authoritative answer -- the election key exists only while its lease is
# alive, and its value is the nodeID of whoever holds it.
#
# That value is "<hostname>-<pid>", and a container's hostname is its own
# short ID (cmd/scheduler/main.go defaultNodeID), which is what makes the
# mapping back to a container name possible at all.
set -euo pipefail

etcd_container=${ORBIT_ETCD_CONTAINER:-compose-etcd-1}
key=${ORBIT_ELECTION_KEY:-/orbit/scheduler-leader/}

node=$(docker exec "$etcd_container" etcdctl get --prefix --print-value-only "$key" | head -1)

if [[ -z "$node" ]]; then
  echo "no leader: election key '$key' is empty (no scheduler running, or none has won yet)" >&2
  exit 1
fi

# Strip the -<pid> suffix to recover the container hostname / short ID.
short=${node%-*}

name=$(docker ps --filter "id=$short" --format '{{.Names}}')
if [[ -z "$name" ]]; then
  # A leader that isn't a local container -- e.g. a kind pod campaigning on
  # the same key against the same etcd. Still a real answer, so report the
  # raw nodeID rather than pretending there's no leader.
  echo "$node (not a local container)"
  exit 0
fi

echo "$name"

#!/usr/bin/env bash
# The failover demo, as a script rather than a list of keystrokes, so it
# can be recorded non-interactively (asciinema rec -c) and re-run by hand.
# Driving it from a script is what makes the demo possible at all: it has
# to watch logs and kill containers at the same time, which one pair of
# hands at one prompt can't do.
#
# Precondition: two schedulers and some workers already running --
#   docker compose -f deploy/compose/docker-compose.yml --profile app up -d \
#     --scale scheduler=2 --scale worker=3
set -uo pipefail
cd "$(dirname "$0")/.."
export COMPOSE_FILE=deploy/compose/docker-compose.yml

dim=$'\033[2m'; bold=$'\033[1m'; cyan=$'\033[36m'; green=$'\033[32m'; yellow=$'\033[33m'; off=$'\033[0m'

say()  { echo; echo "${cyan}# $*${off}"; }
run()  { echo "${bold}\$ $*${off}"; "$@"; }
beat() { sleep "${1:-2}"; }

# Prints the pipeline and then runs exactly that string, so what the
# recording shows is what actually executed -- no display/command drift.
sh_run() { echo "${bold}\$ $1${off}"; eval "$1"; }

say "Two schedulers are running. Exactly one of them leads."
run docker compose ps --format '{{.Name}}  {{.Status}}' | grep scheduler
beat 2

say "etcd holds the answer -- the election key exists only while its lease is alive."
run ./demo/leader.sh
beat 2

leader=$(./demo/leader.sh) || { echo "no leader; is the app profile up?" >&2; exit 1; }

say "CRASH: SIGKILL can't be caught, so nothing gets to resign."
echo "${dim}  The standby has to outwait a lease nobody is renewing (ORBIT_ELECTION_TTL=10s).${off}"
beat 1
killed_at=$(date -u +%H:%M:%S)
run docker kill "$leader"
echo "${yellow}  killed at ${killed_at}${off}"

echo
echo "${dim}  waiting for the standby to notice...${off}"
# Poll until a DIFFERENT, still-running container holds the key. The
# "!= $leader" test alone is not enough: for the first few seconds after
# the kill, etcd still reports the dead process's nodeID, and leader.sh
# renders that as "<nodeID> (not a local container)" -- a value that
# already differs from the old container name and would end the wait
# immediately, before any promotion has happened.
for _ in $(seq 1 30); do
  new=$(./demo/leader.sh 2>/dev/null || true)
  [[ -n "$new" && "$new" != "$leader" && "$new" != *"not a local container"* ]] && break
  sleep 1
done
echo "${dim}  new leader: ${new}${off}"
beat 1
sh_run 'docker compose logs -t --since 60s scheduler | grep "elected leader" | tail -2'
echo "${green}  ^ promoted ~8s after the kill -- that gap IS the lease expiring${off}"
beat 3

say "The killed container does NOT come back on its own."
echo "${dim}  Docker suppresses restart: unless-stopped for an operator kill.${off}"
run docker inspect "$leader" --format 'RestartPolicy={{.HostConfig.RestartPolicy.Name}}  Status={{.State.Status}}  RestartCount={{.RestartCount}}'
beat 2
run docker start "$leader"
beat 3

say "GRACEFUL: SIGTERM is caught, and the leader calls Resign() on its way out."
echo "${dim}  Deleting the key immediately means the standby is promoted at once.${off}"
beat 1
leader=$(./demo/leader.sh)
run docker stop "$leader"
beat 2
sh_run 'docker compose logs -t --since 30s scheduler | grep -E "resigned leadership|elected leader" | tail -2'
echo "${green}  ^ same second, microseconds apart -- versus ~8s for the crash${off}"
beat 3

say "Jobs never stopped firing through either one."
sh_run 'docker compose logs --since 120s worker | grep -c executed'
beat 2
echo
echo "${bold}  graceful: microseconds     crash: ~8s     the difference is one Resign() call.${off}"
beat 2

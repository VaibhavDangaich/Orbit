# Regenerate with:  vhs demo/failover.tape   (from the repo root)
#
# Requires two schedulers already running:
#   docker compose -f deploy/compose/docker-compose.yml --profile app up -d --build \
#     --scale scheduler=2 --scale worker=3
#
# Order is crash-then-graceful, and the `docker start` between them is
# NOT optional. `restart: unless-stopped` does not bring back a container
# an operator killed -- Docker suppresses the restart policy for manual
# stops and kills alike (verified: RestartCount stays 0 after docker
# kill). Without that explicit restart, the crash demo leaves one
# scheduler running and the graceful demo then has no standby to promote.

Output docs/failover.gif

Set Shell bash
Set FontSize 14
Set Width 1200
Set Height 700
Set Padding 25
Set TypingSpeed 25ms
# 10fps keeps the file small enough to sit in a README; nothing here moves
# fast enough to need more, and the proof is in the log timestamps anyway.
Set Framerate 10

# Shorten the command line without faking it -- COMPOSE_FILE is the real
# env var docker compose reads, so every command below is one you can type.
Hide
Type "export COMPOSE_FILE=deploy/compose/docker-compose.yml"
Enter
Type "clear"
Enter
Sleep 1s
Show

Type "# Two schedulers running. Exactly one of them is the leader."
Enter
Type "docker compose ps --format '{{.Name}}  {{.Status}}' | grep scheduler"
Enter
Sleep 3s

Type "docker compose logs scheduler | grep -E 'campaigning|elected leader'"
Enter
Sleep 4s

Type "# etcd holds the answer -- the key exists only while its lease is alive."
Enter
Type "./demo/leader.sh"
Enter
Sleep 3s

Type "clear"
Enter
Sleep 500ms

Type "# CRASH: SIGKILL can't be caught, so nothing resigns."
Enter
Type "# The standby has to wait out the dead lease (ORBIT_ELECTION_TTL=10s)."
Enter
Sleep 1s
Type "docker kill $(./demo/leader.sh)"
Enter
Sleep 2s

Type "docker compose logs -t -f --since 1s scheduler | grep --line-buffered 'elected leader'"
Enter
# The long pause here is the whole point: watch the wall clock while
# nothing happens, then the promotion lands ~10s after the kill.
Sleep 15s
Ctrl+C
Sleep 2s

Type "# The killed container does NOT come back on its own -- Docker ignores"
Enter
Type "# restart: unless-stopped for an operator kill. Bring it back as the standby:"
Enter
Type "docker start $(docker ps -aq --filter status=exited --filter name=compose-scheduler | head -1)"
Enter
Sleep 4s

Type "clear"
Enter
Sleep 500ms

Type "# GRACEFUL: SIGTERM is caught, and the leader calls Resign() on the way out."
Enter
Type "# Deleting the key immediately means the standby promotes in milliseconds."
Enter
Sleep 1s
Type "docker stop $(./demo/leader.sh)"
Enter
Sleep 5s

# The outgoing leader's "resigned leadership" next to the incoming one's
# "elected leader" is the whole proof: same second, milliseconds apart,
# versus the ~10s TTL gap above. 'stopped' stays in the pattern as a
# fallback -- it's logged unconditionally on shutdown, so the demo still
# shows the handoff even if the resign line isn't there.
Type "docker compose logs -t --since 20s scheduler | grep -E 'resigned|stopped|elected leader'"
Enter
Sleep 5s

Type "# Jobs never stopped firing through either one."
Enter
Type "docker compose logs --since 60s worker | grep -c 'executed'"
Enter
Sleep 4s

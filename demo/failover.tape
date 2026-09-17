# Regenerate with:  vhs demo/failover.tape   (from the repo root)
#
# Requires two schedulers already running:
#   docker compose -f deploy/compose/docker-compose.yml --profile app up -d --build \
#     --scale scheduler=2 --scale worker=3
#
# Order is crash-then-graceful, not the other way round, and that's
# deliberate: each demo needs a live standby to promote. Killing first
# leaves the killed container to restart (restart: unless-stopped) and
# rejoin as the standby, which is exactly what the graceful demo then needs.
# Doing it the other way would leave `docker stop` with nothing to hand
# leadership to, since a stopped container stays stopped.

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

# 'stopped', not 'resign': a SUCCESSFUL Resign() logs nothing (scheduler
# main.go only logs resign on error), so the outgoing leader's last line is
# "stopped". Pairing it with the incoming leader's "elected leader"
# timestamp is what shows the gap -- milliseconds here, vs ~10s above.
Type "docker compose logs -t --since 20s scheduler | grep -E 'stopped|elected leader'"
Enter
Sleep 5s

Type "# Jobs never stopped firing through either one."
Enter
Type "docker compose logs --since 60s worker | grep -c 'executed'"
Enter
Sleep 4s

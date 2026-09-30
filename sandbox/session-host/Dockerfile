# The session host: mounts the session's JuiceFS volume, builds the agent's network,
# and runs the agent under gVisor. It carries no agent software; the agent's root
# filesystem is the agent image, mounted read-only at /agent-root. See sandbox/README.md.
FROM alpine:3.22@sha256:4bcff63911fcb4448bd4fdacec207030997caf25e9bea4045fa6c8c44de311d1
RUN apk add --no-cache bash coreutils iproute2 iptables ip6tables util-linux jq
# gVisor (runsc + its helpers) and JuiceFS, fetched and verified by build.sh.
COPY gvisor/ /opt/gvisor/
COPY juicefs /usr/local/bin/juicefs
COPY entrypoint.sh /usr/local/bin/session-host
COPY agent-exec.sh /usr/local/bin/agent-exec
COPY agent-connect.sh /usr/local/bin/agent-connect
RUN chmod +x /usr/local/bin/session-host /usr/local/bin/agent-exec /usr/local/bin/agent-connect /opt/gvisor/runsc
ENTRYPOINT ["/usr/local/bin/session-host"]

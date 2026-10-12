# A workload that counts in memory, as the fork's pod_restore_test.sh's does: 48 MiB of
# random data in /dev/shm, whose digest is its start token, and a count written to /count
# every 0.2 s; and 48 MiB of the image's own, /blob, for a lazily pulled layer to be read.
FROM mirror.gcr.io/library/busybox:1.37.0@sha256:bdf57e528e45e4433820e045b29b4597825a1c9e38353532d90a01445013f82e
RUN head -c 50331648 /dev/urandom >/blob
CMD ["sh", "-c", "head -c 50331648 /dev/urandom >/dev/shm/data; start=$(md5sum </dev/shm/data | cut -c 1-16); i=0; while :; do i=$((i+1)); echo \"$start $i\" >/count; sleep 0.2; done"]

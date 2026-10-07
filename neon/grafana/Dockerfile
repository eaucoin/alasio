# Grafana as alasio runs it (cli/src/manifests/grafana.ts): Grafana, the Infinity data
# source it reads the lake's query endpoint with, its settings, and what it is
# provisioned with (provisioning/, dashboards/), which start.sh completes as it starts.
# Nothing is fetched at run time. Published as ghcr.io/eaucoin/alasio-grafana by
# .github/workflows/release.yml.
FROM grafana/grafana:13.2.3-ubuntu@sha256:efa8e935921096c08e4636c320d2f955cb843fa0889f471022e0d568af780d3f AS infinity

# Infinity 4.1.1, signed by Grafana, from its plugin catalogue, as its checksum says it is.
ADD --checksum=sha256:e24f6030e998cf5dd76c70bb26a5bcb56a34bc530446358e21ae0164f21af3d5 \
    https://storage.googleapis.com/grafana-plugins-catalog/yesoreyeram-infinity-datasource/release/4.1.1/yesoreyeram-infinity-datasource-4.1.1.linux-amd64.zip \
    /tmp/infinity.zip
USER root
RUN grafana cli --pluginsDir /opt/grafana/plugins --pluginUrl /tmp/infinity.zip plugins install yesoreyeram-infinity-datasource

FROM grafana/grafana:13.2.3-ubuntu@sha256:efa8e935921096c08e4636c320d2f955cb843fa0889f471022e0d568af780d3f
LABEL org.opencontainers.image.source=https://github.com/eaucoin/alasio \
      org.opencontainers.image.description="alasio's Grafana: dashboards and alerts on its lake, alerting through its Telegram bot"

COPY --from=infinity /opt/grafana/plugins /opt/grafana/plugins
COPY grafana.ini /etc/grafana/grafana.ini
COPY provisioning /etc/grafana/alasio/provisioning
COPY dashboards /etc/grafana/alasio/dashboards
COPY start.sh /etc/grafana/alasio/start.sh
ENV GF_PATHS_PLUGINS=/opt/grafana/plugins GF_PATHS_PROVISIONING=/var/lib/grafana/provisioning
ENTRYPOINT ["/etc/grafana/alasio/start.sh"]

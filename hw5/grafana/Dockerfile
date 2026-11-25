FROM grafana/grafana:12.2.0

USER root
ENV GF_PATHS_PLUGINS=/opt/grafana-plugins
RUN --mount=type=secret,id=pip_ca \
    if [ -f /run/secrets/pip_ca ]; then export SSL_CERT_FILE=/run/secrets/pip_ca; fi \
    && grafana cli --pluginsDir "$GF_PATHS_PLUGINS" plugins install grafana-clickhouse-datasource 4.15.0

COPY provisioning/ /etc/grafana/provisioning/
COPY dashboards/ /opt/grafana-dashboards/
USER 472

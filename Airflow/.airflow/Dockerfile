FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
  && rm -rf /var/lib/apt/lists/* \
  && java -version

USER root
RUN printf '%s\n' \
    '#!/usr/bin/env bash' \
    'set -uo pipefail' \
    'airflow db migrate' \
    'for u in airflow admin; do' \
    '  airflow users create --username "$u" --password airflow --email "$u@lumi.local" \' \
    '    --firstname Lumi --lastname Dev --role Admin ||' \
    '    echo "[entrypoint-dev] $u already present - keeping existing credentials";' \
    'done' \
    'exec airflow "$@"' \
    > /usr/local/bin/airflow-dev-entrypoint.sh \
 && chmod 755 /usr/local/bin/airflow-dev-entrypoint.sh \
 && bash -n /usr/local/bin/airflow-dev-entrypoint.sh

USER airflow
ENTRYPOINT ["/usr/local/bin/airflow-dev-entrypoint.sh"]
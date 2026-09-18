set -uo pipefail

airflow db migrate

for u in airflow admin; do
  airflow users create \
    --username "$u" --password airflow \
    --email "$u@lumi.local" --firstname Lumi --lastname Dev \
    --role Admin \
    || echo "[entrypoint-dev] user '$u' already present - keeping existing credentials"
done

exec airflow "$@"

# RabbitMQ Grafana Dashboards

## Creating or updating dashboards

### Making changes

First, from the `rabbitmq_prometheus` directory, run `make overview metrics` to spin up a local environment using
`docker-compose`. This will provide you with a full Prometheus + Grafana stack, and sample workloads to make the
dashboards 'come alive' with non-zero metrics. To tear down this infrastructure, run `make down`.

Login to `localhost:3000` with the credentials `admin`:`admin` and begin creating / updating your dashboard in the
Grafana UI.

Once you have finished editing your dashboard, navigate to the 'Share' menu next to the name of the dashboard, go to
'Export', and be sure to tick 'Export for sharing externally'. Either save the dashboard JSON to a file or click 'View
JSON' and copy-paste over into this repo.

At this point, you can test your changes work on different versions of Grafana, by editing `services.grafana.image` in
[docker-compose-metrics.yml](../docker-compose-metrics.yml) and running `make metrics` to rebuild the Grafana server.
Be sure to test on the latest version publicly available.

### If creating a new dashboard

You will need some additional files for a new dashboard:

- A description of the dashboard; see [the Erlang-Distribution one](publish/erlang-distribution-11352.md) for example
- Screenshots of the dashboard in action, saved in `./publish/`

### Create a PR

Create a pull request in `rabbitmq-server` with the above changes.

## Making a dashboard available on grafana.com

If you are on the RabbitMQ team, navigate to our GrafanaLabs dashboard at https://grafana.com/orgs/rabbitmq/dashboards,
and log in with the team credentials.

Once a PR is merged, either create a new revision of a dashboard from the JSON checked in under `./dashboards/`, or
create a new dashboard from the JSON.
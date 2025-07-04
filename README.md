# nococlient
Client written in Python to interact with the nocodb api

## Testing

A `docker-compose.yml` file is provided for spinning up a local NocoDB and PostgreSQL instance. Copy `.env.example` to `.env` and adjust any values if needed.

Common development tasks are managed using [doit](https://pydoit.org/). Run unit tests with:

```bash
doit             # or `doit test`
```

The integration tests require a running NocoDB service. They can be executed with:

```bash
doit integration
```

Under the hood this task will start the Docker services (`docker-compose up -d`), run `NOCO_TEST_ONLINE=1 pytest`, and then shut them down.

Running `pytest` without the `NOCO_TEST_ONLINE` variable will skip the integration tests.

## License
This project is licensed under the GNU Affero General Public License v3.0 - see the [LICENSE](LICENSE) file for details.

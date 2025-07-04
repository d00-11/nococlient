# nococlient
Client written in Python to interact with the nocodb api

## Testing

A `docker-compose.yml` file is provided for spinning up a local NocoDB and PostgreSQL instance. Copy `.env.example` to `.env` and adjust any values if needed.

The tests use the [`pytest-docker`](https://pypi.org/project/pytest-docker/) plugin to start and stop these services automatically. Install the plugin and run the tests with:

```bash
pip install pytest-docker
pytest
```

Running `pytest` without the `NOCO_TEST_ONLINE` variable will skip the integration tests. Set `NOCO_TEST_ONLINE=1` to run them if Docker is available.

## License
This project is licensed under the GNU Affero General Public License v3.0 - see the [LICENSE](LICENSE) file for details.

# nococlient
Client written in Python to interact with the nocodb api

## Testing

A `docker-compose.yml` file is provided for spinning up a local NocoDB and PostgreSQL instance. Copy `.env.example` to `.env` and adjust any values if needed. Start the services and run the integration tests using the following commands:

```bash
docker-compose up -d
NOCO_TEST_ONLINE=1 pytest
```

To stop the services run:

```bash
docker-compose down
```

Running `pytest` without the `NOCO_TEST_ONLINE` variable will skip the integration tests.

## License
This project is licensed under the GNU Affero General Public License v3.0 - see the [LICENSE](LICENSE) file for details.

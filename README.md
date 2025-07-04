# nococlient
Client written in Python to interact with the nocodb api

## Testing

A `docker-compose.yml` file is provided for spinning up a local NocoDB and PostgreSQL instance. Copy `.env.example` to `.env` and adjust any values if needed. Tests use `pytest-docker` to automatically start and stop the services.

Run the tests with:

```bash
pytest -q
```

## License
This project is licensed under the GNU Affero General Public License v3.0 - see the [LICENSE](LICENSE) file for details.

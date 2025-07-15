# nococlient
Client written in Python to interact with the NocoDB API

## Installation

Install the package and its runtime dependencies with `pip`:

```bash
pip install .
```

The project uses a `src/` layout and the Python package lives under
`src/nococlient` in the repository.

The client requires two environment variables when used:
`NOCODB_BASE_URL` and `NOCODB_API_KEY`.
Copy `.env.example` to `.env` and adjust the values.

## Development

Development requirements are listed in `requirements-dev.in` and the
pinned `requirements-dev.txt` is generated with
`pipx run pip-tools pip-compile requirements-dev.in -o requirements-dev.txt --generate-hashes`.
Install them with:

```bash
pip install -r requirements-dev.txt
```

Testing requires Docker and Docker Compose. Start the Docker daemon first
(e.g. by running `sudo systemctl start docker` or launching Docker Desktop)
and then run the tests:

```bash
pytest -q
```
## Disclaimer
This code was produced with extensive help from LLMs, but is not entirely vibe-coded.

## License
This project is licensed under the GNU Affero General Public License v3.0 - see
the [LICENSE](LICENSE) file for details.

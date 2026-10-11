# Environment Verification (Goal 1, Task A)

## Python version
The author's machine used Python 3.11.9 (switched from the system default 3.14, which had no pre-built pandas 2.2.3 installer and would have required compiling from source). The saved logs and benchmark in this folder were produced with Python 3.11 as well; the exact interpreter and library versions are recorded in `benchmark/benchmark_context.json` and in the "Measured on" line of `report.md`.

## Key package versions (from `requirements.txt`, pinned exactly)
- pandas 2.2.3
- pyarrow 17.0.0
- psycopg[binary] 3.2.3
- python-dotenv 1.0.1
- PyYAML 6.0.2

`requirements-dev.txt` adds pytest for the test suite.

## `validate-env` output
`validate-env` performs real checks (settings, environment variables, source files, writable folders, PostgreSQL connection and tables) and exits non-zero on any failure. Saved output of the final code (`logs/01_validate_env.txt`):

<!-- BEGIN:validate_env_log -->
```text
$ python -m src.cli validate-env
PROJECT_ROOT= /home/claude/lab3-student
DB host/database= localhost dss150p
Configured source= data/source
[ OK ] .env file: found
[ OK ] settings.yml [pipeline]: all required keys present
[ OK ] settings.yml [quality]: all required keys present
[ OK ] settings.yml [storage_benchmark]: all required keys present
[ OK ] quality thresholds: quantity 1..20, 6 allowed statuses
[ OK ] benchmark settings: repeats=5, filter_status=DELIVERED
[ OK ] environment variables: all required variables are set
[WARN] POSTGRES_PASSWORD: still the placeholder 'change_me'; fine for a local lab only
[ OK ] source customers.csv: header has all 8 expected columns
[ OK ] source products.json: 601 records
[ OK ] source orders.csv: header has all 9 expected columns
[ OK ] writable raw_dir: /home/claude/lab3-student/data/raw
[ OK ] writable staging_dir: /home/claude/lab3-student/data/staging
[ OK ] writable curated_dir: /home/claude/lab3-student/data/curated
[ OK ] writable quarantine_dir: /home/claude/lab3-student/data/quarantine
[ OK ] writable benchmark_dir: /home/claude/lab3-student/data/benchmarks
[ OK ] writable partition_dir: /home/claude/lab3-student/data/partitioned
[ OK ] writable state: /home/claude/lab3-student/state
[ OK ] database connection: localhost:5432/dss150p
[ OK ] database tables: curated.sales_order_lines, audit.partition_loads, audit.pipeline_runs
validate-env: 19 ok, 1 warning(s), 0 failure(s)
[exit code: 0]
```
<!-- END:validate_env_log -->

The `[WARN]` line is expected while `.env` still holds the placeholder password from `.env.example`. Failure cases are shown in `logs/14_failure_handling.txt`.

## Why the virtual environment should not be committed to Git
A virtual environment is a local, machine-specific installation of Python packages — it contains compiled binaries and absolute file paths tied to one computer and operating system. Committing it would make the repository enormous and would not help anyone else set up the project, since a `.venv` built on Windows will not work on macOS or Linux. `requirements.txt` records *what* to install in a portable, platform-independent way — anyone can run `pip install -r requirements.txt` to regenerate an equivalent environment. The environment is disposable and reproducible; the requirements file is the source of truth.

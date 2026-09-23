# Environment Verification (Goal 1, Task A)

## Python version
3.11.9 (switched from the system default 3.14, which had no pre-built pandas 2.2.3 installer available and would have required compiling from source)

## Key package versions (from requirements.txt, pinned exactly)
- pandas 2.2.3
- pyarrow 17.0.0
- psycopg[binary] 3.2.3
- python-dotenv 1.0.1
- PyYAML 6.0.2

## validate-env output
    PROJECT_ROOT= C:\Users\zoehc\Documents\dss150p-lab03-starter
    DB host/database= localhost dss150p
    Configured source= data/source

## Why the virtual environment should not be committed to Git
A virtual environment is a local, machine-specific installation of Python packages — it contains compiled binaries and absolute file paths tied to this specific computer and operating system (Windows, in this case). Committing it would make the repository enormous and would not actually help anyone else set up the project, since a `.venv` built on Windows won't work on macOS or Linux. Instead, `requirements.txt` records *what* to install in a portable, platform-independent way — anyone can run `pip install -r requirements.txt` to regenerate an equivalent environment on their own machine. The environment is disposable and reproducible; the requirements file is the actual source of truth.
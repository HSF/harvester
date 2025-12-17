# Harvester

Harvester is a resource-facing service between WFMS and the collection of pilots.
It is a lightweight stateless service running on a VObox or an edge node of HPC centers
to provide a uniform view for various resources.

For a detailed description and installation instructions, please check out this project's wiki tab:
https://github.com/HSF/harvester/wiki

----------

Packaging note
--------------

This project uses `pyproject.toml` (Hatch/hatchling) as the canonical source of packaging
metadata (license, build-backend, dependencies). Keep `pyproject.toml` in sync when making
packaging changes; `setup.py` is maintained for legacy compatibility and should mirror
the same license and supported Python versions.

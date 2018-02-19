# Building Documentation

An environment with several prerequisites is needed to build the
documentation.

A conda environment with pip packages included is in `environment.yml` of the current directory, and may be created with:

```bash
conda env create
conda activate intake
```

## Build docs

To make HTML documentation:

```bash
cd docs/
make html
```

Outputs to `build/html/index.html`

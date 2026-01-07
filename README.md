# ML-Lineage-Trackern - ML Experiment and Model Lineage Tracking

## Overview

ML-Lineage-Tracker is a lightweight CLI tool for tracking **datasets, experiments (runs), and models** to provide **end-to-end lineage** across the ML lifecycle. It records how models are produced so workflows are reproducible, traceable, and auditable.

This project focuses on **metadata and relationships**, not model training or artifact storage.

---

## Core Concepts

### Dataset

A versioned collection of data used for training.

Tracked information:

* Name and version
* Source or path
* Creator (actor)
* Timestamp

---

### Run (Experiment)

A single execution of a training attempt.

Tracked information:

* Dataset version(s) used
* Parameters / hyperparameters
* Code reference (optional)
* Metrics
* Start and end time
* Actor (who ran it)

---

### Model

A trained artifact produced by a run.

Tracked information:

* Artifact reference (e.g. S3 URI or file path)
* Associated run
* Lifecycle stage (registered, staging, production, archived)
* Creation time

The system stores references to artifacts, not the binaries themselves.

---

## Lineage Model

Objects are connected through immutable, append-only relationships:

```
Dataset → Run → Model
```

This enables queries such as which models were trained on a dataset, which run produced a model, and who created or promoted it.

---

## Architecture (MVP)

* CLI-first interface
* Supabase (Postgres) metadata store
* Immutable, append-only records 
* Artifact references stored as URIs
* Identity derived from local environment

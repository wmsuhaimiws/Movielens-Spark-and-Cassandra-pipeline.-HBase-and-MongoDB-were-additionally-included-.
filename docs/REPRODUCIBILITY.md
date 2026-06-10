# MovieLens 100k — Spark + Cassandra Big Data Pipeline

An end-to-end analytical pipeline is implemented on the
[MovieLens 100k](https://grouplens.org/datasets/movielens/) dataset:
**HDFS → Spark RDDs → DataFrames → Cassandra → read-back validation**, with an
optional **HBase + MongoDB** extension. The same pipeline is provided in two forms — a
full-cluster notebook and a self-contained Google Colab notebook — and detailed run
instructions for both are given in `HOW_TO_RUN.md`.

## Files

| File | Purpose |
|---|---|
| `movielens_pipeline.ipynb` | Main (cluster) notebook. Markdown / code / interpretation cells are alternated across all 10 technical requirements and 5 analytical tasks. The interpretation cells carry **verified** results from a real run. |
| `movielens_pipeline_colab.ipynb` | Colab notebook. HDFS, Cassandra and the helper modules are all installed/generated in-session, so nothing has to be uploaded. |
| `schemas.py` | Explicit Spark `StructType` schemas for `u.user`, `u.data`, `u.item`. |
| `genres.py` | The canonical ordered list of the 19 MovieLens genre flag columns. |
| `HOW_TO_RUN.md` | Step-by-step run guide for both options (local/cluster and Colab). |
| `SETUP_WINDOWS_WSL.md` | Full setup guide for running the cluster option on Windows 11 via WSL2. |
| `start-movielens.sh` | One-command per-session startup for the WSL2 option (SSH, HDFS, Cassandra, optional HBase/MongoDB, then Jupyter). |
| `README.md` | This file. |

For the cluster notebook, `movielens_pipeline.ipynb`, `schemas.py` and `genres.py` must
be kept together in one directory, because `schemas` and `genres` are imported by the
notebook. For the Colab notebook nothing has to be co-located, since the helper modules
are written into the VM at runtime.

## Assumed versions

| Component | Version |
|---|---|
| Java (OpenJDK) | 11 |
| Python | 3.10 |
| Apache Hadoop / HDFS | 3.3.6 |
| Apache Spark / PySpark | 3.5.1 (Scala 2.12) |
| Apache Cassandra | 4.1.4 |
| spark-cassandra-connector | 3.5.0 (`com.datastax.spark:spark-cassandra-connector_2.12:3.5.0`) |
| `cassandra-driver` (Python) | latest (used for DDL / readiness checks) |
| Apache HBase *(extension)* | 2.5.8 + `happybase` 1.2.0 |
| MongoDB *(extension)* | 7.0 + mongo-spark-connector 10.3.0 |
| matplotlib | 3.8.x |

A **single-node pseudo-distributed** setup is assumed, in which HDFS, Spark, Cassandra
(and optionally HBase/MongoDB) are all hosted on `localhost`.

## Cluster / local option — reproducibility

> The fastest path on Windows 11 is described in `SETUP_WINDOWS_WSL.md`. The Colab path
> (no local install required) is described in `HOW_TO_RUN.md`.

### 1. Environment setup

The driver-side dependencies are installed with:

```bash
pip install pyspark==3.5.1 matplotlib happybase pymongo cassandra-driver
```

Java 11 must be the active JDK:

```bash
java -version          # -> openjdk version "11..."
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

The Spark↔Cassandra and Spark↔Mongo connectors are resolved automatically from Maven
Central at `SparkSession` startup via `spark.jars.packages`, so no jars have to be managed
manually. (Internet access is required on first launch so that the connectors can be
resolved into `~/.ivy2`.)

### 2. Data placement in HDFS

```bash
# MovieLens 100k is downloaded and unzipped
wget https://files.grouplens.org/datasets/movielens/ml-100k.zip
unzip ml-100k.zip -d ~/data        # -> ~/data/ml-100k/{u.user,u.data,u.item,...}

# HDFS is started (if it is not already running)
start-dfs.sh

# The three raw files are landed in HDFS
hdfs dfs -mkdir -p /movielens
hdfs dfs -put -f ~/data/ml-100k/u.user ~/data/ml-100k/u.data ~/data/ml-100k/u.item /movielens/
hdfs dfs -ls /movielens            # the three files are verified
```

The HDFS paths `hdfs://localhost:9000/movielens/...` are read by the notebook (the full
host:port authority is required — a bare `hdfs:///` has no host and fails). These `put`
commands are also issued by the notebook's load cell (which auto-locates the local folder),
so this step may be skipped.

### 3. Cassandra startup

```bash
# A service install:
sudo systemctl start cassandra
# ...or a tarball install:
$CASSANDRA_HOME/bin/cassandra -f &

# Readiness is confirmed once it answers:
cqlsh 127.0.0.1 9042 -e "SELECT release_version FROM system.local;"
```

The keyspace and tables are created **from inside the notebook** (Section 7), so no manual
schema step is required. A lab default is assumed: no authentication, native transport on
`9042`.

### 4. (Optional) HBase and MongoDB for the extension

```bash
# HBase + Thrift gateway (happybase communicates over Thrift)
start-hbase.sh
hbase thrift start &        # port 9090

# MongoDB
sudo systemctl start mongod # port 27017
```

If the extension is not being run, the "Added-Value Extension" cells at the foot of the
notebook are simply left un-executed; the core pipeline is fully independent of them.

### 5. Running the notebook

```bash
cd <this-directory>
jupyter notebook movielens_pipeline.ipynb
```

The cells are run top to bottom. On the first run the connectors are downloaded from
Maven (internet required, ~1 min).

### Re-running across sessions (WSL2)

The WSL services (HDFS, Cassandra) stop when Ubuntu is closed, so they must be restarted each
session. The bundled `start-movielens.sh` does this in one command (SSH → HDFS → Cassandra →
optional HBase/MongoDB → Jupyter), with readiness checks. For HDFS data to survive a restart,
`hadoop.tmp.dir` must point at a persistent folder (e.g. `~/hdfs-tmp`) rather than `/tmp`; this
one-time configuration is covered in `SETUP_WINDOWS_WSL.md` (Step 3).

## Analytical tasks answered

- **(i)** The average rating per movie is calculated.
- **(ii)** The top 10 movies by average rating are identified (a ≥50-rating support threshold is applied).
- **(iii)** Users with ≥50 ratings are identified and each one's favourite (most-rated) genre is determined.
- **(iv)** Users younger than 20 are found.
- **(v)** Users whose occupation is `scientist` **and** whose age is 30–40 inclusive are found.

### Verified results (from a real MovieLens 100k run)

- Cleaning: users 943→943, ratings 100000→100000, items 1682→1682 (the dataset is already clean).
- Top movie: *Close Shave, A (1995)* — avg 4.491 over 112 ratings.
- Power users (≥50 ratings): **568**; the favourite-genre split is Drama 368 / Comedy 100 / Action 80 / Thriller 15 / Horror 4 / Children 2.
- Users under 20: **77**.
- Scientists aged 30–40: **16**.

## Troubleshooting

| Symptom | Resolution |
|---|---|
| Connector not found / Ivy error | Internet is required on the first run so that `spark.jars.packages` can be resolved. Behind a proxy, `~/.ivy2` should be pre-populated or local jars passed with `--jars`. |
| `NameNode` missing from `jps` after a WSL restart (`Connection refused :9000`) | HDFS storage was on `/tmp` and got wiped. `hadoop.tmp.dir` must be set to a persistent folder, then HDFS reformatted once (see `SETUP_WINDOWS_WSL.md`, Step 3 / Troubleshooting). |
| `Incomplete HDFS URI, no host: hdfs:///...` | The full authority `hdfs://localhost:9000/movielens` must be used; the notebook's `HDFS_BASE` already does. |
| `NoHostAvailable` (Cassandra) | It should be confirmed that `cqlsh 127.0.0.1 9042` (or `nodetool status`) responds and that `spark.cassandra.connection.host` matches. |
| Garbled film titles | `u.item` must be read as **latin-1** (this is handled in the RDD parsing cell). |
| Java version errors | Java 11 is required by Spark 3.5 and Cassandra 4.1; Java 17+ may require extra `--add-opens` flags. |
| `cqlsh` crashes with `No module named 'six.moves'` | The bundled `cqlsh` is incompatible with newer Python; the native `cassandra-driver` should be used instead (this is done automatically in the Colab notebook). |

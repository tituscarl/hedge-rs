[![main](https://github.com/flowerinthenight/hedge-rs/actions/workflows/main.yml/badge.svg)](https://github.com/flowerinthenight/hedge-rs/actions/workflows/main.yml)
[![crates.io](https://img.shields.io/crates/v/hedge_rs)](https://crates.io/crates/hedge_rs)
[![docs-rs](https://img.shields.io/docsrs/hedge_rs)](https://docs.rs/hedge_rs/latest/hedge_rs/)

## hedge-rs

A cluster membership Rust library. It is built on [spindle-rs](https://github.com/flowerinthenight/spindle-rs), a distributed locking library built on [Cloud Spanner](https://cloud.google.com/spanner/) and [TrueTime](https://cloud.google.com/spanner/docs/true-time-external-consistency). It is a port (subset only) of the original [hedge](https://github.com/flowerinthenight/hedge), which is written in Go. Ported features include:

* Tracking of member nodes - good for clusters with sizes that can change dynamically overtime, such as [GCP MIGs](https://cloud.google.com/compute/docs/instance-groups#managed_instance_groups), and [Kubernetes Deployments](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/);
* Leader election - the cluster elects and maintains a single leader node at all times;
* List of members - get a list of all member nodes at any time;
* Send - any member node can send messages to the leader at any time; and
* Broadcast - any member node can broadcast messages to all nodes at any time.

## Running the sample

First, create the Spanner table for `spindle-rs`:

```sql
CREATE TABLE locktable (
    name STRING(MAX) NOT NULL,
    heartbeat TIMESTAMP OPTIONS (allow_commit_timestamp=true),
    token TIMESTAMP OPTIONS (allow_commit_timestamp=true),
    writer STRING(MAX),
) PRIMARY KEY (name)
```


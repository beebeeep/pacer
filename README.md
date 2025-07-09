pacer
=====

A simple distributed rate-limiter with HTTP API

Usage
-----
Send empty POST request to `/limit/{max_rps}/{bucket}`, response code will be either `200 OK` or `429 Too Many Requests`.
Buckets for each RPS limit are created on-demand.

High-availablity setup
----------------------
You can configure arbitrary number of replicas, they will be synchronizing their buckets
so that RPS limit for each bucket is calculated across the whole cluster.
Then you can simply have any HTTP load balancer doing round-robin routing across all replicas.




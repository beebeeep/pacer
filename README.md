pacer
=====

A simple rate-limiter with HTTP interface.

Usage
-----
Send empty POST request to `/{class}/{bucket}`, response code will be either `200 OK` or `429 Too Many Requests`.
Buckets are created on-demand, each bucket will have RPS limit according to its class. Classes are preconfigured, default class `default` has limit of 100 RPS. 


## Query Engine

The Query Engine introduces querying capabilities within Styx (H-Styx or Hybrid Styx), enabling low-latency transactions as well as querying and aggregation using DuckDB. This results in a serverless, self-contained SFaaS system that can support both OLTP and OLAP (HTAP) capabilities. 

This is the code for Query Engine container, that can be added into Styx deployment using a configuration flag. Steps to run this along with configurations are specified in the main [`README.md`](https://github.com/smrutik25/styx/tree/main) 

To support the Query Engine, certain changes were also made within the [`coordinator`](https://github.com/smrutik25/styx/tree/main/coordinator) , [`worker`](https://github.com/smrutik25/styx/tree/main/styx-package) and  [`styx-package`](https://github.com/smrutik25/styx/tree/main/styx-package).


### HATtrick Benchmark
The HATtrick [`demo/demo-hattrick`](https://github.com/smrutik25/styx/tree/main/demo/demo-hattrick) benchmark was used to test and calculate metrics for hybrid load. This involved cloning and modifying the original HTAP benchmark [`HATtrick`](https://github.com/UWHustle/HATtrick) to test against a baseline of Postgres with Streaming Replication, while also recreating the same benchmark within parameters of Styx. 
The stateful functions for HATtrick are present in [`demo/demo-hattrick/functions`](https://github.com/smrutik25/styx/tree/main/demo/demo-hattrick/functions)


### Thesis Project

This code is the implementation of the Master's thesis titled: [Global State Querying in Stream Processing using Snapshots](https://repository.tudelft.nl/record/uuid:60d4f13b-19d7-4f3e-9477-75566477d7df)

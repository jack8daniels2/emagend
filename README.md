emagend
--------

The problem is (intentionally) very vague. Every aspect of the solution is dependent on the use-case(s).

In general, such use-cases don't require a high query performance and thus don't necessitate a large amount of resources. A simple solution in such scenarios would be two map-reduce jobs - first one to preprocess and aggregate the raw logs, and second one to perform the query on top of aggregated logs. HDFS like file systems shard the data automatically and it would scale pretty well. It also allows you to store as much infrmation as you want about the access and retrieve later. But, this solution isn't a very interesting interview problem.

So, we focus on a solution that gives us good query performance and we (responsibly) throw resourses at indexing the data.

**Assumption 1: Query performance is the most critcial part of this solution**

With such access log data, there is usually a retention period. Also, there is some pattern that we can expect from the queries. This drives the decisions around optimizations. At this point of the project, this might be too early to make optimizations, but I definitely see a value of a bloom filter (or similar) and a request/response cache.

**Assumption: We are going to assume that there is no pattern in the IPs that are queried for, that is, it is completely random. But, we are going to see more queries for more recent access. It is not a strict requirement, but we need to make some assumption to show value of a proposed bloom filter/cache.**

The given log generator code doesn't gaurantee unique IPs and doesn't give any information on the day that access was made. We are going to assume that a log file contains unique IPs, all belonging to the same date. Most likely, the ingest part of the solution is going to be idempotent and won't be affected if the IPs are not unique. Also, extacting unique IPs from a file is not a very hard problem.
A webserver of this scale is going to be multi-tenant, with many such logs files per day. We are going to assume that we have data in the format `logs/<date>/access_*.log` that we ingest at a regular interval. It would have been better reading it off a message queue, and we don't need to maintain a 'cursor'. For now, it is a completely bulk ingest, a log dir at a time.

**Assumption: Each log file contains unique set of IPs all belonging to the access made on the same date. We'll have multiple such files `access_*.log` for each day, in a directory named `<date>`**

### Solution
We are going to use a database backend and an in-memory cache. Since we are going to potentially store billions of rows (IPv4 - max 4 billion to be exact), we'll use Riak (or similar key value store) that uses consistent hashing to shard and is CP (out of CAP). Riak is not a great choice because it prefers immutable values or CRDTs, but for the actual purpose of this exercise, it doesn't matter.

The key will be IP address and value will be a encoded list of dates when it accessed the website.
There are many options on how we want to store this list.

1. List of dates

    It would allow 2 binary searches to check a particular range. O(log(n)) but will take a lot of space - 4 bytes per date, I believe.
2. **A bitarray where each bit indicates the day since a predefined epoch when the access was made**

   This schema will save a lot of space, can be easily rolled up to move epochs forward (to age old data), but would take O(m) for query where m is the number of dates in the range.
3. A cumulative list where each cell indicates the number of times an access was made since epoch

    This schema will take more space but improves search performance to O(log(n)) without wasting too much space. There are further optimizations to this schema, but again depend upon retention period and query pattern.

For in-memory cache, we'll tentatively use a bloom filter[IP] for a particular period that we expect to be queried for more often. This needs to be tuned based on performance and usage, but just for a proof of concept, we'll maintain it for current month.
A counting bloom filter would have been a better approach as we could have easily maintained a sliding window cache of last 30 days.

### Dependencies
* bitarray
* riak
* dateutil

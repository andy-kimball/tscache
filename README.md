# tscache

A timestamp cache is meant to be used with a data store that is able to
maintain multiple versions of each value. The cache efficiently tracks the
latest logical time at which any key or range of keys has been accessed. Keys
are binary values of any length, and times are represented as hybrid logical
timestamps (see hlc package). The cache guarantees that the read timestamp of
any given key or range will never decrease. In other words, if a lookup returns
timestamp A and repeating the same lookup returns timestamp B, then B >= A.

Add and lookup operations do not block or interfere with one another, which
enables predictable operation latencies. Also, the impact of the cache on the
GC is virtually nothing, even when the cache is very large. These properties
are enabled by employing a lock-free skiplist implementation that uses an
arena allocator. Skiplist nodes refer to one another by offset into the arena
rather than by pointer, so the GC has very few objects to track.

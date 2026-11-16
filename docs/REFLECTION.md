# Reflection

The surprising result was a green 10,000-job chaos run that had never exercised
lease recovery: the cancellation stream selected old running jobs, including jobs
left behind by killed workers, before their leases could be reclaimed. Counting
kills and checking final states was therefore insufficient. The harness was
changed to protect abandoned jobs from cancellation and require actual generation
increments; the original output was retained. The main remaining concern is the
boundary of the guarantee: SQLite can atomically commit a result row and job state,
but it cannot make a GPU run, an external upload or an HTTP call exactly once.
Production readiness also depends on storage recovery, capacity headroom and
operational controls that short local tests cannot establish.

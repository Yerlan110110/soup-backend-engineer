# AI assistance

I used OpenAI Codex to prepare this submission. Codex drafted the design,
implemented the queue and test harnesses, ran the recorded checks, investigated
the failures described in the report, and helped write the documentation.

The submitted design incorporates Codex's suggestions to use SQLite with short
write transactions, fence each worker attempt with a generation token, and commit
the effect and terminal state together. The harness uses real worker processes
and checks the final database independently of the queue code.

Two issues were found during the AI-assisted runs. First, the workload client
omitted the JSON content type and got HTTP 422. A regression check reproduced
the rejection, then passed after the header was added. Second, the first full
chaos run passed its original checks but never reclaimed an expired lease. The
cancellation pattern masked that path. The harness was updated to protect jobs
abandoned by killed workers and require observed reclaims. The report retains
the original output and explains both changes.

Codex executed the checks shipped in this archive. The results establish what
ran in the local macOS environment. The Compose file was checked against the
official schema. Building and running the containers was not verified in that
environment.

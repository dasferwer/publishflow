# PublishFlow architecture decisions

## Immutable versions

`articles` contains the current workflow state and searchable metadata.
`article_versions` stores complete immutable snapshots. Editing a draft creates
the next numbered version in the same transaction, so audit and rollback tools
can be added without reconstructing content from diffs.

## Explicit workflow

State transitions live in the service layer instead of arbitrary status
updates. Authors can edit their own drafts and submit them. Editors approve,
request changes, schedule, publish and archive. Every transition appends an
`article_history` row with actor, previous state, new state and details.

## Transactional outbox

A request never attempts a database commit and RabbitMQ publish as two unrelated
writes. The domain record and `outbox_events` row commit together. A separate
publisher uses `FOR UPDATE SKIP LOCKED`, publishes persistent messages and only
then records `published_at`.

## Batch import

Import requests persist the job and every input item before publishing
`content.import.requested`. The consumer locks the job, recognizes terminal
states on redelivery, and handles each row in a nested transaction. Duplicate
slugs are marked failed without rolling back valid rows.

## Scheduler

The scheduler selects due rows with `FOR UPDATE SKIP LOCKED`. Several scheduler
replicas can therefore run without publishing one article twice. The automatic
transition, audit record and outbox event commit atomically.

## Failure scenarios

| Failure | Behaviour |
|---|---|
| PostgreSQL unavailable | API healthcheck fails; no partial write is accepted |
| RabbitMQ unavailable | Committed outbox rows remain pending for retry |
| Publisher restarts after publish | Consumer-side job state makes redelivery safe |
| Import contains a duplicate slug | That item fails; valid items still import |
| Import worker crashes mid-job | Pending items are completed on redelivery |
| Two schedulers select due content | Row locks let only one process each article |
| Author reads another author's draft | API returns `403 Forbidden` |
| Invalid workflow transition | API returns `409 Conflict` |

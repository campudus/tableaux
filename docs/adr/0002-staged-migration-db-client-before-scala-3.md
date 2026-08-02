# Staged migration: DB-client rewrite on Vert.x 3.9.x/Scala 2.12, then Scala 3 + Vert.x 4.5.11

The migration bundles two independently risky changes: replacing the legacy `vertx-mysql-postgresql-client` (mauricio-based async SQL client, gone in Vert.x 4) with the reactive `vertx-pg-client`, and migrating Scala 2.12 → 3. `vertx-pg-client` is already published for Vert.x 3.9.x, so the DB-client rewrite doesn't actually require the Scala 3 jump. We do them as two sequential, independently green-tested stages instead of one combined change:

1. **Stage 1**: bump to Vert.x 3.9.x, rewrite `SQLConnection.scala`/`database.scala` against `vertx-pg-client`, still on Scala 2.12. Additionally compile with `-Xsource:3` to surface Scala-3 incompatibilities early while the rest of the stack is still known-good.
2. **Stage 2**: Vert.x 4.5.11 + Scala 3.3 LTS + Gradle 9.x.

Each stage is its own PR against master, gated on the full test suite passing. This isolates which change broke something if something breaks, at the cost of an extra intermediate PR.

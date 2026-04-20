# Changelog

## v0.3.1

Released 2026-04-20.

- Added support for DuckDB v1.5.2

## v0.3.0

Released 2026-04-05.

- Added pushdown for `OR` conjunctions. This means `WHERE` clauses like
  `tags['foo'] == 'bar' OR tags['baz'] IS NOT NULL` and similar can be
  executed efficiently.

## v0.2.0

Released 2026-04-02.

- Added pushdown for `tags['key'] IN ('val1', 'val2')` style predicates,
  making them more performant

## v0.1.1

Released 2026-04-01.

- Fixed builds on Windows

## v0.1.0

Released 2026-03-28. Initial public release.

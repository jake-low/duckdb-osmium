# Changelog

## v0.4.2

Released 2026-07-09.

- Added support for DuckDB v1.5.4
- Fixed line and area handling to skip elements with incomplete geometries
  (rather than aborting the query when they are encountered)

## v0.4.1

Released 2026-06-06.

- Added support for DuckDB v1.5.3

## v0.4.0

Released 2026-05-01.

- Ensure that output polygons always follow OGC winding order (CCW exterior rings).
  Previously, polygons derived from closed ways would follow whatever direction
  they were drawn in the raw OSM data.

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

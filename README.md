# duckdb-osmium

An extension for [duckdb](https://duckdb.org) that allows reading OpenStreetMap data in OSM XML and OSM PBF formats using [libosmium](https://osmcode.org/libosmium).

DuckDB's first-party [`spatial` extension](https://duckdb.org/docs/stable/core_extensions/spatial/overview) can also read OSM PBF files using the [`ST_ReadOSM()`](https://duckdb.org/docs/stable/core_extensions/spatial/functions#st_readosm) function. However, this method returns rows in OSM's native data model (Nodes, Ways, and Relations). `duckdb-osmium`, in contrast, uses libosmium to reconstruct geometries for OSM elements and returns data including a geometry column containing Points, LineStrings, or Polygons/MultiPolygons.

This extension supports predicate pushdown (so elements that don't match the WHERE clause won't be processed) and projection pushdown (so that if you don't SELECT the geometry column in your query, geometry construction is skipped internally).

## Installation

Run this in a DuckDB shell to install the extension:

```
INSTALL osmium FROM community;
```

This downloads the compiled extension from the [DuckDB Community Extensions](https://duckdb.org/community_extensions/) repository.

## Examples

Get the OSM IDs, names, and Point geometries of all `place=city` nodes:

```sql
LOAD osmium;

SELECT id, tags['name'] as name, geometry
FROM 'example.osm.pbf'
WHERE kind = 'node'
  AND tags['place'] = 'city';
```

Copy the OSM type, OSM ID, tags, and Polygon geometries of all `building=*` areas to a GeoJSON file:

```sql
LOAD osmium;
LOAD spatial;

COPY (
  SELECT type, id, tags, geometry
  FROM 'example.osm.pbf'
  WHERE kind = 'area'
    AND tags['building'] IS NOT NULL
) TO 'buildings.geojson' WITH (FORMAT GDAL, DRIVER GeoJSON);
```

Calculate the total length of roads, grouped by `surface` tag:

```sql
LOAD osmium;
LOAD spatial;

-- tells DuckDB not to assume lat,lon order for ST_Length_Spheroid()
SET geometry_always_xy = true;

SELECT
    tags['surface'] AS surface,
    SUM(ST_Length_Spheroid(geometry)) AS total_length_m
FROM 'example.osm.pbf'
WHERE kind = 'line'
  AND tags['highway'] IS NOT NULL
GROUP BY surface
ORDER BY total_length_m DESC;
```

## Schema

The following columns are available:

- `kind` - the constructed type; one of `node`, `line`, `area`, or `relation` (see next section)
- `type` - the original OSM type; one of `node`, `way`, or `relation`
- `id` - the OSM ID (unique among elements of the same type; for a globally unique key you want `(type, id)`)
- `tags` - the element's tags, as a `MAP(VARCHAR, VARCHAR)`
- `geometry` - the constructed geometry; a Point, LineString, Polygon, or MultiPolygon depending on `kind` (NULL for `relation` rows)
- `version` - the element's version number, which OSM increments each time it is modified (`UINTEGER`)
- `timestamp` - the time of the element's last edit, as a `TIMESTAMP WITH TIME ZONE` in UTC
- `changeset` - the ID of the changeset the element was last edited in (`UINTEGER`)
- `uid` - the numeric ID of the user who last edited the element (`UINTEGER`)
- `username` - the display name of the user who last edited the element (`VARCHAR`)
- `refs` - for `relation` rows, the member element IDs as a `BIGINT[]` (NULL for other kinds)
- `ref_roles` - for `relation` rows, the member roles as a `VARCHAR[]`, aligned with `refs` (NULL for other kinds)
- `ref_types` - for `relation` rows, the member types as a `VARCHAR[]` (each `node`, `way`, or `relation`), aligned with `refs` (NULL for other kinds)

The `version`, `timestamp`, `changeset`, `uid`, and `username` columns come from OSM element metadata, which is optional: files can be written without it, and individual fields can be absent (for example, edits with redacted authorship carry no `uid` or `username`). Any absent field is returned as NULL.

## Disambiguating lines and areas

Closed ways in OSM can represent either lines or polygons. Which interpretation is indended is implied by the element's tags. For example, a closed way with `highway=roundabout` represents a LineString; one with `building=house` represents a Polygon.

This extension does not have any opinion on which tags imply features are lines or areas; it's up to you to specify which you want in your query. This is what the `kind` column is for. It has four possible values:
- `node`: a tagged OSM node; its `geometry` will be a `Point`
- `line`: an open Way, or a closed way that is not tagged `area=yes`; its geometry will be a `LineString`
- `area`: a closed Way that is not tagged `area=no`, or a Relation with `type=multipolygon` or `type=boundary`. Its geometry will be a `Polygon` or `MultiPolygon`
- `relation`: other Relation types; they have NULL geometry

It's important to understand that closed ways which are not tagged `area=yes` or `area=no` are fundamentally ambiguous; the correct interpretation has to be guessed based on the way's other tags. **This extension will emit these ways twice**, once with `kind = 'line'` and a LineString geometry, and once as `kind = 'area'` and a Polygon geometry. It's up to you to filter the results with a WHERE clause to get the interpretation you want.

## License

This code is available under the MIT License; see the LICENSE file for details.

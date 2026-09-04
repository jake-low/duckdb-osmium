#define DUCKDB_EXTENSION_MAIN

#include "osmium_extension.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/geometry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/object_cache.hpp"

#include <osmium/area/assembler.hpp>
#include <osmium/area/multipolygon_manager.hpp>
#include <osmium/geom/wkb.hpp>
#include <osmium/handler.hpp>
#include <osmium/handler/node_locations_for_ways.hpp>
#include <osmium/index/map/all.hpp>
#include <osmium/index/node_locations_map.hpp>
#include <osmium/io/file_format.hpp>
#include <osmium/io/pbf_input.hpp>
#include <osmium/io/xml_input.hpp>
#include <osmium/io/reader.hpp>
#include <osmium/osm/area.hpp>
#include <osmium/osm/node.hpp>
#include <osmium/osm/relation.hpp>
#include <osmium/osm/way.hpp>
#include <osmium/relations/manager_util.hpp>
#include <osmium/visitor.hpp>

#include <algorithm>
#include <cstring>
#include <functional>
#include <limits>

enum OsmKind : uint8_t { KIND_NODE = 0, KIND_LINE = 1, KIND_AREA = 2, KIND_RELATION = 3 };
enum OsmType : uint8_t { TYPE_NODE = 0, TYPE_WAY = 1, TYPE_RELATION = 2 };

struct OsmRow {
	OsmKind kind;
	OsmType type;
	int64_t id;
	// OSM element metadata. Files can omit metadata, and individual fields can be
	// absent (e.g. anonymized PBF files carry no uid or username). Each field's
	// zero/empty value is surfaced as SQL NULL.
	uint32_t version = 0;
	int64_t timestamp_seconds = 0;
	uint32_t changeset = 0;
	uint32_t uid = 0;
	std::string username;
	std::vector<std::pair<std::string, std::string>> tags;
	std::string geometry; // WKB encoded
	std::vector<int64_t> refs;
	std::vector<std::string> ref_roles;
	std::vector<OsmType> ref_types;
};

struct KindFilter {
	bool nodes = true;
	bool lines = true;
	bool areas = true;
	bool relations = true;

	KindFilter &operator&=(const KindFilter &other) {
		nodes = nodes && other.nodes;
		lines = lines && other.lines;
		areas = areas && other.areas;
		relations = relations && other.relations;
		return *this;
	}

	bool operator==(const KindFilter &other) const {
		return nodes == other.nodes && lines == other.lines && areas == other.areas && relations == other.relations;
	}
};

struct TypeFilter {
	bool nodes = true;
	bool ways = true;
	bool relations = true;

	TypeFilter &operator&=(const TypeFilter &other) {
		nodes = nodes && other.nodes;
		ways = ways && other.ways;
		relations = relations && other.relations;
		return *this;
	}

	bool operator==(const TypeFilter &other) const {
		return nodes == other.nodes && ways == other.ways && relations == other.relations;
	}
};

// A filter that checks if the number of tags on an element falls within the given
// (inclusive) range. Used for pushdown of `cardinality(tags) > 0` and similar
// predicates. The default range accepts all elements.
struct TagCountFilter {
	uint64_t min = 0;
	uint64_t max = std::numeric_limits<uint64_t>::max();

	bool Unbounded() const {
		return min == 0 && max == std::numeric_limits<uint64_t>::max();
	}

	bool Matches(const osmium::TagList &tags) const {
		if (Unbounded()) {
			return true;
		}
		const uint64_t n = tags.size();
		return n >= min && n <= max;
	}

	// Returns true for an empty range, e.g. cardinality(tags) = 0 AND cardinality(tags) > 0
	bool None() const {
		return min > max;
	}

	TagCountFilter &operator&=(const TagCountFilter &other) {
		min = std::max(min, other.min);
		max = std::min(max, other.max);
		return *this;
	}

	bool operator==(const TagCountFilter &other) const {
		return min == other.min && max == other.max;
	}
};

// Only five (kind, type) pairs sensible: (node, node), (line, way), (area, way)
// (area, relation) and (relation, relation). The others represent conditions
// that no element can match, so we can skip reading the file at all.
static bool NoMatchingKindAndType(const KindFilter &kinds, const TypeFilter &types) {
	return !(kinds.nodes && types.nodes) && !((kinds.lines || kinds.areas) && types.ways) &&
	       !((kinds.areas || kinds.relations) && types.relations);
}

struct IdFilter {
	bool active = false;             // if inactive, no ID filtering is performed
	std::unordered_set<int64_t> ids; // if active, an element must have an ID in this set to be matched

	bool Matches(int64_t id) const {
		return !active || ids.count(id) > 0;
	}

	void Intersect(const std::unordered_set<int64_t> &other) {
		if (!active) {
			ids = other;
			active = true;
			return;
		}
		std::unordered_set<int64_t> both;
		for (auto id : other) {
			if (ids.count(id)) {
				both.insert(id);
			}
		}
		ids = std::move(both);
	}

	bool None() const {
		return active && ids.empty();
	}

	bool operator==(const IdFilter &other) const {
		return active == other.active && ids == other.ids;
	}
};

struct TagPredicate {
	std::string key;
	std::unordered_set<std::string> values; // empty = key-exists check; non-empty = value IN values

	bool operator==(const TagPredicate &other) const {
		return key == other.key && values == other.values;
	}

	void Serialize(duckdb::Serializer &serializer) const {
		// sort the values so that unordered sets which are equal (by ==) are serialized identically
		duckdb::vector<std::string> sorted_values(values.begin(), values.end());
		std::sort(sorted_values.begin(), sorted_values.end());
		serializer.WriteProperty(100, "key", key);
		serializer.WriteProperty(101, "values", sorted_values);
	}

	static TagPredicate Deserialize(duckdb::Deserializer &deserializer) {
		TagPredicate result;
		result.key = deserializer.ReadProperty<std::string>(100, "key");
		auto values = deserializer.ReadProperty<duckdb::vector<std::string>>(101, "values");
		result.values.insert(values.begin(), values.end());
		return result;
	}
};

static const char *KIND_NAMES[] = {"node", "line", "area", "relation"};
static const char *TYPE_NAMES[] = {"node", "way", "relation"};

using location_index_type = osmium::index::map::Map<osmium::unsigned_object_id_type, osmium::Location>;
using location_handler_type = osmium::handler::NodeLocationsForWays<location_index_type>;

static constexpr idx_t COL_KIND = 0;
static constexpr idx_t COL_TYPE = 1;
static constexpr idx_t COL_ID = 2;
static constexpr idx_t COL_TAGS = 3;
static constexpr idx_t COL_GEOMETRY = 4;
static constexpr idx_t COL_VERSION = 5;
static constexpr idx_t COL_TIMESTAMP = 6;
static constexpr idx_t COL_CHANGESET = 7;
static constexpr idx_t COL_UID = 8;
static constexpr idx_t COL_USERNAME = 9;
static constexpr idx_t COL_REFS = 10;
static constexpr idx_t COL_REF_ROLES = 11;
static constexpr idx_t COL_REF_TYPES = 12;
static constexpr idx_t NUM_COLUMNS = 13;

// Check whether an element's tags satisfy a single (non-conjunctive) predicate
static bool MatchesTagPredicate(const osmium::TagList &tags, const TagPredicate &pred) {
	const char *val = tags.get_value_by_key(pred.key.c_str());
	if (!val) {
		return false;
	}
	return pred.values.empty() || pred.values.count(val);
}

// Check whether an element's tags satisfy all pushed-down predicates.
//
// Predicates are stored in conjunctive normal form: the terms in the outer
// vector are ANDed and the terms in each inner vectors are ORed. For example:
// A AND (B OR C) -> [[A], [B, C]]
static bool MatchesTagPredicates(const osmium::TagList &tags, const std::vector<std::vector<TagPredicate>> &groups) {
	for (const auto &group : groups) {
		bool any = false;
		for (const auto &pred : group) {
			if (MatchesTagPredicate(tags, pred)) {
				any = true;
				break;
			}
		}
		if (!any) {
			return false;
		}
	}
	return true;
}

// Check an element's tags against every pushed-down filter on the tags column.
static bool MatchesTagFilters(const osmium::TagList &tags, const std::vector<std::vector<TagPredicate>> &groups,
                              const TagCountFilter &counts) {
	// NOTE: order here is intentional; checking counts first is the fastest way to
	// reject untagged nodes (which make up a large fraction of most OSM PBF files)
	// when a tag predicate is used.
	return counts.Matches(tags) && MatchesTagPredicates(tags, groups);
}

template <typename TAssembler>
class RelationAreaManager
    : public osmium::relations::RelationsManager<RelationAreaManager<TAssembler>, false, true, false> {
	using assembler_config_type = typename TAssembler::config_type;
	assembler_config_type m_assembler_config;
	std::vector<std::vector<TagPredicate>> m_predicates;
	IdFilter m_ids;
	TagCountFilter m_tag_counts;

public:
	explicit RelationAreaManager(assembler_config_type config, std::vector<std::vector<TagPredicate>> predicates = {},
	                             IdFilter ids = {}, TagCountFilter tag_counts = {})
	    : m_assembler_config(std::move(config)), m_predicates(std::move(predicates)), m_ids(std::move(ids)),
	      m_tag_counts(tag_counts) {
	}

	// Called with a relation the assembler failed to build geometry for, so the
	// caller can still emit a row (with NULL geometry) for that relation.
	std::function<void(const osmium::Relation &)> failed_relation_callback;

	bool new_relation(const osmium::Relation &relation) const {
		if (!m_ids.Matches(relation.id())) {
			return false;
		}
		const char *type = relation.tags().get_value_by_key("type");
		if (!type) {
			return false;
		}
		if (std::strcmp(type, "multipolygon") != 0 && std::strcmp(type, "boundary") != 0) {
			return false;
		}
		return MatchesTagFilters(relation.tags(), m_predicates, m_tag_counts);
	}

	void complete_relation(const osmium::Relation &relation) {
		std::vector<const osmium::Way *> ways;
		ways.reserve(relation.members().size());
		for (const auto &member : relation.members()) {
			if (member.ref() != 0) {
				ways.push_back(this->get_member_way(member.ref()));
			}
		}

		bool assembled = false;
		try {
			TAssembler assembler {m_assembler_config};
			assembled = assembler(relation, ways, this->buffer());
		} catch (const osmium::invalid_location &) {
			// assembled stays false; handled below by emitting a NULL-geometry row
		}
		if (!assembled && failed_relation_callback) {
			failed_relation_callback(relation);
		}
	}

	void after_way(const osmium::Way &) const noexcept {
		// closed ways are handled directly in ProcessBuffer; do nothing here
	}
};

static void ResolveWayLocations(location_index_type &index, osmium::Way &way) {
	for (auto &nd : way.nodes()) {
		const auto loc = index.get_noexcept(static_cast<osmium::unsigned_object_id_type>(nd.ref()));
		if (loc) {
			nd.set_location(loc);
		}
	}
}

// Resolves way node locations only for ways in the given ID set.
// Used to resolve locations for multipolygon member ways before the
// assembler runs, without paying the cost of resolving all ways.
class MemberWayLocationResolver : public osmium::handler::Handler {
public:
	MemberWayLocationResolver(location_index_type &index, const std::unordered_set<osmium::object_id_type> &way_ids)
	    : m_index(index), m_way_ids(way_ids) {
	}

	void way(osmium::Way &way) {
		if (m_way_ids.count(way.id())) {
			ResolveWayLocations(m_index, way);
		}
	}

private:
	location_index_type &m_index;
	const std::unordered_set<osmium::object_id_type> &m_way_ids;
};

// Index types that store their data in a mmap'd file on disk, rather than in memory.
// The mmap_array types are NOT among them: those are anonymous mappings, which are
// basically virtual files stored in memory.
//
// Used to determine whether libosmium's index->used_memory() is reporting bytes in RAM
// or on disk; if it's RAM then we pass that along to DuckDB via GetEstimatedCacheMemory(),
// so it'll count towards DuckDB's memory_limit setting. If the index is on disk, we ignore
// those bytes (so they don't count towards memory_limit).
static const char *FILE_BACKED_INDEX_TYPES[] = {"dense_file_array", "sparse_file_array"};

static bool IsFileBackedIndex(const std::string &index_config) {
	auto index_type = index_config.substr(0, index_config.find(','));
	for (const auto *name : FILE_BACKED_INDEX_TYPES) {
		if (index_type == name) {
			return true;
		}
	}
	return false;
}

struct CachedNodeIndex : public duckdb::ObjectCacheEntry {
	std::unique_ptr<location_index_type> index;
	std::string file_path;
	bool file_backed;

	explicit CachedNodeIndex(const std::string &path, const std::string &index_config)
	    : file_path(path), file_backed(IsFileBackedIndex(index_config)) {
		auto &factory = osmium::index::MapFactory<osmium::unsigned_object_id_type, osmium::Location>::instance();
		index = factory.create_map(index_config);
	}

	std::string GetObjectType() override {
		return ObjectType();
	}

	static std::string ObjectType() {
		return "osmium_node_location_index";
	}

	duckdb::optional_idx GetEstimatedCacheMemory() const override {
		if (file_backed) {
			// file backed indexes don't cost any RAM, so we don't want their
			// sizes to count towards DuckDB's memory_limit setting.
			return duckdb::optional_idx {};
		}
		// reporting a size puts the index in DuckDB's object cache LRU, so that
		// it counts towards the memory limit and can be evicted, rather than
		// being pinned for the lifetime of the database connection.
		return index->used_memory();
	}

	bool IsPopulated() const {
		return index && index->size() > 0;
	}

	void Populate(duckdb::ClientContext &context, const std::string &pbf_path) {
		osmium::io::Reader reader {pbf_path, osmium::osm_entity_bits::node, osmium::io::read_meta::no};
		location_handler_type handler {*index};
		while (osmium::memory::Buffer buffer = reader.read()) {
			if (context.interrupted) {
				throw duckdb::InterruptException();
			}

			osmium::apply(buffer, handler);
		}
		reader.close();
	}
};

static std::string GetIndexConfig(duckdb::ClientContext &context) {
	duckdb::Value index_type_val, index_path_val;
	std::string index_type = "flex_mem";
	std::string index_path;

	if (context.TryGetCurrentSetting("osmium_index_type", index_type_val)) {
		auto s = index_type_val.ToString();
		if (!s.empty()) {
			index_type = s;
		}
	}
	if (context.TryGetCurrentSetting("osmium_index_path", index_path_val)) {
		index_path = index_path_val.ToString();
	}

	if (!index_path.empty()) {
		return index_type + "," + index_path;
	}
	return index_type;
}

static std::string MakeCacheKey(duckdb::ClientContext &context, const std::string &file_path,
                                const std::string &index_config) {
	auto &fs = duckdb::FileSystem::GetFileSystem(context);
	auto handle = fs.OpenFile(file_path, duckdb::FileFlags::FILE_FLAGS_READ);
	return "osmium_node_index:" + file_path + ":" + std::to_string(fs.GetFileSize(*handle)) + ":" +
	       std::to_string(fs.GetLastModifiedTime(*handle).value) + ":" + index_config;
}

static duckdb::shared_ptr<CachedNodeIndex> GetOrBuildNodeIndex(duckdb::ClientContext &context,
                                                               const std::string &file_path) {
	auto index_config = GetIndexConfig(context);
	auto cache_key = MakeCacheKey(context, file_path, index_config);

	auto &cache = duckdb::ObjectCache::GetObjectCache(context);
	auto entry = cache.Get<CachedNodeIndex>(cache_key);

	if (entry && entry->IsPopulated()) {
		return entry;
	}

	auto new_entry = duckdb::make_shared_ptr<CachedNodeIndex>(file_path, index_config);
	new_entry->Populate(context, file_path);
	cache.Put(cache_key, new_entry);
	return new_entry;
}

struct OsmBindData : public duckdb::TableFunctionData {
	std::string file_path;
	KindFilter kind_filter;
	TypeFilter type_filter;
	IdFilter id_filter;
	std::vector<std::vector<TagPredicate>> tag_predicates;
	TagCountFilter tag_count_filter;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto copy = duckdb::make_uniq<OsmBindData>();
		copy->file_path = file_path;
		copy->kind_filter = kind_filter;
		copy->type_filter = type_filter;
		copy->id_filter = id_filter;
		copy->tag_predicates = tag_predicates;
		copy->tag_count_filter = tag_count_filter;
		return copy;
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		auto &o = other.Cast<OsmBindData>();
		return file_path == o.file_path && kind_filter == o.kind_filter && type_filter == o.type_filter &&
		       id_filter == o.id_filter && tag_predicates == o.tag_predicates && tag_count_filter == o.tag_count_filter;
	}
};

// Serialize bind data to bytes. DuckDB calls this in the subplan optimizer to
// identify duplicate subplans and avoid running them twice. This can lead to
// bugs if logically different queries serialize to the same bytes, so we need
// to be careful to include every pushed-down filter expression in the serial
// representation. The default implementation of this function only includes
// the function arguments, which will produce wrong results when predicate
// pushdown is implemented.
static void OsmSerialize(duckdb::Serializer &serializer, const duckdb::optional_ptr<duckdb::FunctionData> bind_data_p,
                         const duckdb::TableFunction &) {
	auto &bind_data = bind_data_p->Cast<OsmBindData>();

	duckdb::vector<duckdb::vector<TagPredicate>> groups;
	groups.reserve(bind_data.tag_predicates.size());
	for (const auto &group : bind_data.tag_predicates) {
		groups.emplace_back(group.begin(), group.end());
	}

	// sort the ids so that unordered sets which are equal (by ==) are serialized identically
	duckdb::vector<int64_t> sorted_ids(bind_data.id_filter.ids.begin(), bind_data.id_filter.ids.end());
	std::sort(sorted_ids.begin(), sorted_ids.end());

	serializer.WriteProperty(100, "file_path", bind_data.file_path);
	serializer.WriteProperty(101, "kind_nodes", bind_data.kind_filter.nodes);
	serializer.WriteProperty(102, "kind_lines", bind_data.kind_filter.lines);
	serializer.WriteProperty(103, "kind_areas", bind_data.kind_filter.areas);
	serializer.WriteProperty(104, "kind_relations", bind_data.kind_filter.relations);
	serializer.WriteProperty(105, "tag_predicates", groups);
	serializer.WriteProperty(106, "type_nodes", bind_data.type_filter.nodes);
	serializer.WriteProperty(107, "type_ways", bind_data.type_filter.ways);
	serializer.WriteProperty(108, "type_relations", bind_data.type_filter.relations);
	serializer.WriteProperty(109, "id_filter_active", bind_data.id_filter.active);
	serializer.WriteProperty(110, "id_filter_ids", sorted_ids);
	serializer.WriteProperty(111, "tag_count_min", bind_data.tag_count_filter.min);
	serializer.WriteProperty(112, "tag_count_max", bind_data.tag_count_filter.max);
}

static duckdb::unique_ptr<duckdb::FunctionData> OsmDeserialize(duckdb::Deserializer &deserializer,
                                                               duckdb::TableFunction &) {
	auto bind_data = duckdb::make_uniq<OsmBindData>();
	bind_data->file_path = deserializer.ReadProperty<std::string>(100, "file_path");
	bind_data->kind_filter.nodes = deserializer.ReadProperty<bool>(101, "kind_nodes");
	bind_data->kind_filter.lines = deserializer.ReadProperty<bool>(102, "kind_lines");
	bind_data->kind_filter.areas = deserializer.ReadProperty<bool>(103, "kind_areas");
	bind_data->kind_filter.relations = deserializer.ReadProperty<bool>(104, "kind_relations");

	auto groups = deserializer.ReadProperty<duckdb::vector<duckdb::vector<TagPredicate>>>(105, "tag_predicates");
	bind_data->tag_predicates.reserve(groups.size());
	for (const auto &group : groups) {
		bind_data->tag_predicates.emplace_back(group.begin(), group.end());
	}

	bind_data->type_filter.nodes = deserializer.ReadProperty<bool>(106, "type_nodes");
	bind_data->type_filter.ways = deserializer.ReadProperty<bool>(107, "type_ways");
	bind_data->type_filter.relations = deserializer.ReadProperty<bool>(108, "type_relations");

	bind_data->id_filter.active = deserializer.ReadProperty<bool>(109, "id_filter_active");
	auto ids = deserializer.ReadProperty<duckdb::vector<int64_t>>(110, "id_filter_ids");
	bind_data->id_filter.ids.insert(ids.begin(), ids.end());

	bind_data->tag_count_filter.min = deserializer.ReadProperty<uint64_t>(111, "tag_count_min");
	bind_data->tag_count_filter.max = deserializer.ReadProperty<uint64_t>(112, "tag_count_max");

	return std::move(bind_data);
}

struct OsmGlobalState : public duckdb::GlobalTableFunctionState {
	std::unique_ptr<osmium::io::Reader> reader;

	duckdb::shared_ptr<CachedNodeIndex> cached_index;

	std::unique_ptr<RelationAreaManager<osmium::area::Assembler>> mp_manager;
	std::unordered_set<osmium::object_id_type> mp_member_way_ids;

	osmium::geom::WKBFactory<> wkb_factory {osmium::geom::wkb_type::wkb, osmium::geom::out_type::binary};

	std::vector<OsmRow> current_batch;
	idx_t batch_offset = 0;
	bool exhausted = false;

	KindFilter kind_filter;
	TypeFilter type_filter;
	IdFilter id_filter;
	std::vector<std::vector<TagPredicate>> tag_predicates;
	TagCountFilter tag_count_filter;

	// Column mapping: schema column index -> output vector index (-1 if not projected)
	int col_out[NUM_COLUMNS];

	bool needs_geometry = false;
	bool needs_tags = false;
	bool needs_metadata = false; // are any of the metadata columns projected?
	bool needs_username = false; // username specifically (since copying it requires an allocation)
	bool needs_refs = false;
	bool needs_ref_roles = false;
	bool needs_ref_types = false;

	OsmGlobalState() {
		for (idx_t i = 0; i < NUM_COLUMNS; i++) {
			col_out[i] = -1;
		}
	}

	idx_t MaxThreads() const override {
		return 1; // TODO: could parallize buffer processing (is WKB construction CPU or memory bound?)
	}
};

// Compute the signed area (in square degrees) of a closed way using the shoelace
// formula. A positive area means the ring is wound counter-clockwise.
static double SignedArea(const osmium::Way &way) {
	double sum = 0.0;
	const auto &nodes = way.nodes();
	if (nodes.size() < 3) {
		return 0.0;
	}
	for (auto it = nodes.cbegin(); std::next(it) != nodes.cend(); ++it) {
		const auto a = it->location();
		const auto b = std::next(it)->location();
		sum += a.lon() * b.lat() - b.lon() * a.lat();
	}
	return 0.5 * sum;
}

static std::vector<std::pair<std::string, std::string>> ExtractTags(const osmium::TagList &tags) {
	std::vector<std::pair<std::string, std::string>> result;
	result.reserve(tags.size());
	for (const auto &tag : tags) {
		result.emplace_back(tag.key(), tag.value());
	}
	return result;
}

// Copy an element's metadata (version, timestamp, changeset, uid, username) onto
// the row. The username copy is gated separately because it is the only field that
// allocates; the scalar fields are cheap enough to always populate when any metadata
// column is projected.
static void SetMetadata(OsmRow &row, const osmium::OSMObject &object, bool needs_username) {
	row.version = object.version();
	const auto ts = object.timestamp();
	if (ts.valid()) {
		row.timestamp_seconds = ts.seconds_since_epoch();
	}
	row.changeset = object.changeset();
	row.uid = object.uid();
	if (needs_username) {
		row.username = object.user();
	}
}

// Build an area row (kind=area, type=relation) for a relation, leaving geometry
// NULL. Used when a multipolygon/boundary relation matches the query but its
// geometry can't be assembled (e.g. member ways missing from the file).
static OsmRow MakeRelationAreaRow(const OsmGlobalState &state, const osmium::Relation &relation) {
	OsmRow row;
	row.kind = KIND_AREA;
	row.type = TYPE_RELATION;
	row.id = relation.id();
	if (state.needs_metadata) {
		SetMetadata(row, relation, state.needs_username);
	}
	if (state.needs_tags) {
		row.tags = ExtractTags(relation.tags());
	}
	return row;
}

// Process one osmium buffer: iterate matching elements and append rows
// to state.current_batch. If the MP manager is active, also feeds ways
// to the assembler (which may produce multipolygon area rows via callback).
static void ProcessBuffer(OsmGlobalState &state, osmium::memory::Buffer &buffer) {
	auto &filter = state.kind_filter;
	auto &types = state.type_filter;
	auto &ids = state.id_filter;
	auto &preds = state.tag_predicates;
	auto &tag_counts = state.tag_count_filter;
	auto &batch = state.current_batch;

	// When the MP manager is active, resolve locations for member ways
	// and feed the buffer to the manager. Assembled multipolygon areas
	// are pushed to current_batch by the callback.
	if (state.mp_manager) {
		state.mp_manager->failed_relation_callback = [&state, &batch](const osmium::Relation &relation) {
			batch.push_back(MakeRelationAreaRow(state, relation));
		};
		MemberWayLocationResolver resolver {*state.cached_index->index, state.mp_member_way_ids};
		auto &mp_handler = state.mp_manager->handler([&state, &batch](osmium::memory::Buffer &&area_buffer) {
			for (const auto &area : area_buffer.select<osmium::Area>()) {
				if (area.from_way()) {
					// pretty sure this is always false since our assembler only handles relations,
					// but just to be safe...
					continue;
				}

				if (!MatchesTagFilters(area.tags(), state.tag_predicates, state.tag_count_filter)) {
					// TODO: is this necessary? we already checked in RelationAreaManager::new_relation(),
					// so I think tags will always match here
					continue;
				}

				OsmRow row;
				row.kind = KIND_AREA;
				row.type = TYPE_RELATION;
				row.id = area.orig_id();
				if (state.needs_metadata) {
					SetMetadata(row, area, state.needs_username);
				}
				if (state.needs_tags) {
					row.tags = ExtractTags(area.tags());
				}
				if (state.needs_geometry) {
					try {
						row.geometry = state.wkb_factory.create_multipolygon(area);
					} catch (const osmium::geometry_error &) {
						// leave geometry NULL; emit the row anyway
					} catch (const osmium::invalid_location &) {
						// leave geometry NULL; emit the row anyway
					}
				}
				batch.push_back(std::move(row));
			}
		});
		osmium::apply(buffer, resolver, mp_handler);
	}

	if (filter.nodes && types.nodes) {
		for (const auto &node : buffer.select<osmium::Node>()) {
			if (!ids.Matches(node.id())) {
				continue;
			}
			if (!MatchesTagFilters(node.tags(), preds, tag_counts)) {
				continue;
			}
			OsmRow row;
			row.kind = KIND_NODE;
			row.type = TYPE_NODE;
			row.id = node.id();
			if (state.needs_metadata) {
				SetMetadata(row, node, state.needs_username);
			}
			if (state.needs_tags) {
				row.tags = ExtractTags(node.tags());
			}
			if (state.needs_geometry) {
				try {
					row.geometry = state.wkb_factory.create_point(node);
				} catch (const osmium::invalid_location &) {
					// leave geometry NULL; emit the row anyway
				}
			}
			batch.push_back(std::move(row));
		}
	}

	if ((filter.lines || filter.areas) && types.ways) {
		for (auto &way : buffer.select<osmium::Way>()) {
			if (!ids.Matches(way.id())) {
				continue;
			}
			if (!MatchesTagFilters(way.tags(), preds, tag_counts)) {
				continue;
			}

			if (state.cached_index) {
				ResolveWayLocations(*state.cached_index->index, way);
			}

			bool is_closed = way.is_closed() && way.nodes().size() >= 4;

			const char *area_tag = way.tags().get_value_by_key("area");
			bool area_yes = area_tag && std::strcmp(area_tag, "yes") == 0;
			bool area_no = area_tag && std::strcmp(area_tag, "no") == 0;

			bool emit_line;
			bool emit_area;
			if (!is_closed) {
				emit_line = filter.lines;
				emit_area = false;
			} else if (area_yes) {
				emit_line = false;
				emit_area = filter.areas;
			} else if (area_no) {
				emit_line = filter.lines;
				emit_area = false;
			} else {
				emit_line = filter.lines;
				emit_area = filter.areas;
			}

			if (emit_line) {
				OsmRow row;
				row.kind = KIND_LINE;
				row.type = TYPE_WAY;
				row.id = way.id();
				if (state.needs_metadata) {
					SetMetadata(row, way, state.needs_username);
				}
				if (state.needs_tags) {
					row.tags = ExtractTags(way.tags());
				}
				if (state.needs_geometry) {
					try {
						row.geometry = state.wkb_factory.create_linestring(way);
					} catch (const osmium::geometry_error &) {
						// leave geometry NULL; emit the row anyway
					} catch (const osmium::invalid_location &) {
						// leave geometry NULL; emit the row anyway
					}
				}
				batch.push_back(std::move(row));
			}

			if (emit_area) {
				OsmRow row;
				row.kind = KIND_AREA;
				row.type = TYPE_WAY;
				row.id = way.id();
				if (state.needs_metadata) {
					SetMetadata(row, way, state.needs_username);
				}
				if (state.needs_tags) {
					row.tags = ExtractTags(way.tags());
				}
				if (state.needs_geometry) {
					// OSM ways do not have a guaranteed winding order, so check which
					// direction the way's nodes are wound in and make sure that the
					// resulting polygon has a CCW exterior ring (the OGC convention).
					try {
						const auto dir = SignedArea(way) < 0.0 ? osmium::geom::direction::backward
						                                       : osmium::geom::direction::forward;
						row.geometry = state.wkb_factory.create_polygon(way, osmium::geom::use_nodes::unique, dir);
					} catch (const osmium::geometry_error &) {
						// leave geometry NULL; emit the row anyway
					} catch (const osmium::invalid_location &) {
						// leave geometry NULL; emit the row anyway
					}
				}
				batch.push_back(std::move(row));
			}
		}
	}

	if ((filter.relations || filter.areas) && types.relations) {
		for (const auto &relation : buffer.select<osmium::Relation>()) {
			if (!ids.Matches(relation.id())) {
				continue;
			}
			const char *type = relation.tags().get_value_by_key("type");
			bool is_area_relation =
			    type && (std::strcmp(type, "multipolygon") == 0 || std::strcmp(type, "boundary") == 0);

			if (is_area_relation) {
				// When geometry is needed, relation-based areas are handled
				// by the MP manager/assembler pipeline above.
				// When geometry is not needed, emit them directly here.
				if (state.needs_geometry || !filter.areas) {
					continue;
				}
				if (!MatchesTagFilters(relation.tags(), preds, tag_counts)) {
					continue;
				}
				OsmRow row;
				row.kind = KIND_AREA;
				row.type = TYPE_RELATION;
				row.id = relation.id();
				if (state.needs_metadata) {
					SetMetadata(row, relation, state.needs_username);
				}
				if (state.needs_tags) {
					row.tags = ExtractTags(relation.tags());
				}
				batch.push_back(std::move(row));
				continue;
			}

			if (!filter.relations) {
				continue;
			}
			if (!MatchesTagFilters(relation.tags(), preds, tag_counts)) {
				continue;
			}

			OsmRow row;
			row.kind = KIND_RELATION;
			row.type = TYPE_RELATION;
			row.id = relation.id();
			if (state.needs_metadata) {
				SetMetadata(row, relation, state.needs_username);
			}
			if (state.needs_tags) {
				row.tags = ExtractTags(relation.tags());
			}

			if (state.needs_refs || state.needs_ref_roles || state.needs_ref_types) {
				for (const auto &member : relation.members()) {
					if (state.needs_refs) {
						row.refs.push_back(member.ref());
					}
					if (state.needs_ref_roles) {
						row.ref_roles.emplace_back(member.role());
					}
					if (state.needs_ref_types) {
						switch (member.type()) {
						case osmium::item_type::node:
							row.ref_types.push_back(TYPE_NODE);
							break;
						case osmium::item_type::way:
							row.ref_types.push_back(TYPE_WAY);
							break;
						case osmium::item_type::relation:
							row.ref_types.push_back(TYPE_RELATION);
							break;
						default:
							row.ref_types.push_back(TYPE_NODE);
							break;
						}
					}
				}
			}

			batch.push_back(std::move(row));
		}
	}
}

static duckdb::unique_ptr<duckdb::FunctionData> OsmBind(duckdb::ClientContext &context,
                                                        duckdb::TableFunctionBindInput &input,
                                                        duckdb::vector<duckdb::LogicalType> &return_types,
                                                        duckdb::vector<duckdb::string> &names) {
	auto bind_data = duckdb::make_uniq<OsmBindData>();
	bind_data->file_path = input.inputs[0].GetValue<std::string>();

	names.emplace_back("kind");
	return_types.emplace_back(duckdb::LogicalType::VARCHAR);

	names.emplace_back("type");
	return_types.emplace_back(duckdb::LogicalType::VARCHAR);

	names.emplace_back("id");
	return_types.emplace_back(duckdb::LogicalType::BIGINT);

	names.emplace_back("tags");
	return_types.emplace_back(duckdb::LogicalType::MAP(duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR));

	names.emplace_back("geometry");
	return_types.emplace_back(duckdb::LogicalType::GEOMETRY());

	names.emplace_back("version");
	return_types.emplace_back(duckdb::LogicalType::UINTEGER);

	names.emplace_back("timestamp");
	return_types.emplace_back(duckdb::LogicalType::TIMESTAMP_TZ);

	names.emplace_back("changeset");
	return_types.emplace_back(duckdb::LogicalType::UINTEGER);

	names.emplace_back("uid");
	return_types.emplace_back(duckdb::LogicalType::UINTEGER);

	names.emplace_back("username");
	return_types.emplace_back(duckdb::LogicalType::VARCHAR);

	names.emplace_back("refs");
	return_types.emplace_back(duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT));

	names.emplace_back("ref_roles");
	return_types.emplace_back(duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR));

	names.emplace_back("ref_types");
	return_types.emplace_back(duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR));

	return bind_data;
}

// Resolve a projected column binding to the original schema column index.
// Returns INVALID_INDEX if the binding doesn't refer to our table.
static idx_t ResolveSchemaColumn(const duckdb::LogicalGet &get, idx_t table_index,
                                 const duckdb::ColumnBinding &binding) {
	if (binding.table_index != table_index) {
		return duckdb::DConstants::INVALID_INDEX;
	}
	auto &column_ids = get.GetColumnIds();
	auto proj_idx = binding.column_index;
	if (proj_idx >= column_ids.size()) {
		return duckdb::DConstants::INVALID_INDEX;
	}
	return column_ids[proj_idx].GetPrimaryIndex();
}

// Try to extract a predicate restricting a single column to a set of constants:
//
//   col = 'a'                 ->  {'a'}
//   col IN ('a', 'b')         ->  {'a', 'b'}
//   col = 'a' OR col = 'b'    ->  {'a', 'b'}
//
// On success sets schema_idx to the column and values to the constants. Used by
// the kind, type and id filters; tag filtering has its own pushdown extraction
// logic since the expr shape is different (wrapped in map_extract_value()).
static bool TryExtractColumnConstants(const duckdb::Expression &expr, const duckdb::LogicalGet &get, idx_t table_index,
                                      idx_t &schema_idx, std::vector<duckdb::Value> &values) {
	switch (expr.GetExpressionType()) {
	case duckdb::ExpressionType::COMPARE_EQUAL: {
		auto &comp = expr.Cast<duckdb::BoundComparisonExpression>();

		const duckdb::BoundColumnRefExpression *col = nullptr;
		const duckdb::BoundConstantExpression *val = nullptr;
		if (comp.left->GetExpressionClass() == duckdb::ExpressionClass::BOUND_COLUMN_REF &&
		    comp.right->GetExpressionClass() == duckdb::ExpressionClass::BOUND_CONSTANT) {
			col = &comp.left->Cast<duckdb::BoundColumnRefExpression>();
			val = &comp.right->Cast<duckdb::BoundConstantExpression>();
		} else if (comp.right->GetExpressionClass() == duckdb::ExpressionClass::BOUND_COLUMN_REF &&
		           comp.left->GetExpressionClass() == duckdb::ExpressionClass::BOUND_CONSTANT) {
			col = &comp.right->Cast<duckdb::BoundColumnRefExpression>();
			val = &comp.left->Cast<duckdb::BoundConstantExpression>();
		}
		if (!col || !val || val->value.IsNull()) {
			return false;
		}

		auto idx = ResolveSchemaColumn(get, table_index, col->binding);
		if (idx == duckdb::DConstants::INVALID_INDEX) {
			return false;
		}
		schema_idx = idx;
		values = {val->value};
		return true;
	}
	case duckdb::ExpressionType::COMPARE_IN: {
		auto &op = expr.Cast<duckdb::BoundOperatorExpression>();
		if (op.children.size() < 2) {
			return false;
		}
		if (op.children[0]->GetExpressionClass() != duckdb::ExpressionClass::BOUND_COLUMN_REF) {
			return false;
		}
		auto &col = op.children[0]->Cast<duckdb::BoundColumnRefExpression>();
		auto idx = ResolveSchemaColumn(get, table_index, col.binding);
		if (idx == duckdb::DConstants::INVALID_INDEX) {
			return false;
		}

		std::vector<duckdb::Value> result;
		for (idx_t i = 1; i < op.children.size(); i++) {
			if (op.children[i]->GetExpressionClass() != duckdb::ExpressionClass::BOUND_CONSTANT) {
				return false;
			}
			auto &c = op.children[i]->Cast<duckdb::BoundConstantExpression>();
			if (c.value.IsNull()) {
				return false;
			}
			result.push_back(c.value);
		}

		schema_idx = idx;
		values = std::move(result);
		return true;
	}
	case duckdb::ExpressionType::CONJUNCTION_OR: {
		auto &conj = expr.Cast<duckdb::BoundConjunctionExpression>();

		auto common_idx = duckdb::DConstants::INVALID_INDEX;
		std::vector<duckdb::Value> result;
		for (const auto &child : conj.children) {
			auto child_idx = duckdb::DConstants::INVALID_INDEX;
			std::vector<duckdb::Value> child_values;
			if (!TryExtractColumnConstants(*child, get, table_index, child_idx, child_values)) {
				return false;
			}
			if (common_idx != duckdb::DConstants::INVALID_INDEX && child_idx != common_idx) {
				return false;
			}
			common_idx = child_idx;
			result.insert(result.end(), child_values.begin(), child_values.end());
		}
		if (common_idx == duckdb::DConstants::INVALID_INDEX) {
			return false;
		}

		schema_idx = common_idx;
		values = std::move(result);
		return true;
	}
	default:
		return false;
	}
}

// Interpret constants compared against the kind column as a set of kinds.
// Returns false if one of the given values is unknown or not a string. This
// will skip the predicate pushdown and DuckDB will apply the predicate as a
// post-filter operation instead.
static bool ValuesToKindFilter(const std::vector<duckdb::Value> &values, KindFilter &out) {
	KindFilter result {false, false, false, false};
	for (const auto &value : values) {
		if (value.type().id() != duckdb::LogicalTypeId::VARCHAR) {
			return false;
		}
		auto kind = value.GetValue<std::string>();
		if (kind == "node") {
			result.nodes = true;
		} else if (kind == "line") {
			result.lines = true;
		} else if (kind == "area") {
			result.areas = true;
		} else if (kind == "relation") {
			result.relations = true;
		} else {
			return false;
		}
	}
	out = result;
	return true;
}

// Interpret constants compared against the type column as a set of element types.
// Returns false if one of the given types is unknown or the wrong type (see above).
static bool ValuesToTypeFilter(const std::vector<duckdb::Value> &values, TypeFilter &out) {
	TypeFilter result {false, false, false};
	for (const auto &value : values) {
		if (value.type().id() != duckdb::LogicalTypeId::VARCHAR) {
			return false;
		}
		auto type = value.GetValue<std::string>();
		if (type == "node") {
			result.nodes = true;
		} else if (type == "way") {
			result.ways = true;
		} else if (type == "relation") {
			result.relations = true;
		} else {
			return false;
		}
	}
	out = result;
	return true;
}

// Interpret constants compared against the id column as a set of OSM IDs.
static bool ValuesToIds(const std::vector<duckdb::Value> &values, std::unordered_set<int64_t> &out) {
	std::unordered_set<int64_t> result;
	for (const auto &value : values) {
		if (!value.type().IsIntegral()) {
			return false;
		}
		duckdb::Value id;
		if (!value.DefaultTryCastAs(duckdb::LogicalType::BIGINT, id, nullptr) || id.IsNull()) {
			return false;
		}
		result.insert(id.GetValue<int64_t>());
	}
	out = std::move(result);
	return true;
}

// Check if an expression is map_extract_value(column_ref(tags), constant_key).
// Returns the key string if matched or empty string otherwise.
static std::string TryExtractMapKey(const duckdb::Expression &expr, const duckdb::LogicalGet &get, idx_t table_index) {
	if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_FUNCTION) {
		return "";
	}
	auto &func = expr.Cast<duckdb::BoundFunctionExpression>();
	if (func.function.name != "map_extract_value") {
		return "";
	}
	if (func.children.size() != 2) {
		return "";
	}

	// First child should be a column ref to the tags column
	auto &first = *func.children[0];
	if (first.GetExpressionClass() != duckdb::ExpressionClass::BOUND_COLUMN_REF) {
		return "";
	}
	auto &col = first.Cast<duckdb::BoundColumnRefExpression>();
	auto schema_idx = ResolveSchemaColumn(get, table_index, col.binding);
	if (schema_idx != COL_TAGS) {
		return "";
	}

	// Second child should be a string constant (the key)
	auto &second = *func.children[1];
	if (second.GetExpressionClass() != duckdb::ExpressionClass::BOUND_CONSTANT) {
		return "";
	}
	auto &key_const = second.Cast<duckdb::BoundConstantExpression>();
	if (key_const.value.type().id() != duckdb::LogicalTypeId::VARCHAR) {
		return "";
	}
	return key_const.value.GetValue<std::string>();
}

// Try to extract a tag predicate from:
//   tags['key'] IS NOT NULL          ->  TagPredicate{key, {}}
//   tags['key'] = 'value'            ->  TagPredicate{key, {"value"}}
//   tags['key'] IN ('v1', 'v2', ...) ->  TagPredicate{key, {"v1", "v2", ...}}
static bool TryExtractTagPredicate(const duckdb::Expression &expr, const duckdb::LogicalGet &get, idx_t table_index,
                                   TagPredicate &out) {
	// Case 1: IS NOT NULL(map_extract_value(tags, 'key'))
	if (expr.GetExpressionType() == duckdb::ExpressionType::OPERATOR_IS_NOT_NULL) {
		auto &op = expr.Cast<duckdb::BoundOperatorExpression>();
		if (op.children.size() != 1) {
			return false;
		}
		auto key = TryExtractMapKey(*op.children[0], get, table_index);
		if (key.empty()) {
			return false;
		}
		out.key = std::move(key);
		return true;
	}

	// Case 2: COMPARE_EQUAL(map_extract_value(tags, 'key'), constant)
	//      or COMPARE_EQUAL(constant, map_extract_value(tags, 'key'))
	if (expr.GetExpressionType() == duckdb::ExpressionType::COMPARE_EQUAL) {
		auto &comp = expr.Cast<duckdb::BoundComparisonExpression>();

		const duckdb::Expression *map_expr = nullptr;
		const duckdb::Expression *val_expr = nullptr;

		if (comp.left->GetExpressionClass() == duckdb::ExpressionClass::BOUND_FUNCTION &&
		    comp.right->GetExpressionClass() == duckdb::ExpressionClass::BOUND_CONSTANT) {
			map_expr = comp.left.get();
			val_expr = comp.right.get();
		} else if (comp.right->GetExpressionClass() == duckdb::ExpressionClass::BOUND_FUNCTION &&
		           comp.left->GetExpressionClass() == duckdb::ExpressionClass::BOUND_CONSTANT) {
			map_expr = comp.right.get();
			val_expr = comp.left.get();
		}
		if (!map_expr || !val_expr) {
			return false;
		}

		auto key = TryExtractMapKey(*map_expr, get, table_index);
		if (key.empty()) {
			return false;
		}

		auto &val_const = val_expr->Cast<duckdb::BoundConstantExpression>();
		if (val_const.value.type().id() != duckdb::LogicalTypeId::VARCHAR) {
			return false;
		}

		out.key = std::move(key);
		out.values = {val_const.value.GetValue<std::string>()};
		return true;
	}

	// Case 3: COMPARE_IN(map_extract_value(tags, 'key'), 'v1', 'v2', ...)
	if (expr.GetExpressionType() == duckdb::ExpressionType::COMPARE_IN) {
		auto &op = expr.Cast<duckdb::BoundOperatorExpression>();
		if (op.children.size() < 2) {
			return false;
		}

		auto key = TryExtractMapKey(*op.children[0], get, table_index);
		if (key.empty()) {
			return false;
		}

		std::unordered_set<std::string> vals;
		for (idx_t i = 1; i < op.children.size(); i++) {
			if (op.children[i]->GetExpressionClass() != duckdb::ExpressionClass::BOUND_CONSTANT) {
				return false;
			}
			auto &c = op.children[i]->Cast<duckdb::BoundConstantExpression>();
			if (c.value.type().id() != duckdb::LogicalTypeId::VARCHAR) {
				return false;
			}
			vals.insert(c.value.GetValue<std::string>());
		}

		out.key = std::move(key);
		out.values = std::move(vals);
		return true;
	}

	return false;
}

// Try to extract a CONJUNCTION_OR where every child is a tag predicate.
// Returns the disjunctive group (for use as one element of the outer AND).
static bool TryExtractTagPredicateOr(const duckdb::Expression &expr, const duckdb::LogicalGet &get, idx_t table_index,
                                     std::vector<TagPredicate> &group) {
	if (expr.GetExpressionType() != duckdb::ExpressionType::CONJUNCTION_OR) {
		return false;
	}
	auto &conj = expr.Cast<duckdb::BoundConjunctionExpression>();

	std::vector<TagPredicate> result;
	for (const auto &child : conj.children) {
		TagPredicate pred;
		if (!TryExtractTagPredicate(*child, get, table_index, pred)) {
			return false;
		}
		result.push_back(std::move(pred));
	}

	group = std::move(result);
	return true;
}

// Strip any casts off an expression. Comparing cardinality(tags) (a UBIGINT)
// against an integer literal can leave a cast on either side of the expr.
static const duckdb::Expression &UnwrapCasts(const duckdb::Expression &expr) {
	const duckdb::Expression *result = &expr;
	while (result->GetExpressionClass() == duckdb::ExpressionClass::BOUND_CAST) {
		result = result->Cast<duckdb::BoundCastExpression>().child.get();
	}
	return *result;
}

// Mirror a comparison, so that `n < cardinality(tags)` can be read as
// `cardinality(tags) > n`. DuckDB has FlipComparisonExpression, but
// it doesn't appear to really be part of the public API.
static duckdb::ExpressionType FlipComparison(duckdb::ExpressionType type) {
	switch (type) {
	case duckdb::ExpressionType::COMPARE_LESSTHAN:
		return duckdb::ExpressionType::COMPARE_GREATERTHAN;
	case duckdb::ExpressionType::COMPARE_GREATERTHAN:
		return duckdb::ExpressionType::COMPARE_LESSTHAN;
	case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO;
	case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO;
	default:
		return type; // = and != are symmetric
	}
}

static bool IsTagCardinality(const duckdb::Expression &expr, const duckdb::LogicalGet &get, idx_t table_index) {
	auto &unwrapped = UnwrapCasts(expr);
	if (unwrapped.GetExpressionClass() != duckdb::ExpressionClass::BOUND_FUNCTION) {
		return false;
	}
	auto &func = unwrapped.Cast<duckdb::BoundFunctionExpression>();
	if (func.function.name != "cardinality" || func.children.size() != 1) {
		return false;
	}
	auto &arg = UnwrapCasts(*func.children[0]);
	if (arg.GetExpressionClass() != duckdb::ExpressionClass::BOUND_COLUMN_REF) {
		return false;
	}
	auto &col = arg.Cast<duckdb::BoundColumnRefExpression>();
	return ResolveSchemaColumn(get, table_index, col.binding) == COL_TAGS;
}

// Try to extract a bound on the number of tags from `cardinality(tags) <op> n`.
// Only integral, non-negative constants are pushed down; other cases are left
// for DuckDB to handle with post-filtering (which is slower but still correct).
static bool TryExtractTagCount(const duckdb::Expression &expr, const duckdb::LogicalGet &get, idx_t table_index,
                               TagCountFilter &out) {
	if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_COMPARISON) {
		return false;
	}
	auto &comp = expr.Cast<duckdb::BoundComparisonExpression>();

	// Normalize to `cardinality(tags) <op> constant`, flipping the comparison
	// if the query was written the other way around.
	auto type = expr.GetExpressionType();
	const duckdb::Expression *const_expr = nullptr;
	if (IsTagCardinality(*comp.left, get, table_index)) {
		const_expr = comp.right.get();
	} else if (IsTagCardinality(*comp.right, get, table_index)) {
		const_expr = comp.left.get();
		type = FlipComparison(type);
	} else {
		return false;
	}

	auto &unwrapped = UnwrapCasts(*const_expr);
	if (unwrapped.GetExpressionClass() != duckdb::ExpressionClass::BOUND_CONSTANT) {
		return false;
	}
	auto &value = unwrapped.Cast<duckdb::BoundConstantExpression>().value;
	if (value.IsNull() || !value.type().IsIntegral()) {
		return false;
	}

	duckdb::Value as_bigint;
	if (!value.DefaultTryCastAs(duckdb::LogicalType::BIGINT, as_bigint, nullptr)) {
		return false;
	}
	const auto n = duckdb::BigIntValue::Get(as_bigint);
	if (n < 0) {
		return false;
	}
	const auto count = static_cast<uint64_t>(n);
	const auto unbounded = std::numeric_limits<uint64_t>::max();

	switch (type) {
	case duckdb::ExpressionType::COMPARE_EQUAL:
		out = {count, count};
		return true;
	case duckdb::ExpressionType::COMPARE_NOTEQUAL:
		// A hole in the middle of the range isn't normally representable, but
		// we can special-case zero since != 0 is the same as > 0.
		if (count == 0) {
			out = {1, unbounded};
			return true;
		} else {
			return false;
		}
	case duckdb::ExpressionType::COMPARE_GREATERTHAN:
		out = {count + 1, unbounded};
		return true;
	case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		out = {count, unbounded};
		return true;
	case duckdb::ExpressionType::COMPARE_LESSTHAN:
		// `< 0` matches nothing; we can express that as an empty range.
		out = count == 0 ? TagCountFilter {1, 0} : TagCountFilter {0, count - 1};
		return true;
	case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
		out = {0, count};
		return true;
	default:
		return false;
	}
}

static void OsmComplexFilterPushdown(duckdb::ClientContext &context, duckdb::LogicalGet &get,
                                     duckdb::FunctionData *bind_data_p,
                                     duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &filters) {
	auto &bind_data = bind_data_p->Cast<OsmBindData>();

	for (idx_t i = 0; i < filters.size();) {
		bool consumed = false;

		// Try kind, type and id pushdown. Each restricts a single column to a
		// set of constants, so they share an extractor. Repeated predicates on
		// the same column are ANDed together.
		{
			auto schema_idx = duckdb::DConstants::INVALID_INDEX;
			std::vector<duckdb::Value> values;
			if (TryExtractColumnConstants(*filters[i], get, get.table_index, schema_idx, values)) {
				if (schema_idx == COL_KIND) {
					KindFilter kf;
					if (ValuesToKindFilter(values, kf)) {
						bind_data.kind_filter &= kf;
						consumed = true;
					}
				} else if (schema_idx == COL_TYPE) {
					TypeFilter tf;
					if (ValuesToTypeFilter(values, tf)) {
						bind_data.type_filter &= tf;
						consumed = true;
					}
				} else if (schema_idx == COL_ID) {
					std::unordered_set<int64_t> ids;
					if (ValuesToIds(values, ids)) {
						bind_data.id_filter.Intersect(ids);
						consumed = true;
					}
				}
			}
		}

		// Try single tag predicate pushdown (becomes a group of size 1)
		if (!consumed) {
			TagPredicate pred;
			if (TryExtractTagPredicate(*filters[i], get, get.table_index, pred)) {
				bind_data.tag_predicates.push_back({std::move(pred)});
				consumed = true;
			}
		}

		// Try OR of tag predicates pushdown (becomes a disjunctive group)
		if (!consumed) {
			std::vector<TagPredicate> group;
			if (TryExtractTagPredicateOr(*filters[i], get, get.table_index, group)) {
				bind_data.tag_predicates.push_back(std::move(group));
				consumed = true;
			}
		}

		// Try tag count pushdown. Repeated predicates narrow the range.
		if (!consumed) {
			TagCountFilter tcf;
			if (TryExtractTagCount(*filters[i], get, get.table_index, tcf)) {
				bind_data.tag_count_filter &= tcf;
				consumed = true;
			}
		}

		if (consumed) {
			// Remove the filter: our pushdown is exact, so DuckDB doesn't
			// need to re-evaluate it. This also allows the column pruning
			// optimizer to eliminate columns referenced only by these filters
			// (e.g. the tags column for count(*) queries with tag predicates).
			filters.erase(filters.begin() + i);
		} else {
			i++;
		}
	}
}

static duckdb::unique_ptr<duckdb::GlobalTableFunctionState> OsmInitGlobal(duckdb::ClientContext &context,
                                                                          duckdb::TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<OsmBindData>();
	auto state = duckdb::make_uniq<OsmGlobalState>();

	// Copy query parameters
	state->kind_filter = bind_data.kind_filter;
	state->type_filter = bind_data.type_filter;
	state->id_filter = bind_data.id_filter;
	state->tag_predicates = bind_data.tag_predicates;
	state->tag_count_filter = bind_data.tag_count_filter;

	// Determine which columns are projected
	for (idx_t i = 0; i < input.column_ids.size(); i++) {
		auto col = input.column_ids[i];
		if (col < NUM_COLUMNS) {
			state->col_out[col] = static_cast<int>(i);
		}
	}
	state->needs_geometry = state->col_out[COL_GEOMETRY] >= 0;
	state->needs_tags = state->col_out[COL_TAGS] >= 0;
	state->needs_username = state->col_out[COL_USERNAME] >= 0;
	state->needs_metadata = state->needs_username || state->col_out[COL_VERSION] >= 0 ||
	                        state->col_out[COL_TIMESTAMP] >= 0 || state->col_out[COL_CHANGESET] >= 0 ||
	                        state->col_out[COL_UID] >= 0;
	state->needs_refs = state->col_out[COL_REFS] >= 0;
	state->needs_ref_roles = state->col_out[COL_REF_ROLES] >= 0;
	state->needs_ref_types = state->col_out[COL_REF_TYPES] >= 0;

	auto read_metadata = state->needs_metadata ? osmium::io::read_meta::yes : osmium::io::read_meta::no;

	// Contradictory predicates, e.g. `kind IN ('node') AND kind IN ('area')`,
	// or `kind = 'line' AND type = 'node'`
	if (NoMatchingKindAndType(bind_data.kind_filter, bind_data.type_filter) || bind_data.id_filter.None() ||
	    bind_data.tag_count_filter.None()) {
		state->exhausted = true;
		return state;
	}

	// Which element types produce rows, given both the kind and type filters.
	bool emit_ways = (bind_data.kind_filter.lines || bind_data.kind_filter.areas) && bind_data.type_filter.ways;
	bool emit_relations =
	    (bind_data.kind_filter.relations || bind_data.kind_filter.areas) && bind_data.type_filter.relations;

	// Set up the multipolygon manager if relation-based areas with geometry are needed.
	bool use_mp_manager = bind_data.kind_filter.areas && bind_data.type_filter.relations && state->needs_geometry;

	// Build or retrieve cached node location index if needed. The multipolygon
	// assembler needs member way locations too, even when way rows aren't emitted.
	if (state->needs_geometry && (emit_ways || use_mp_manager)) {
		state->cached_index = GetOrBuildNodeIndex(context, bind_data.file_path);
	}

	if (use_mp_manager) {
		osmium::area::Assembler::config_type assembler_config;
		state->mp_manager = std::make_unique<RelationAreaManager<osmium::area::Assembler>>(
		    assembler_config, bind_data.tag_predicates, bind_data.id_filter, bind_data.tag_count_filter);

		// Read relations, feed them to the MP manager (which filters
		// using MatchesTagPredicates internally), and collect member way IDs
		// so the selective resolver knows which ways need locations.
		osmium::io::Reader rel_reader {bind_data.file_path, osmium::osm_entity_bits::relation, read_metadata};
		while (osmium::memory::Buffer buffer = rel_reader.read()) {
			if (context.interrupted) {
				throw duckdb::InterruptException();
			}

			osmium::apply(buffer, *state->mp_manager);
		}
		rel_reader.close();

		// Collect member way IDs from all relations the manager accepted.
		state->mp_manager->relations_database().for_each_relation([&](const osmium::relations::RelationHandle &handle) {
			for (const auto &member : handle->members()) {
				if (member.type() == osmium::item_type::way) {
					state->mp_member_way_ids.insert(member.ref());
				}
			}
		});
		state->mp_manager->prepare_for_lookup();
	}

	// Open the main reader with the appropriate entity types.
	auto entity_bits = osmium::osm_entity_bits::nothing;
	if (bind_data.kind_filter.nodes && bind_data.type_filter.nodes) {
		entity_bits |= osmium::osm_entity_bits::node;
	}
	if (emit_ways || use_mp_manager) {
		// the assembler consumes member ways from the main pass
		entity_bits |= osmium::osm_entity_bits::way;
	}
	if (emit_relations) {
		entity_bits |= osmium::osm_entity_bits::relation;
	}
	state->reader = std::make_unique<osmium::io::Reader>(bind_data.file_path, entity_bits, read_metadata);

	return state;
}

static void OsmScan(duckdb::ClientContext &context, duckdb::TableFunctionInput &data, duckdb::DataChunk &output) {
	auto &state = data.global_state->Cast<OsmGlobalState>();

	if (state.exhausted && state.batch_offset >= state.current_batch.size()) {
		output.SetCardinality(0);
		return;
	}

	int kind_out = state.col_out[COL_KIND];
	int type_out = state.col_out[COL_TYPE];
	int id_out = state.col_out[COL_ID];
	int tags_out = state.col_out[COL_TAGS];
	int geom_out = state.col_out[COL_GEOMETRY];
	int refs_out = state.col_out[COL_REFS];
	int roles_out = state.col_out[COL_REF_ROLES];
	int types_out = state.col_out[COL_REF_TYPES];
	int version_out = state.col_out[COL_VERSION];
	int ts_out = state.col_out[COL_TIMESTAMP];
	int changeset_out = state.col_out[COL_CHANGESET];
	int uid_out = state.col_out[COL_UID];
	int username_out = state.col_out[COL_USERNAME];

	idx_t tags_offset = 0;
	idx_t refs_offset = 0;
	idx_t ref_roles_offset = 0;
	idx_t ref_types_offset = 0;

	idx_t count = 0;
	idx_t capacity = STANDARD_VECTOR_SIZE;

	while (count < capacity) {
		if (state.batch_offset >= state.current_batch.size()) {
			// current batch is consumed so produce another one
			state.current_batch.clear();
			state.batch_offset = 0;

			if (state.exhausted) {
				break;
			}

			if (context.interrupted) {
				throw duckdb::InterruptException();
			}

			osmium::memory::Buffer buffer = state.reader->read();
			if (!buffer) {
				state.reader->close();
				state.exhausted = true;
				// Emit NULL-geometry area rows for multipolygon/boundary relations
				// that were never assembled, either because member ways were missing
				// from the file or because the relation had no way members at all.
				if (state.mp_manager) {
					state.mp_manager->for_each_incomplete_relation(
					    [&state](const osmium::relations::RelationHandle &handle) {
						    state.current_batch.push_back(MakeRelationAreaRow(state, *handle));
					    });
				}
				if (state.current_batch.empty()) {
					break;
				}
			} else {
				ProcessBuffer(state, buffer);
				if (state.current_batch.empty()) {
					continue;
				}
			}
		}

		while (count < capacity && state.batch_offset < state.current_batch.size()) {
			auto &row = state.current_batch[state.batch_offset++];

			if (kind_out >= 0) {
				auto &vec = output.data[kind_out];
				duckdb::FlatVector::GetData<duckdb::string_t>(vec)[count] =
				    duckdb::StringVector::AddString(vec, KIND_NAMES[row.kind]);
			}

			if (type_out >= 0) {
				auto &vec = output.data[type_out];
				duckdb::FlatVector::GetData<duckdb::string_t>(vec)[count] =
				    duckdb::StringVector::AddString(vec, TYPE_NAMES[row.type]);
			}

			if (id_out >= 0) {
				duckdb::FlatVector::GetData<int64_t>(output.data[id_out])[count] = row.id;
			}

			if (tags_out >= 0) {
				auto &tags_vec = output.data[tags_out];
				auto &tags_keys = duckdb::MapVector::GetKeys(tags_vec);
				auto &tags_values = duckdb::MapVector::GetValues(tags_vec);

				idx_t num_tags = row.tags.size();
				duckdb::ListVector::Reserve(tags_vec, tags_offset + num_tags);
				auto tags_list_data = duckdb::ListVector::GetData(tags_vec);
				tags_list_data[count].offset = tags_offset;
				tags_list_data[count].length = num_tags;
				for (idx_t i = 0; i < num_tags; i++) {
					duckdb::FlatVector::GetData<duckdb::string_t>(tags_keys)[tags_offset + i] =
					    duckdb::StringVector::AddString(tags_keys, row.tags[i].first);
					duckdb::FlatVector::GetData<duckdb::string_t>(tags_values)[tags_offset + i] =
					    duckdb::StringVector::AddString(tags_values, row.tags[i].second);
				}
				tags_offset += num_tags;
				duckdb::ListVector::SetListSize(tags_vec, tags_offset);
			}

			if (geom_out >= 0) {
				auto &vec = output.data[geom_out];
				if (row.geometry.empty()) {
					duckdb::FlatVector::SetNull(vec, count, true);
				} else {
					// CAREFUL: don't write this string_t into vec directly. It points at
					// row.geometry, which is freed when the batch is refilled, possibly
					// before this chunk is finished. FromBinary is safe because it copies
					// the data into vec's string heap.
					duckdb::string_t wkb(row.geometry.data(), row.geometry.size());
					duckdb::string_t geom;
					duckdb::Geometry::FromBinary(wkb, geom, vec, true);
					duckdb::FlatVector::GetData<duckdb::string_t>(vec)[count] = geom;
				}
			}

			if (version_out >= 0) {
				auto &vec = output.data[version_out];
				if (row.version == 0) {
					duckdb::FlatVector::SetNull(vec, count, true);
				} else {
					duckdb::FlatVector::GetData<uint32_t>(vec)[count] = row.version;
				}
			}

			if (ts_out >= 0) {
				auto &vec = output.data[ts_out];
				if (row.timestamp_seconds == 0) {
					duckdb::FlatVector::SetNull(vec, count, true);
				} else {
					duckdb::FlatVector::GetData<duckdb::timestamp_t>(vec)[count] =
					    duckdb::timestamp_t(row.timestamp_seconds * 1000000LL);
				}
			}

			if (changeset_out >= 0) {
				auto &vec = output.data[changeset_out];
				if (row.changeset == 0) {
					duckdb::FlatVector::SetNull(vec, count, true);
				} else {
					duckdb::FlatVector::GetData<uint32_t>(vec)[count] = row.changeset;
				}
			}

			if (uid_out >= 0) {
				auto &vec = output.data[uid_out];
				if (row.uid == 0) {
					duckdb::FlatVector::SetNull(vec, count, true);
				} else {
					duckdb::FlatVector::GetData<uint32_t>(vec)[count] = row.uid;
				}
			}

			if (username_out >= 0) {
				auto &vec = output.data[username_out];
				if (row.username.empty()) {
					duckdb::FlatVector::SetNull(vec, count, true);
				} else {
					duckdb::FlatVector::GetData<duckdb::string_t>(vec)[count] =
					    duckdb::StringVector::AddString(vec, row.username);
				}
			}

			if (refs_out >= 0) {
				auto &vec = output.data[refs_out];
				if (row.kind != KIND_RELATION) {
					duckdb::FlatVector::SetNull(vec, count, true);
				} else {
					auto &refs_child = duckdb::ListVector::GetEntry(vec);
					idx_t num_refs = row.refs.size();
					duckdb::ListVector::Reserve(vec, refs_offset + num_refs);
					auto refs_list_data = duckdb::ListVector::GetData(vec);
					refs_list_data[count].offset = refs_offset;
					refs_list_data[count].length = num_refs;
					auto refs_data = duckdb::FlatVector::GetData<int64_t>(refs_child);
					for (idx_t i = 0; i < num_refs; i++) {
						refs_data[refs_offset + i] = row.refs[i];
					}
					refs_offset += num_refs;
					duckdb::ListVector::SetListSize(vec, refs_offset);
				}
			}

			if (roles_out >= 0) {
				auto &vec = output.data[roles_out];
				if (row.kind != KIND_RELATION) {
					duckdb::FlatVector::SetNull(vec, count, true);
				} else {
					auto &child = duckdb::ListVector::GetEntry(vec);
					idx_t num = row.ref_roles.size();
					duckdb::ListVector::Reserve(vec, ref_roles_offset + num);
					auto list_data = duckdb::ListVector::GetData(vec);
					list_data[count].offset = ref_roles_offset;
					list_data[count].length = num;
					for (idx_t i = 0; i < num; i++) {
						duckdb::FlatVector::GetData<duckdb::string_t>(child)[ref_roles_offset + i] =
						    duckdb::StringVector::AddString(child, row.ref_roles[i]);
					}
					ref_roles_offset += num;
					duckdb::ListVector::SetListSize(vec, ref_roles_offset);
				}
			}

			if (types_out >= 0) {
				auto &vec = output.data[types_out];
				if (row.kind != KIND_RELATION) {
					duckdb::FlatVector::SetNull(vec, count, true);
				} else {
					auto &child = duckdb::ListVector::GetEntry(vec);
					idx_t num = row.ref_types.size();
					duckdb::ListVector::Reserve(vec, ref_types_offset + num);
					auto list_data = duckdb::ListVector::GetData(vec);
					list_data[count].offset = ref_types_offset;
					list_data[count].length = num;
					for (idx_t i = 0; i < num; i++) {
						duckdb::FlatVector::GetData<duckdb::string_t>(child)[ref_types_offset + i] =
						    duckdb::StringVector::AddString(child, TYPE_NAMES[row.ref_types[i]]);
					}
					ref_types_offset += num;
					duckdb::ListVector::SetListSize(vec, ref_types_offset);
				}
			}

			count++;
		}
	}

	output.SetCardinality(count);
}

static duckdb::TableFunction GetOsmScanFunction() {
	duckdb::TableFunction func("osmium_read", {duckdb::LogicalType::VARCHAR}, OsmScan, OsmBind, OsmInitGlobal);
	func.projection_pushdown = true;
	func.pushdown_complex_filter = OsmComplexFilterPushdown;
	func.serialize = OsmSerialize;
	func.deserialize = OsmDeserialize;
	return func;
}

static duckdb::unique_ptr<duckdb::TableRef> OsmReplacementScan(duckdb::ClientContext &context,
                                                               duckdb::ReplacementScanInput &input,
                                                               duckdb::optional_ptr<duckdb::ReplacementScanData> data) {
	auto table_name = duckdb::ReplacementScan::GetFullPath(input);
	if (!duckdb::ReplacementScan::CanReplace(table_name, {"osm.pbf", "osm"})) {
		return nullptr;
	}

	auto table_function = duckdb::make_uniq<duckdb::TableFunctionRef>();
	duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> children;
	children.push_back(duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value(table_name)));
	table_function->function = duckdb::make_uniq<duckdb::FunctionExpression>("osmium_read", std::move(children));

	auto &fs = duckdb::FileSystem::GetFileSystem(context);
	table_function->alias = fs.ExtractBaseName(table_name);

	return table_function;
}

static void LoadInternal(duckdb::ExtensionLoader &loader) {
	loader.RegisterFunction(GetOsmScanFunction());

	auto &db = loader.GetDatabaseInstance();
	auto &config = duckdb::DBConfig::GetConfig(db);
	config.replacement_scans.emplace_back(OsmReplacementScan);

	config.AddExtensionOption("osmium_index_type",
	                          "Node location index type (flex_mem, dense_file_array, dense_mem_array, "
	                          "sparse_mem_array, etc.)",
	                          duckdb::LogicalType::VARCHAR, duckdb::Value("flex_mem"));
	config.AddExtensionOption("osmium_index_path", "File path for file-backed index types (e.g. dense_file_array)",
	                          duckdb::LogicalType::VARCHAR, duckdb::Value(""));
}

namespace duckdb {

void OsmiumExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string OsmiumExtension::Name() {
	return "osmium";
}

std::string OsmiumExtension::Version() const {
#ifdef EXT_VERSION_OSMIUM
	return EXT_VERSION_OSMIUM;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(osmium, loader) {
	LoadInternal(loader);
}
}

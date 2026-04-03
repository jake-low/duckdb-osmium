#define DUCKDB_EXTENSION_MAIN

#include "osmium_extension.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/geometry.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
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
#include <osmium/io/pbf_input.hpp>
#include <osmium/io/xml_input.hpp>
#include <osmium/io/reader.hpp>
#include <osmium/osm/area.hpp>
#include <osmium/osm/node.hpp>
#include <osmium/osm/relation.hpp>
#include <osmium/osm/way.hpp>
#include <osmium/relations/manager_util.hpp>
#include <osmium/visitor.hpp>

#include <cstring>

enum OsmKind : uint8_t { KIND_NODE = 0, KIND_LINE = 1, KIND_AREA = 2, KIND_RELATION = 3 };
enum OsmType : uint8_t { TYPE_NODE = 0, TYPE_WAY = 1, TYPE_RELATION = 2 };

struct OsmRow {
	OsmKind kind;
	OsmType type;
	int64_t id;
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

	bool NeedsNodeIndex() const {
		return lines || areas;
	}

	bool NeedsMultipolygonManager() const {
		return areas;
	}
};

struct TagPredicate {
	std::string key;
	std::unordered_set<std::string> values; // empty = key-exists check; non-empty = value IN values
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
static constexpr idx_t COL_REFS = 5;
static constexpr idx_t COL_REF_ROLES = 6;
static constexpr idx_t COL_REF_TYPES = 7;
static constexpr idx_t NUM_COLUMNS = 8;

// Check whether an element's tags satisfy all pushed-down predicates.
// DuckDB makes OR predicates opaque to us (and evaluates them as a
// post-filter) so we only have to worry about AND semantics here.
static bool MatchesTagPredicate(const osmium::TagList &tags, const TagPredicate &pred) {
	const char *val = tags.get_value_by_key(pred.key.c_str());
	if (!val) {
		return false;
	}
	return pred.values.empty() || pred.values.count(val);
}

static bool MatchesTagPredicates(const osmium::TagList &tags, const std::vector<TagPredicate> &predicates) {
	for (const auto &pred : predicates) {
		if (!MatchesTagPredicate(tags, pred)) {
			return false;
		}
	}
	return true;
}

template <typename TAssembler>
class RelationAreaManager
    : public osmium::relations::RelationsManager<RelationAreaManager<TAssembler>, false, true, false> {
	using assembler_config_type = typename TAssembler::config_type;
	assembler_config_type m_assembler_config;
	std::vector<TagPredicate> m_predicates;

public:
	explicit RelationAreaManager(assembler_config_type config, std::vector<TagPredicate> predicates = {})
	    : m_assembler_config(std::move(config)), m_predicates(std::move(predicates)) {
	}

	bool new_relation(const osmium::Relation &relation) const {
		const char *type = relation.tags().get_value_by_key("type");
		if (!type) {
			return false;
		}
		if (std::strcmp(type, "multipolygon") != 0 && std::strcmp(type, "boundary") != 0) {
			return false;
		}
		if (!MatchesTagPredicates(relation.tags(), m_predicates)) {
			return false;
		}
		return std::any_of(
		    relation.members().cbegin(), relation.members().cend(),
		    [](const osmium::RelationMember &member) { return member.type() == osmium::item_type::way; });
	}

	void complete_relation(const osmium::Relation &relation) {
		std::vector<const osmium::Way *> ways;
		ways.reserve(relation.members().size());
		for (const auto &member : relation.members()) {
			if (member.ref() != 0) {
				ways.push_back(this->get_member_way(member.ref()));
			}
		}

		try {
			TAssembler assembler {m_assembler_config};
			assembler(relation, ways, this->buffer());
		} catch (const osmium::invalid_location &) {
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

struct CachedNodeIndex : public duckdb::ObjectCacheEntry {
	std::unique_ptr<location_index_type> index;
	std::string file_path;

	explicit CachedNodeIndex(const std::string &path, const std::string &index_config) : file_path(path) {
		auto &factory = osmium::index::MapFactory<osmium::unsigned_object_id_type, osmium::Location>::instance();
		index = factory.create_map(index_config);
	}

	std::string GetObjectType() override {
		return ObjectType();
	}

	static std::string ObjectType() {
		return "osmium_node_location_index";
	}

	// Non-evictable: return invalid index
	duckdb::optional_idx GetEstimatedCacheMemory() const override {
		return duckdb::optional_idx {};
	}

	bool IsPopulated() const {
		return index && index->size() > 0;
	}

	void Populate(const std::string &pbf_path) {
		osmium::io::Reader reader {pbf_path, osmium::osm_entity_bits::node};
		location_handler_type handler {*index};
		osmium::apply(reader, handler);
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

static std::string MakeCacheKey(const std::string &file_path, const std::string &index_config) {
	return "osmium_node_index:" + file_path + ":" + index_config;
}

static duckdb::shared_ptr<CachedNodeIndex> GetOrBuildNodeIndex(duckdb::ClientContext &context,
                                                               const std::string &file_path) {
	auto index_config = GetIndexConfig(context);
	auto cache_key = MakeCacheKey(file_path, index_config);

	auto &cache = duckdb::ObjectCache::GetObjectCache(context);
	auto entry = cache.Get<CachedNodeIndex>(cache_key);

	if (entry && entry->IsPopulated()) {
		return entry;
	}

	auto new_entry = duckdb::make_shared_ptr<CachedNodeIndex>(file_path, index_config);
	new_entry->Populate(file_path);
	cache.Put(cache_key, new_entry);
	return new_entry;
}

struct OsmBindData : public duckdb::TableFunctionData {
	std::string file_path;
	KindFilter kind_filter;
	std::vector<TagPredicate> tag_predicates;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto copy = duckdb::make_uniq<OsmBindData>();
		copy->file_path = file_path;
		copy->kind_filter = kind_filter;
		copy->tag_predicates = tag_predicates;
		return copy;
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		auto &o = other.Cast<OsmBindData>();
		return file_path == o.file_path;
	}
};

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
	std::vector<TagPredicate> tag_predicates;

	// Column mapping: schema column index -> output vector index (-1 if not projected)
	int col_out[NUM_COLUMNS];
	bool needs_geometry = false;
	bool needs_tags = false;

	OsmGlobalState() {
		for (idx_t i = 0; i < NUM_COLUMNS; i++) {
			col_out[i] = -1;
		}
	}

	idx_t MaxThreads() const override {
		return 1; // TODO: could parallize buffer processing (is WKB construction CPU or memory bound?)
	}
};

static std::vector<std::pair<std::string, std::string>> ExtractTags(const osmium::TagList &tags) {
	std::vector<std::pair<std::string, std::string>> result;
	result.reserve(tags.size());
	for (const auto &tag : tags) {
		result.emplace_back(tag.key(), tag.value());
	}
	return result;
}

// Process one osmium buffer: iterate matching elements and append rows
// to state.current_batch. If the MP manager is active, also feeds ways
// to the assembler (which may produce multipolygon area rows via callback).
static void ProcessBuffer(OsmGlobalState &state, osmium::memory::Buffer &buffer) {
	auto &filter = state.kind_filter;
	auto &preds = state.tag_predicates;
	auto &batch = state.current_batch;

	// When the MP manager is active, resolve locations for member ways
	// and feed the buffer to the manager. Assembled multipolygon areas
	// are pushed to current_batch by the callback.
	if (state.mp_manager) {
		MemberWayLocationResolver resolver {*state.cached_index->index, state.mp_member_way_ids};
		auto &mp_handler = state.mp_manager->handler([&state, &batch](osmium::memory::Buffer &&area_buffer) {
			for (const auto &area : area_buffer.select<osmium::Area>()) {
				if (area.from_way()) {
					// pretty sure this is always false since our assembler only handles relations,
					// but just to be safe...
					continue;
				}

				if (!MatchesTagPredicates(area.tags(), state.tag_predicates)) {
					// TODO: is this necessary? we already checked in RelationAreaManager::new_relation(),
					// so I think tags will always match here
					continue;
				}

				OsmRow row;
				row.kind = KIND_AREA;
				row.type = TYPE_RELATION;
				row.id = area.orig_id();
				if (state.needs_tags) {
					row.tags = ExtractTags(area.tags());
				}
				if (state.needs_geometry) {
					try {
						row.geometry = state.wkb_factory.create_multipolygon(area);
					} catch (const osmium::geometry_error &) {
						continue;
					}
				}
				batch.push_back(std::move(row));
			}
		});
		osmium::apply(buffer, resolver, mp_handler);
	}

	if (filter.nodes) {
		for (const auto &node : buffer.select<osmium::Node>()) {
			if (node.tags().empty()) {
				continue;
			}
			if (!MatchesTagPredicates(node.tags(), preds)) {
				continue;
			}
			OsmRow row;
			row.kind = KIND_NODE;
			row.type = TYPE_NODE;
			row.id = node.id();
			if (state.needs_tags) {
				row.tags = ExtractTags(node.tags());
			}
			if (state.needs_geometry) {
				try {
					row.geometry = state.wkb_factory.create_point(node);
				} catch (const osmium::invalid_location &) {
					continue;
				}
			}
			batch.push_back(std::move(row));
		}
	}

	if (filter.lines || filter.areas) {
		for (auto &way : buffer.select<osmium::Way>()) {
			if (!MatchesTagPredicates(way.tags(), preds)) {
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
				if (state.needs_tags) {
					row.tags = ExtractTags(way.tags());
				}
				if (state.needs_geometry) {
					try {
						row.geometry = state.wkb_factory.create_linestring(way);
					} catch (const osmium::geometry_error &) {
						continue;
					}
				}
				batch.push_back(std::move(row));
			}

			if (emit_area) {
				OsmRow row;
				row.kind = KIND_AREA;
				row.type = TYPE_WAY;
				row.id = way.id();
				if (state.needs_tags) {
					row.tags = ExtractTags(way.tags());
				}
				if (state.needs_geometry) {
					try {
						row.geometry = state.wkb_factory.create_polygon(way);
					} catch (const osmium::geometry_error &) {
						continue;
					}
				}
				batch.push_back(std::move(row));
			}
		}
	}

	if (filter.relations || filter.areas) {
		for (const auto &relation : buffer.select<osmium::Relation>()) {
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
				if (!MatchesTagPredicates(relation.tags(), preds)) {
					continue;
				}
				OsmRow row;
				row.kind = KIND_AREA;
				row.type = TYPE_RELATION;
				row.id = relation.id();
				if (state.needs_tags) {
					row.tags = ExtractTags(relation.tags());
				}
				batch.push_back(std::move(row));
				continue;
			}

			if (!filter.relations) {
				continue;
			}
			if (!MatchesTagPredicates(relation.tags(), preds)) {
				continue;
			}

			OsmRow row;
			row.kind = KIND_RELATION;
			row.type = TYPE_RELATION;
			row.id = relation.id();
			if (state.needs_tags) {
				row.tags = ExtractTags(relation.tags());
			}

			for (const auto &member : relation.members()) {
				row.refs.push_back(member.ref());
				row.ref_roles.emplace_back(member.role());
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

// Try to extract a kind = 'literal' equality from a comparison expression.
// Returns the kind string if found, empty string otherwise.
static std::string TryExtractKindEquality(const duckdb::Expression &expr, const duckdb::LogicalGet &get,
                                          idx_t table_index) {
	if (expr.GetExpressionType() != duckdb::ExpressionType::COMPARE_EQUAL) {
		return "";
	}
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

	if (!col || !val) {
		return "";
	}

	auto schema_idx = ResolveSchemaColumn(get, table_index, col->binding);
	if (schema_idx != COL_KIND) {
		return "";
	}

	if (val->value.type().id() != duckdb::LogicalTypeId::VARCHAR) {
		return "";
	}

	return val->value.GetValue<std::string>();
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

static void OsmComplexFilterPushdown(duckdb::ClientContext &context, duckdb::LogicalGet &get,
                                     duckdb::FunctionData *bind_data_p,
                                     duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &filters) {
	auto &bind_data = bind_data_p->Cast<OsmBindData>();

	for (idx_t i = 0; i < filters.size();) {
		bool consumed = false;

		// Try kind = 'literal' pushdown
		// TODO: handle kind IN ('foo', 'bar') predicates
		auto kind_str = TryExtractKindEquality(*filters[i], get, get.table_index);
		if (!kind_str.empty()) {
			if (kind_str == "node") {
				bind_data.kind_filter = {true, false, false, false};
			} else if (kind_str == "line") {
				bind_data.kind_filter = {false, true, false, false};
			} else if (kind_str == "area") {
				bind_data.kind_filter = {false, false, true, false};
			} else if (kind_str == "relation") {
				bind_data.kind_filter = {false, false, false, true};
			}
			consumed = true;
		}

		// TODO: handle predicates like kind IN ('node', 'area')

		// Try tag predicate pushdown
		if (!consumed) {
			TagPredicate pred;
			if (TryExtractTagPredicate(*filters[i], get, get.table_index, pred)) {
				bind_data.tag_predicates.push_back(std::move(pred));
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
	state->tag_predicates = bind_data.tag_predicates;

	// Determine which columns are projected
	for (idx_t i = 0; i < input.column_ids.size(); i++) {
		auto col = input.column_ids[i];
		if (col < NUM_COLUMNS) {
			state->col_out[col] = static_cast<int>(i);
		}
	}
	state->needs_geometry = state->col_out[COL_GEOMETRY] >= 0;
	state->needs_tags = state->col_out[COL_TAGS] >= 0;

	// Build or retrieve cached node location index if needed.
	bool needs_node_index = state->needs_geometry && bind_data.kind_filter.NeedsNodeIndex();
	if (needs_node_index) {
		state->cached_index = GetOrBuildNodeIndex(context, bind_data.file_path);
	}

	// Set up the multipolygon manager if areas with geometry are needed.
	bool use_mp_manager = bind_data.kind_filter.NeedsMultipolygonManager() && state->needs_geometry;
	if (use_mp_manager) {
		osmium::area::Assembler::config_type assembler_config;
		state->mp_manager =
		    std::make_unique<RelationAreaManager<osmium::area::Assembler>>(assembler_config, bind_data.tag_predicates);

		// Read relations, feed them to the MP manager (which filters
		// using MatchesTagPredicates internally), and collect member way IDs
		// so the selective resolver knows which ways need locations.
		osmium::io::Reader rel_reader {bind_data.file_path, osmium::osm_entity_bits::relation};
		while (osmium::memory::Buffer buffer = rel_reader.read()) {
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
	if (bind_data.kind_filter.nodes) {
		entity_bits |= osmium::osm_entity_bits::node;
	}
	if (bind_data.kind_filter.lines || bind_data.kind_filter.areas) {
		entity_bits |= osmium::osm_entity_bits::way;
	}
	if (bind_data.kind_filter.relations || bind_data.kind_filter.areas) {
		entity_bits |= osmium::osm_entity_bits::relation;
	}
	state->reader = std::make_unique<osmium::io::Reader>(bind_data.file_path, entity_bits);

	return state;
}

static void OsmScan(duckdb::ClientContext &context, duckdb::TableFunctionInput &data, duckdb::DataChunk &output) {
	auto &state = data.global_state->Cast<OsmGlobalState>();

	if (state.exhausted) {
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

	idx_t tags_offset = 0;
	idx_t refs_offset = 0;
	idx_t ref_roles_offset = 0;
	idx_t ref_types_offset = 0;

	idx_t count = 0;
	idx_t capacity = STANDARD_VECTOR_SIZE;

	while (count < capacity) {
		if (state.batch_offset >= state.current_batch.size()) {
			// current batch is empty so read another one
			state.current_batch.clear();
			state.batch_offset = 0;

			osmium::memory::Buffer buffer = state.reader->read();
			if (!buffer) {
				state.reader->close();
				state.exhausted = true;
				break;
			}
			ProcessBuffer(state, buffer);

			if (state.current_batch.empty()) {
				continue;
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
					auto wkb = duckdb::StringVector::AddStringOrBlob(vec, row.geometry.data(), row.geometry.size());
					duckdb::string_t geom;
					duckdb::Geometry::FromBinary(wkb, geom, vec, true);
					duckdb::FlatVector::GetData<duckdb::string_t>(vec)[count] = geom;
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

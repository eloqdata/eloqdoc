/**
 *    Copyright (C) 2025 EloqData Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the license:
 *    1. GNU Affero General Public License, version 3, as published by the Free
 *    Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */
#include <memory>

#include "mongo/db/modules/eloq/src/base/eloq_catalog_factory.h"
#include "mongo/db/modules/eloq/src/base/eloq_key.h"
#include "mongo/db/modules/eloq/src/base/eloq_record.h"
#include "mongo/db/modules/eloq/src/base/eloq_table_schema.h"

#ifdef verify
#undef verify
#endif

#include "mongo/db/modules/eloq/tx_service/include/cc/ccm_scanner.h"
#include "mongo/db/modules/eloq/tx_service/include/cc/range_cc_map.h"
#include "mongo/db/modules/eloq/tx_service/include/cc/scan.h"
#include "mongo/db/modules/eloq/tx_service/include/cc/template_cc_map.h"
#include "mongo/db/modules/eloq/tx_service/include/sequences/sequences.h"
#include "mongo/db/modules/eloq/tx_service/include/table_statistics.h"
#include "mongo/db/modules/eloq/tx_service/include/type.h"

namespace Eloq {
txservice::TableSchema::uptr MongoCatalogFactory::CreateTableSchema(
    const txservice::TableName& table_name, const std::string& catalog_image, uint64_t version) {
    assert(table_name.Engine() == txservice::TableEngine::EloqDoc);
    return std::make_unique<MongoTableSchema>(table_name, catalog_image, version);
}

txservice::CcMap::uptr MongoCatalogFactory::CreatePkCcMap(
    const txservice::TableName& table_name,
    const txservice::TableSchema* table_schema,
    bool ccm_has_full_entries,
    txservice::CcShard* shard,
    txservice::NodeGroupId cc_ng_id) {
    assert(table_name.Engine() == txservice::TableEngine::EloqDoc);

    uint64_t key_version = table_schema->KeySchema()->SchemaTs();
    return std::make_unique<txservice::TemplateCcMap<MongoKey, MongoRecord, true, true>>(
        shard, cc_ng_id, table_name, key_version, table_schema, ccm_has_full_entries);
}

txservice::CcMap::uptr MongoCatalogFactory::CreateSkCcMap(
    const txservice::TableName& table_name,
    const txservice::TableSchema* table_schema,
    txservice::CcShard* shard,
    txservice::NodeGroupId cc_ng_id) {
    const auto* mongo_table_schema = static_cast<const MongoTableSchema*>(table_schema);
    if (mongo_table_schema != nullptr) {
        uint64_t key_version = table_schema->IndexKeySchema(table_name)->SchemaTs();
        return std::make_unique<txservice::TemplateCcMap<MongoKey, MongoRecord, true, true>>(
            shard, cc_ng_id, table_name, key_version, table_schema, false);
    }
    return nullptr;
}

txservice::CcMap::uptr MongoCatalogFactory::CreateRangeMap(
    const txservice::TableName& range_table_name,
    const txservice::TableSchema* table_schema,
    uint64_t schema_ts,
    txservice::CcShard* shard,
    txservice::NodeGroupId ng_id) {
    assert(range_table_name.Type() == txservice::TableType::RangePartition);
    return std::make_unique<txservice::RangeCcMap<MongoKey>>(
        range_table_name, table_schema, schema_ts, shard, ng_id);
}

std::unique_ptr<txservice::TableRangeEntry> MongoCatalogFactory::CreateTableRange(
    txservice::TxKey start_key,
    uint64_t version_ts,
    int64_t partition_id,
    std::unique_ptr<txservice::StoreRange> slices) {
    assert(start_key.Type() == txservice::KeyType::NegativeInf || start_key.IsOwner());
    // The range's start key must not be null. If the start points to negative
    // infinity, it points to MongoKey::NegativeInfinity().
    const MongoKey* start = start_key.GetKey<MongoKey>();
    assert(start != nullptr);

    txservice::TemplateStoreRange<MongoKey>* range_ptr =
        static_cast<txservice::TemplateStoreRange<MongoKey>*>(slices.release());

    std::unique_ptr<txservice::TemplateStoreRange<MongoKey>> typed_range{range_ptr};

    return std::make_unique<txservice::TemplateTableRangeEntry<MongoKey>>(
        start, version_ts, partition_id, std::move(typed_range));
}

std::unique_ptr<txservice::CcScanner> MongoCatalogFactory::CreatePkCcmScanner(
    txservice::ScanDirection direction, const txservice::KeySchema* key_schema) {
    if (direction == txservice::ScanDirection::Forward) {
        return std::make_unique<txservice::RangePartitionedCcmScanner<MongoKey, MongoRecord, true>>(
            direction, txservice::ScanIndexType::Primary, key_schema);
    } else {
        return std::make_unique<
            txservice::RangePartitionedCcmScanner<MongoKey, MongoRecord, false>>(
            direction, txservice::ScanIndexType::Primary, key_schema);
    }
}

std::unique_ptr<txservice::CcScanner> MongoCatalogFactory::CreateSkCcmScanner(
    txservice::ScanDirection direction, const txservice::KeySchema* compound_key_schema) {
    if (direction == txservice::ScanDirection::Forward) {
        return std::make_unique<txservice::RangePartitionedCcmScanner<MongoKey, MongoRecord, true>>(
            direction, txservice::ScanIndexType::Secondary, compound_key_schema);
    } else {
        return std::make_unique<
            txservice::RangePartitionedCcmScanner<MongoKey, MongoRecord, false>>(
            direction, txservice::ScanIndexType::Secondary, compound_key_schema);
    }
}

std::unique_ptr<txservice::CcScanner> MongoCatalogFactory::CreateRangeCcmScanner(
    txservice::ScanDirection direction,
    const txservice::KeySchema* key_schema,
    const txservice::TableName& range_table_name) {
    assert(range_table_name.Type() == txservice::TableType::RangePartition);
    return std::make_unique<txservice::HashParitionCcScanner<MongoKey, txservice::RangeRecord>>(
        direction,
        range_table_name.IsBase() ? txservice::ScanIndexType::Primary
                                  : txservice::ScanIndexType::Secondary,
        key_schema);
}


std::unique_ptr<txservice::Statistics> MongoCatalogFactory::CreateTableStatistics(
    const txservice::TableSchema* table_schema, txservice::NodeGroupId cc_ng_id) {
    return std::make_unique<txservice::TableStatistics<MongoKey>>(table_schema, cc_ng_id);
}

std::unique_ptr<txservice::Statistics> MongoCatalogFactory::CreateTableStatistics(
    const txservice::TableSchema* table_schema,
    std::unordered_map<txservice::TableName, std::pair<uint64_t, std::vector<txservice::TxKey>>>
        sample_pool_map,
    txservice::CcShard* ccs,
    txservice::NodeGroupId cc_ng_id) {
    return std::make_unique<txservice::TableStatistics<MongoKey>>(
        table_schema, std::move(sample_pool_map), ccs, cc_ng_id);
}

txservice::TxKey MongoCatalogFactory::NegativeInfKey() const {
    return MongoKey::GetNegInfTxKey();
}

txservice::TxKey MongoCatalogFactory::PositiveInfKey() const {
    return MongoKey::GetPosInfTxKey();
}

txservice::TxKey MongoCatalogFactory::CreateTxKey() const {
    return txservice::TxKey{std::make_unique<MongoKey>()};
}

txservice::TxKey MongoCatalogFactory::CreateTxKey(const char* data, size_t size) const {
    return txservice::TxKey{std::make_unique<MongoKey>(data, size)};
}

const txservice::TxKey* MongoCatalogFactory::PackedNegativeInfinity() const {
    return MongoKey::PackedNegativeInfinityTxKey();
}

std::unique_ptr<txservice::TxRecord> MongoCatalogFactory::CreateTxRecord() const {

    return std::make_unique<MongoRecord>();
}


}  // namespace Eloq

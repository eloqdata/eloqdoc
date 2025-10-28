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
#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "mongo/db/modules/eloq/data_substrate/tx_service/include/catalog_factory.h"

namespace Eloq {

class MongoCatalogFactory : public txservice::CatalogFactory {
public:
    MongoCatalogFactory() = default;
    // ~MongoCatalogFactory() override = default;

    txservice::TableSchema::uptr CreateTableSchema(const txservice::TableName& table_name,
                                                   const std::string& catalog_image,
                                                   uint64_t version) override;


    txservice::CcMap::uptr CreatePkCcMap(const txservice::TableName& table_name,
                                         const txservice::TableSchema* table_schema,
                                         bool ccm_has_full_entries,
                                         txservice::CcShard* shard,
                                         txservice::NodeGroupId cc_ng_id) override;

    txservice::CcMap::uptr CreateSkCcMap(const txservice::TableName& table_name,
                                         const txservice::TableSchema* table_schema,
                                         txservice::CcShard* shard,
                                         txservice::NodeGroupId cc_ng_id) override;

    txservice::CcMap::uptr CreateRangeMap(const txservice::TableName& range_table_name,
                                          const txservice::TableSchema* table_schema,
                                          uint64_t schema_ts,
                                          txservice::CcShard* shard,
                                          txservice::NodeGroupId ng_id) override;

    std::unique_ptr<txservice::TableRangeEntry> CreateTableRange(
        txservice::TxKey start_key,
        uint64_t version_ts,
        int64_t partition_id,
        std::unique_ptr<txservice::StoreRange> slices = nullptr) override;

    std::unique_ptr<txservice::CcScanner> CreatePkCcmScanner(
        txservice::ScanDirection direction, const txservice::KeySchema* key_schema) override;

    std::unique_ptr<txservice::CcScanner> CreateSkCcmScanner(
        txservice::ScanDirection direction,
        const txservice::KeySchema* compound_key_schema) override;
    std::unique_ptr<txservice::CcScanner> CreateRangeCcmScanner(
        txservice::ScanDirection direction,
        const txservice::KeySchema* key_schema,
        const txservice::TableName& range_table_name) override;

    std::unique_ptr<txservice::Statistics> CreateTableStatistics(
        const txservice::TableSchema* table_schema, txservice::NodeGroupId cc_ng_id) override;

    std::unique_ptr<txservice::Statistics> CreateTableStatistics(
        const txservice::TableSchema* table_schema,
        std::unordered_map<txservice::TableName, std::pair<uint64_t, std::vector<txservice::TxKey>>>
            sample_pool_map,
        txservice::CcShard* ccs,
        txservice::NodeGroupId cc_ng_id) override;


    txservice::TxKey NegativeInfKey() const override;
    txservice::TxKey PositiveInfKey() const override;

    txservice::TxKey CreateTxKey() const override;
    txservice::TxKey CreateTxKey(const char* data, size_t size) const override;
    const txservice::TxKey* PackedNegativeInfinity() const override;
    std::unique_ptr<txservice::TxRecord> CreateTxRecord() const override;
};

}  // namespace Eloq

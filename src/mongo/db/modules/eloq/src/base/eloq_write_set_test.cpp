/**
 *    Copyright (C) 2026 EloqData Inc.
 *    Licensed under the GNU Affero General Public License, version 3.
 */
#include "mongo/platform/basic.h"

#include "mongo/db/modules/eloq/src/base/eloq_key.h"
#include "mongo/db/modules/eloq/src/base/eloq_record.h"
#include "mongo/unittest/unittest.h"
#include "read_write_set.h"

namespace mongo {
namespace {

using txservice::OperationType;
using txservice::ReadWriteSet;
using txservice::TxErrorCode;
using txservice::TxKey;

void assertWriteResult(TxErrorCode expected, TxErrorCode actual) {
    ASSERT_EQ(static_cast<int>(expected), static_cast<int>(actual));
}

std::unique_ptr<Eloq::MongoRecord> recordWithBytes(size_t bytes) {
    auto record = std::make_unique<Eloq::MongoRecord>();
    record->SetEncodedBlob(std::string(bytes, 'x'));
    return record;
}

TEST(EloqWriteSetBytes, CountsEncodedKeyRecordAndTypeBits) {
    const txservice::TableName table(
        std::string("test"), txservice::TableType::Primary, txservice::TableEngine::EloqDoc);
    ReadWriteSet writes;
    auto key = std::make_unique<Eloq::MongoKey>(std::string("key"));
    auto record = recordWithBytes(32);
    const unsigned char typeBits[] = {1, 2, 3};
    record->SetUnpackInfo(typeBits, sizeof(typeBits));
    const size_t expected = key->SerializedLength() + record->SerializedLength();
    ASSERT_EQ(expected, ReadWriteSet::WriteBytes(TxKey(key.get()), record.get()));
    assertWriteResult(
        TxErrorCode::NO_ERROR,
        writes.AddWrite(table, 1, TxKey(std::move(key)), std::move(record), OperationType::Update));
    ASSERT_EQ(expected, writes.WriteSetBytes());

    // Replacing an existing entry still consumes the bytes of every AddWrite call; admission
    // must not use the final net footprint of the map.
    key = std::make_unique<Eloq::MongoKey>(std::string("key"));
    record = recordWithBytes(16);
    const size_t replacementBytes = key->SerializedLength() + record->SerializedLength();
    assertWriteResult(
        TxErrorCode::NO_ERROR,
        writes.AddWrite(table, 1, TxKey(std::move(key)), std::move(record), OperationType::Update));
    ASSERT_EQ(1U, writes.WriteSetSize());
    ASSERT_EQ(expected + replacementBytes, writes.WriteSetBytes());

    key = std::make_unique<Eloq::MongoKey>(std::string("key"));
    const size_t deleteBytes = key->SerializedLength();
    ASSERT_EQ(deleteBytes, ReadWriteSet::WriteBytes(TxKey(key.get()), nullptr));
    assertWriteResult(
        TxErrorCode::NO_ERROR,
        writes.AddWrite(table, 1, TxKey(std::move(key)), nullptr, OperationType::Delete));
    ASSERT_EQ(expected + replacementBytes + deleteBytes, writes.WriteSetBytes());
    writes.Reset();
    ASSERT_EQ(0U, writes.WriteSetBytes());
}

TEST(EloqWriteSetBytes, AcceptsTheHardLimitAndRejectsTheNextWrite) {
    const txservice::TableName table(
        std::string("test"), txservice::TableType::Primary, txservice::TableEngine::EloqDoc);
    ReadWriteSet writes;
    auto key = std::make_unique<Eloq::MongoKey>(std::string("key"));
    const size_t overhead = key->SerializedLength() + Eloq::MongoRecord().SerializedLength();
    auto record = recordWithBytes(ReadWriteSet::MaxWriteSetBytesCnt - overhead);
    assertWriteResult(
        TxErrorCode::NO_ERROR,
        writes.AddWrite(table, 1, TxKey(std::move(key)), std::move(record), OperationType::Update));
    ASSERT_EQ(size_t{ReadWriteSet::MaxWriteSetBytesCnt}, writes.WriteSetBytes());
    assertWriteResult(TxErrorCode::WRITE_SET_BYTES_COUNT_EXCEED_ERR,
                      writes.AddWrite(table,
                                      1,
                                      TxKey(std::make_unique<Eloq::MongoKey>(std::string("next"))),
                                      nullptr,
                                      OperationType::Delete));
    ASSERT_EQ(size_t{ReadWriteSet::MaxWriteSetBytesCnt}, writes.WriteSetBytes());

    writes.Reset();
    key = std::make_unique<Eloq::MongoKey>(std::string("key"));
    record = recordWithBytes(ReadWriteSet::MaxWriteSetBytesCnt - overhead + 1);
    assertWriteResult(
        TxErrorCode::WRITE_SET_BYTES_COUNT_EXCEED_ERR,
        writes.AddWrite(table, 1, TxKey(std::move(key)), std::move(record), OperationType::Update));
    ASSERT_EQ(0U, writes.WriteSetBytes());
    ASSERT_EQ(0U, writes.WriteSetSize());
}

}  // namespace
}  // namespace mongo

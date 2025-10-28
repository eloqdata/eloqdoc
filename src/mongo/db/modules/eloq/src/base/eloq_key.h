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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "mongo/db/record_id.h"
#include "mongo/db/storage/key_string.h"

#include "mongo/db/modules/eloq/data_substrate/tx_service/include/tx_key.h"

namespace Eloq {
/**
 * MongoKey stores the mem-comparable which equals to KeyString in Mongo.
 */
class MongoKey {
public:
    void Reset() {
        packed_key_.clear();
    }

    explicit MongoKey() = default;

    explicit MongoKey(const char* buf, size_t len) : packed_key_{buf, len} {}
    explicit MongoKey(std::string_view packed_key)
        : packed_key_{packed_key.data(), packed_key.size()} {}
    explicit MongoKey(const std::string& packed_key)
        : packed_key_{packed_key.data(), packed_key.size()} {}
    explicit MongoKey(std::string&& packed_key) : packed_key_{std::move(packed_key)} {}

    // https://stackoverflow.com/questions/76858243/why-is-stdcopy-faster-than-stdstring-constructor
    // https://quick-bench.com/q/2IPRbwhFkUKaRHMC_Ym4JPrBeuw
    // Using std::string constructor directly is better than using std::copy?
    MongoKey(const MongoKey& other) : packed_key_{other.packed_key_} {}
    MongoKey(MongoKey&& other) noexcept : packed_key_{std::move(other.packed_key_)} {}

    explicit MongoKey(const mongo::RecordId& rid);
    explicit MongoKey(const mongo::KeyString& ks);

    static txservice::TxKey Create(const char* data, size_t size) {
        return txservice::TxKey{std::make_unique<MongoKey>(data, size)};
    }

    ~MongoKey() = default;

    static const MongoKey* PackedNegativeInfinity() {
        static char neg_inf_packed_key = 0x00;
        static MongoKey neg_inf_key(&neg_inf_packed_key, 1);
        return &neg_inf_key;
    }

    static MongoKey PackedPositiveInfinity(const txservice::KeySchema* key_schema) {
        size_t max_length = mongo::KeyString::TypeBits::kMaxBytesNeeded;

        MongoKey pos_inf_key;
        pos_inf_key.packed_key_.resize(max_length, 0xFF);
        return pos_inf_key;
    }

    MongoKey& operator=(const MongoKey& rhs) noexcept {
        if (this != &rhs) {
            packed_key_ = rhs.packed_key_;
        }
        return *this;
    }

    MongoKey& operator=(MongoKey&& rhs) noexcept {
        if (this != &rhs) {
            packed_key_ = std::move(rhs.packed_key_);
        }
        return *this;
    }

    friend bool operator==(const MongoKey& lhs, const MongoKey& rhs) {
        const MongoKey* neg_ptr = NegativeInfinity();
        const MongoKey* pos_ptr = PositiveInfinity();

        if (&lhs == neg_ptr || &lhs == pos_ptr || &rhs == neg_ptr || &rhs == pos_ptr) {
            return &lhs == &rhs;
        }

        return lhs.packed_key_ == rhs.packed_key_;
    }

    friend bool operator!=(const MongoKey& lhs, const MongoKey& rhs) {
        return !(lhs == rhs);
    }

    friend bool operator<(const MongoKey& lhs, const MongoKey& rhs) {
        const MongoKey* neg_ptr = NegativeInfinity();
        const MongoKey* pos_ptr = PositiveInfinity();

        if (&lhs == neg_ptr) {
            return &rhs != neg_ptr;
        } else if (&lhs == pos_ptr || &rhs == neg_ptr) {
            return false;
        } else if (&rhs == pos_ptr) {
            return &lhs != pos_ptr;
        }

        return lhs.packed_key_ < rhs.packed_key_;
    }

    friend bool operator<=(const MongoKey& lhs, const MongoKey& rhs) {
        return !(rhs < lhs);
    }

    size_t Hash() const {
        return std::hash<std::string_view>{}(
            std::string_view{packed_key_.data(), packed_key_.size()});
    }

    void Serialize(std::vector<char>& buf, size_t& offset) const {
        buf.resize(offset + sizeof(uint16_t) + packed_key_.size());
        auto len = static_cast<uint16_t>(packed_key_.size());
        auto val_ptr = reinterpret_cast<const char*>(&len);
        std::copy(val_ptr, val_ptr + sizeof(uint16_t), buf.begin() + offset);
        offset += sizeof(uint16_t);
        std::copy(packed_key_.begin(), packed_key_.end(), buf.begin() + offset);
        offset += len;
    }

    void Serialize(std::string& str) const {
        // A 2-byte integer represents key lengths up to 65535, which is far more
        // enough for MySQL index keys.
        auto len = static_cast<uint16_t>(packed_key_.size());
        auto len_ptr = reinterpret_cast<const char*>(&len);
        str.append(len_ptr, sizeof(uint16_t));
        str.append(packed_key_.data(), len);
    }

    size_t SerializedLength() const {
        return sizeof(uint16_t) + packed_key_.size();
    }

    std::string_view KVSerialize() const {
        assert(false);
        return std::string_view();
    }
    void KVDeserialize(const char* buf, size_t len) {
        assert(false);
    }

    void Deserialize(const char* buf, size_t& offset, const txservice::KeySchema* schema) {
        uint16_t len = *reinterpret_cast<const uint16_t*>(buf + offset);
        offset += sizeof(uint16_t);

        packed_key_.resize(len);
        // auto uBuf = reinterpret_cast<const char*>(buf);
        std::copy(buf + offset, buf + offset + len, packed_key_.begin());
        offset += len;
    }

    txservice::TxKey CloneTxKey() const {
        if (this == NegativeInfinity()) {
            return txservice::TxKey(NegativeInfinity());
        } else if (this == PositiveInfinity()) {
            return txservice::TxKey(PositiveInfinity());
        } else {
            return txservice::TxKey(std::make_unique<MongoKey>(*this));
        }
    }

    void Copy(const MongoKey& rhs) {
        *this = rhs;
    }

    txservice::KeyType Type() const {
        if (this == MongoKey::NegativeInfinity()) {
            return txservice::KeyType::NegativeInf;
        } else if (this == MongoKey::PositiveInfinity()) {
            return txservice::KeyType::PositiveInf;
        } else {
            return txservice::KeyType::Normal;
        }
    }

    bool NeedsDefrag(mi_heap_t* heap) {
        if (packed_key_.data() != nullptr) {
            float page_utilization = mi_heap_page_utilization(heap, packed_key_.data());
            if (page_utilization < 0.8) {
                return true;
            }
        }
        return false;
    }

    mongo::RecordId ToRecordId(bool is_long) const;
    std::string ToString() const;

    void SetPackedKey(const mongo::RecordId& rid);

    void SetPackedKey(const char* data, size_t len) {
        // packed_key_ = std::string{data, len};
        packed_key_.resize(len);
        std::copy(data, data + len, packed_key_.begin());
    }

    std::string_view PackedKeyStringView() const {
        return {packed_key_.data(), packed_key_.size()};
    }

    std::string PackedKeyString() const {
        return {packed_key_.data(), packed_key_.size()};
    }

    const char* Data() const {
        return packed_key_.data();
    }

    size_t Size() const {
        return packed_key_.size();
    }

    size_t MemUsage() const {
        // Most C++ implementations provide SSO, Small String Optimization. Based
        // on libstdc++, basic_string<char> has a local capacity of 15, plus one
        // more byte to store '\0', i.e. strings shorter than 16 are stored
        // directly in the String object, only strings longer than 16 have an
        // allocated buffer to store the contents.
        size_t mem_usage = sizeof(MongoKey);
        if (packed_key_.capacity() >= 16) {
            mem_usage += packed_key_.capacity();
        }
        return mem_usage;
    }

    double PosInInterval(const MongoKey& min_key, const MongoKey& max_key) const {
        assert(min_key <= max_key);

        if (*this <= min_key) {
            return 0;
        } else if (max_key <= *this) {
            return 1;
        } else {
            return 0.5;
        }
    }

    double PosInInterval(const txservice::KeySchema* key_schema,
                         const MongoKey& min_key,
                         const MongoKey& max_key) const {
        return PosInInterval(min_key, max_key);
    }

    static const txservice::TxKey* PackedNegativeInfinityTxKey() {
        static const txservice::TxKey packed_negative_infinity_tx_key{PackedNegativeInfinity()};
        return &packed_negative_infinity_tx_key;
    }

    static const MongoKey* NegativeInfinity() {
        static const MongoKey neg_inf;
        return &neg_inf;
    }

    static const MongoKey* PositiveInfinity() {
        static const MongoKey pos_inf;
        return &pos_inf;
    }

    static const txservice::TxKey* NegInfTxKey() {
        static const txservice::TxKey neg_inf_tx_key{NegativeInfinity()};
        return &neg_inf_tx_key;
    }

    static txservice::TxKey GetNegInfTxKey() {
        return txservice::TxKey(NegativeInfinity());
    }

    static const txservice::TxKey* PosInfTxKey() {
        static const txservice::TxKey pos_inf_tx_key{PositiveInfinity()};
        return &pos_inf_tx_key;
    }

    static txservice::TxKey GetPosInfTxKey() {
        return txservice::TxKey(PositiveInfinity());
    }

    static const ::txservice::TxKeyInterface* TxKeyImpl() {
        static const txservice::TxKeyInterface tx_key_impl{*MongoKey::NegativeInfinity()};
        return &tx_key_impl;
    }

private:
    std::string packed_key_;
};
}  // namespace Eloq

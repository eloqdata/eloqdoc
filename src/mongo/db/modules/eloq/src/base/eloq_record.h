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
#include <cstring>
#include <iomanip>
#include <memory>
#include <sstream>
#include <string_view>
#include <utility>
#include <vector>

#include "mongo/db/record_id.h"

#include "tx_record.h"

namespace Eloq {

class MongoRecord final : public txservice::TxRecord {
public:
    void Reset() {
        encoded_blob_.clear();
        unpack_info_.clear();
    }

    MongoRecord() {
        encoded_blob_.reserve(32);
        unpack_info_.reserve(8);
    }

    MongoRecord(const MongoRecord& rhs)
        : encoded_blob_{rhs.encoded_blob_}, unpack_info_{rhs.unpack_info_} {}

    MongoRecord(MongoRecord&& rhs) noexcept
        : encoded_blob_{std::move(rhs.encoded_blob_)}, unpack_info_{std::move(rhs.unpack_info_)} {}

    ~MongoRecord() override = default;

    MongoRecord& operator=(const MongoRecord& other) {
        if (this != &other) {
            encoded_blob_ = other.encoded_blob_;
            unpack_info_ = other.unpack_info_;
        }

        return *this;
    }

    MongoRecord& operator=(MongoRecord&& other) noexcept {
        if (this != &other) {
            encoded_blob_ = std::move(other.encoded_blob_);
            unpack_info_ = std::move(other.unpack_info_);
        }
        return *this;
    }

    size_t Length() const {
        return encoded_blob_.size() + unpack_info_.size();
    }

    size_t MemUsage() const override {
        return sizeof(MongoRecord) + encoded_blob_.capacity() + unpack_info_.capacity();
    }

    /*
     * Keep the same serialization format like the DataStoreServiceClient::SerializeTxRecord
     */
    void Serialize(std::vector<char>& buf, size_t& offset) const override {
        buf.resize(offset + 2 * sizeof(size_t) + encoded_blob_.size() + unpack_info_.size());

        size_t len = unpack_info_.size();
        auto len_ptr = reinterpret_cast<const char*>(&len);

        std::copy(len_ptr, len_ptr + sizeof(size_t), buf.begin() + offset);
        offset += sizeof(size_t);
        std::copy(unpack_info_.begin(), unpack_info_.end(), buf.begin() + offset);
        offset += unpack_info_.size();

        len = encoded_blob_.size();
        std::copy(len_ptr, len_ptr + sizeof(size_t), buf.begin() + offset);
        offset += sizeof(size_t);
        std::copy(encoded_blob_.begin(), encoded_blob_.end(), buf.begin() + offset);
        offset += encoded_blob_.size();
    }

    void Serialize(std::string& str) const override {
        size_t len = unpack_info_.size();
        auto len_ptr = reinterpret_cast<const char*>(&len);

        str.append(len_ptr, sizeof(size_t));
        str.append(unpack_info_.data(), unpack_info_.size());

        len = encoded_blob_.size();
        str.append(len_ptr, sizeof(size_t));
        str.append(encoded_blob_.data(), encoded_blob_.size());
    }

    size_t SerializedLength() const override {
        // unpack_info_ and encoded_blob_ and their length
        return sizeof(size_t) * 2 + unpack_info_.size() + encoded_blob_.size();
    }

    void Deserialize(const char* buf, size_t& offset) override {
        auto len = *reinterpret_cast<const size_t*>(buf + offset);
        offset += sizeof(size_t);
        unpack_info_.resize(len);
        std::copy(buf + offset, buf + offset + len, unpack_info_.begin());
        offset += len;

        len = *reinterpret_cast<const size_t*>(buf + offset);
        offset += sizeof(size_t);
        encoded_blob_.resize(len);
        std::copy(buf + offset, buf + offset + len, encoded_blob_.begin());
        offset += len;
    };

    static TxRecord::Uptr Create() {
        return std::make_unique<MongoRecord>();
    }

    TxRecord::Uptr Clone() const override {
        return std::make_unique<MongoRecord>(*this);
    }

    void Copy(const TxRecord& rhs) override {
        auto typed_rhs = static_cast<const MongoRecord&>(rhs);

        encoded_blob_ = typed_rhs.encoded_blob_;
        unpack_info_ = typed_rhs.unpack_info_;
    }

    std::string ToString() const override {
        if (encoded_blob_.empty()) {
            return {"NULL"};
        }

        std::stringstream ss;
        ss << "0x";
        ss << std::hex << std::setfill('0');
        for (char pos : encoded_blob_) {
            ss << std::setw(2) << static_cast<unsigned>(static_cast<uint8_t>(pos));
        }
        return ss.str();
    }

    // TODO: copy to RecordData directly.

    mongo::RecordId ToRecordId(bool is_long) const;


    void SetUnpackInfo(const unsigned char* unpack_ptr, const size_t unpack_size) override {
        unpack_info_.resize(unpack_size);
        std::copy(unpack_ptr, unpack_ptr + unpack_size, unpack_info_.begin());
    }

    std::string_view UnpackInfoStringView() const {
        return {unpack_info_.data(), unpack_info_.size()};
    }

    void SetEncodedBlob(const unsigned char* blob_ptr, const size_t blob_size) override {
        encoded_blob_.resize(blob_size);
        std::copy(blob_ptr, blob_ptr + blob_size, encoded_blob_.begin());
    }

    void SetEncodedBlob(std::string_view sv) {
        encoded_blob_.resize(sv.size());
        std::copy(sv.begin(), sv.end(), encoded_blob_.begin());
    }

    const char* EncodedBlobData() const override {
        return encoded_blob_.data();
    }
    size_t EncodedBlobSize() const override {
        return encoded_blob_.size();
    }

    bool NeedsDefrag(mi_heap_t* heap) override {
        bool defraged = false;

        if (encoded_blob_.data() != nullptr) {
            float encoded_blob_utilization = mi_heap_page_utilization(heap, encoded_blob_.data());
            if (encoded_blob_utilization < 0.8) {
                defraged = true;
            }
        }

        if (unpack_info_.data() != nullptr) {
            float unpack_info_utilization = mi_heap_page_utilization(heap, unpack_info_.data());
            if (unpack_info_utilization < 0.8) {
                defraged = true;
            }
        }

        return defraged;
    }
    size_t Size() const override {
        return encoded_blob_.size() + unpack_info_.size();
    }

    const char* UnpackInfoData() const override {
        return unpack_info_.data();
    }

    size_t UnpackInfoSize() const override {
        return unpack_info_.size();
    }

    void Prefetch() const override {
        if (encoded_blob_.data()) {
            __builtin_prefetch(encoded_blob_.data(), 1, 1);
        }
        if (unpack_info_.data()) {
            __builtin_prefetch(unpack_info_.data(), 1, 1);
        }
    }

private:
    // Store the information about RecordData
    std::vector<char> encoded_blob_;
    // Store the information about KeyString::TypeBits optionally.
    std::vector<char> unpack_info_;
};

}  // namespace Eloq

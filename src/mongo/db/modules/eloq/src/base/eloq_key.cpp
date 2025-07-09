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
#include <cassert>
#include <string>
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/endian.h"
#include "mongo/util/log.h"

#include "mongo/db/modules/eloq/src/base/eloq_key.h"

namespace Eloq {

MongoKey::MongoKey(const mongo::RecordId& rid) {
    MONGO_LOG(1) << "MongoKey construction. RecordId: " << rid.toString();
    if (rid.isLong()) {
        assert(false);
        int64_t native = rid.getLong();
        int64_t be = mongo::endian::nativeToBig(native);
        packed_key_ = std::string{reinterpret_cast<const char*>(&be), sizeof(be)};
    } else {
        packed_key_ = std::string{rid.getStringView()};
    }
}

MongoKey::MongoKey(const mongo::KeyString& ks) {
    MONGO_LOG(1) << "MongoKey construction. KeyString: " << ks.toString();
    packed_key_ = std::string{ks.getBuffer(), ks.getSize()};
}

mongo::RecordId MongoKey::ToRecordId(bool is_long) const {
    MONGO_LOG(1) << "MongoKey::ToRecordId";
    if (is_long) {
        assert(false);
        invariant(Size() == sizeof(int64_t));
        int64_t recordId = mongo::endian::bigToNative(*reinterpret_cast<const int64_t*>(Data()));
        return mongo::RecordId{recordId};
    } else {
        // MONGO_LOG(1) << "packed_key_.data():" << packed_key_.data()
        //              << ". packed_key_.size():" << packed_key_.size();
        return mongo::RecordId{packed_key_.data(), packed_key_.size()};
    }
}

std::string MongoKey::ToString() const {
    if (Type() == txservice::KeyType::NegativeInf) {
        return {"NegativeInf"};
    }
    if (Type() == txservice::KeyType::PositiveInf) {
        return {"PositiveInf"};
    }
    if (packed_key_.size() == 0) {
        return {"NULL"};
    }

    std::stringstream ss;
    ss << "0x" << std::hex << std::setfill('0');
    for (auto ch : packed_key_) {
        ss << std::setw(2) << static_cast<unsigned>(static_cast<uint8_t>(ch));
    }

    return ss.str();
}

void MongoKey::SetPackedKey(const mongo::RecordId& rid) {
    invariant(!rid.isLong());
    std::string_view sv = rid.getStringView();
    packed_key_.resize(sv.size());
    std::copy(sv.begin(), sv.end(), packed_key_.begin());
}

};  // namespace Eloq

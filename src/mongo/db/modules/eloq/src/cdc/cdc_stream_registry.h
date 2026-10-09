/**
 *    Copyright (C) 2026-present EloqData Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/**
 * @file
 * The change streams open on this server, which serverStatus reports in its eloqChangeStreams
 * section (docs/cdc-change-stream.md §8; plan C12).
 */

#pragma once

#include <cstdint>
#include <map>
#include <string>

#include "mongo/db/modules/eloq/src/cdc/cdc_event_stream.h"
#include "mongo/stdx/mutex.h"

namespace mongo {

/**
 * @brief The open change streams, each by the stats its CdcEventStream keeps.
 *
 * A stream's stage adds it when made and removes it when destroyed; the stats it registers must
 * stay valid until then. Removing a stream folds its dedup-set overflows into a total, so the
 * section's count covers the server's lifetime.
 */
class CdcStreamRegistry {
public:
    /** @brief One open stream. */
    struct Entry {
        std::string scope;            ///< "deployment", "database" or "collection".
        std::string ns;               ///< The database or collection it watches; "" for all.
        const CdcStreamStats* stats;  ///< Its stream's stats.
    };

    /**
     * @brief The server's registry.
     *
     * @return The registry.
     */
    static CdcStreamRegistry& get();

    /**
     * @brief Adds an open stream.
     *
     * @param scope Its scope.
     * @param ns What it watches.
     * @param stats Its stream's stats, valid until remove().
     * @return The stream's id, for remove().
     */
    uint64_t add(std::string scope, std::string ns, const CdcStreamStats* stats);

    /**
     * @brief Removes a stream, folding its dedup-set overflows into the lifetime total.
     *
     * @param id What add() returned.
     */
    void remove(uint64_t id);

    /**
     * @brief Calls fn(entry) for each open stream and reads the dedup-set overflows of the
     * streams already closed, under one hold of the registry's lock, so that a stream closing
     * meanwhile is counted once, either open or closed, never in neither: the lifetime total
     * never goes back. fn must be quick and must not call back into the registry.
     *
     * @param fn The function.
     * @return The dedup-set overflows of the streams already closed.
     */
    template <typename Fn>
    uint64_t forEach(Fn fn) const {
        stdx::lock_guard<stdx::mutex> lk(_mutex);
        for (const auto& idAndEntry : _entries) {
            fn(idAndEntry.second);
        }
        return _closedDedupOverflows;
    }

private:
    mutable stdx::mutex _mutex;            ///< Guards the members below.
    std::map<uint64_t, Entry> _entries;    ///< The open streams, by id.
    uint64_t _nextId = 1;                  ///< The next stream's id.
    uint64_t _closedDedupOverflows = 0;    ///< The overflows of the streams removed.
};

}  // namespace mongo

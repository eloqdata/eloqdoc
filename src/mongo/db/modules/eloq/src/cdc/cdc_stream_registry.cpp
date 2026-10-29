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

#include "mongo/platform/basic.h"

#include "mongo/db/modules/eloq/src/cdc/cdc_stream_registry.h"

#include <algorithm>

#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/commands/server_status.h"
#include "mongo/db/modules/eloq/data_substrate/core/include/data_substrate.h"
#include "mongo/db/modules/eloq/data_substrate/tx_service/include/tx_service.h"
#include "mongo/util/time_support.h"

namespace mongo {

CdcStreamRegistry& CdcStreamRegistry::get() {
    static CdcStreamRegistry* registry = new CdcStreamRegistry();
    return *registry;
}

uint64_t CdcStreamRegistry::add(std::string scope, std::string ns, const CdcStreamStats* stats) {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    const uint64_t id = _nextId++;
    _entries.emplace(id, Entry{std::move(scope), std::move(ns), stats});
    return id;
}

void CdcStreamRegistry::remove(uint64_t id) {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    auto it = _entries.find(id);
    if (it == _entries.end()) {
        return;
    }
    _closedDedupOverflows += it->second.stats->dedupOverflows.load(std::memory_order_relaxed);
    _entries.erase(it);
}

namespace {

/** The most streams the section lists one by one; the totals cover them all. */
constexpr int kMaxListedStreams = 1000;

/**
 * @brief serverStatus' eloqChangeStreams section:
 * { open, maxLagMicros, dedupSetOverflows, checkpointIntervalMillis,
 *   streams: [{scope, ns, lagMicros, emitBound, producedThrough, dedupSetOverflows, idleMillis}],
 *   streamsTruncated }.
 *
 * A stream's lag is how far the timestamp it has produced through lies below the emit bound it
 * last saw: what its consumer could already have. Both move only when the stream is pulled, so
 * idleMillis, the time since its last pull (-1 before its first), tells a caught-up stream from
 * one whose consumer stopped reading. dedupSetOverflows counts the changes a full dedup set did not
 * record (§5.8), over the server's lifetime; each may have been returned twice, an idempotent
 * repeat. checkpointIntervalMillis is this node's checkpoint round: the streams' watermark, the log
 * group's lowest reported checkpoint timestamp, advances at most once a round (§5.7).
 */
class EloqChangeStreamsSection : public ServerStatusSection {
public:
    EloqChangeStreamsSection() : ServerStatusSection("eloqChangeStreams") {}

    bool includeByDefault() const override {
        return true;
    }

    BSONObj generateSection(OperationContext* opCtx,
                            const BSONElement& configElement) const override {
        const CdcStreamRegistry& registry = CdcStreamRegistry::get();
        const long long now = Date_t::now().toMillisSinceEpoch();
        long long open = 0;
        long long maxLag = 0;
        unsigned long long overflows = 0;
        BSONArrayBuilder streams;
        overflows += registry.forEach([&](const CdcStreamRegistry::Entry& entry) {
            const CdcStreamStats& stats = *entry.stats;
            const uint64_t bound = stats.emitBound.load(std::memory_order_relaxed);
            const uint64_t through = stats.producedThrough.load(std::memory_order_relaxed);
            const uint64_t streamOverflows = stats.dedupOverflows.load(std::memory_order_relaxed);
            const long long lastPull = stats.lastPullMillis.load(std::memory_order_relaxed);
            const long long lag = bound > through ? static_cast<long long>(bound - through) : 0;
            ++open;
            maxLag = std::max(maxLag, lag);
            overflows += streamOverflows;
            if (open <= kMaxListedStreams) {
                BSONObjBuilder stream(streams.subobjStart());
                stream.append("scope", entry.scope);
                stream.append("ns", entry.ns);
                stream.append("lagMicros", lag);
                stream.append("emitBound", static_cast<long long>(bound));
                stream.append("producedThrough", static_cast<long long>(through));
                stream.append("dedupSetOverflows", static_cast<long long>(streamOverflows));
                stream.append("idleMillis", lastPull > 0 ? now - lastPull : -1LL);
            }
        });
        BSONObjBuilder section;
        section.append("open", open);
        section.append("maxLagMicros", maxLag);
        section.append("dedupSetOverflows", static_cast<long long>(overflows));
        const txservice::Checkpointer* checkpointer =
            DataSubstrate::Instance().GetTxService()->GetCheckpointer();
        section.append("checkpointIntervalMillis",
                       1000LL * static_cast<long long>(checkpointer->CheckpointInterval()));
        section.append("streams", streams.arr());
        if (open > kMaxListedStreams) {
            section.append("streamsTruncated", true);
        }
        return section.obj();
    }
} eloqChangeStreamsSection;

}  // namespace
}  // namespace mongo

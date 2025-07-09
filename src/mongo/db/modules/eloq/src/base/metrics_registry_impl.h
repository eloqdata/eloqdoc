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
#include <mutex>
#include <string>
#include <unordered_map>

#include "metrics.h"
#include "metrics_manager.h"

namespace Eloq {
class MetricsRegistryImpl : public metrics::MetricsRegistry {
public:
    struct MetricsRegistryResult {
        std::unique_ptr<metrics::MetricsRegistry> metrics_registry_;
        const char* not_ok_;
    };

    MetricsRegistryImpl(MetricsRegistryImpl const&) = delete;
    void operator=(MetricsRegistryImpl const&) = delete;

    ~MetricsRegistryImpl() = default;
    static MetricsRegistryResult GetRegistry();

    metrics::MetricsErrors Open() override;
    metrics::MetricKey Register(const metrics::Name&,
                                metrics::Type,
                                const metrics::Labels&) override;
    void Collect(metrics::MetricKey, const metrics::Value&) override;

private:
    MetricsRegistryImpl() = default;

    metrics::MetricsMgr::MetricsMgrResult metrics_mgr_result_ =
        metrics::MetricsMgr::GetMetricMgrInstance();

    std::unordered_map<metrics::MetricKey, std::unique_ptr<metrics::CollectorWrapper>>
        collectors_{};

    // Mutex to exclusively protect 'collectors_' during write operations in
    // multi-threaded environments. Use this mutex solely for guarding
    // modifications to 'collectors_'.
    std::mutex collectors_mu_;
};
}  // namespace Eloq
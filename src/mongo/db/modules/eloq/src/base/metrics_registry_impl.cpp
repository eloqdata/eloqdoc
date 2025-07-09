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
#include <mutex>
#include <utility>

#include <glog/logging.h>

#include "metrics_registry_impl.h"

namespace Eloq {
MetricsRegistryImpl::MetricsRegistryResult MetricsRegistryImpl::GetRegistry() {
    struct make_registry_shared : public MetricsRegistryImpl {};
    static std::unique_ptr<MetricsRegistryImpl> registry_impl =
        std::make_unique<make_registry_shared>();

    if (registry_impl->metrics_mgr_result_.not_ok_ == nullptr) {
        return MetricsRegistryImpl::MetricsRegistryResult{std::move(registry_impl), nullptr};
    } else {
        return MetricsRegistryImpl::MetricsRegistryResult{
            nullptr,
            registry_impl->metrics_mgr_result_.not_ok_,
        };
    }
}

//  This method is the one that needs to be extended, the open method does not
//  do anything for the current implementation.
metrics::MetricsErrors MetricsRegistryImpl::Open() {
    return metrics::MetricsErrors::Success;
}

metrics::MetricKey MetricsRegistryImpl::Register(const metrics::Name& name,
                                                 metrics::Type type,
                                                 const metrics::Labels& labels) {
    auto metric = metrics::Metric(name.GetName(), type, labels);

    auto metric_collector =
        metrics_mgr_result_.mgr_->MetricsRegistry(std::make_unique<metrics::Metric>(metric));

    auto key = metric_collector->metric_key_;

    std::unique_lock<std::mutex> lock(collectors_mu_);
    collectors_.insert(std::make_pair(key, std::move(metric_collector)));

    return key;
}

void MetricsRegistryImpl::Collect(metrics::MetricKey key, const metrics::Value& val) {
    auto collector = collectors_[key].get();
    collector->Collect(val);
}
}  // namespace Eloq
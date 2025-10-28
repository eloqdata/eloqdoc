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
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include <cstdint>
#include <gflags/gflags.h>
#include <limits>
#include <string>

#include "mongo/platform/basic.h"

#include "mongo/base/status.h"
#include "mongo/db/modules/eloq/data_substrate/core/include/data_substrate.h"
#include "mongo/db/modules/eloq/data_substrate/tx_service/raft_host_manager/include/INIReader.h"
#include "mongo/db/modules/eloq/src/eloq_global_options.h"
#include "mongo/db/server_options.h"
#include "mongo/util/log.h"
#include "mongo/util/options_parser/constraints.h"
#include "mongo/util/options_parser/option_description.h"

DECLARE_string(data_substrate_config);
DECLARE_int32(core_number);
DECLARE_bool(bootstrap);
DECLARE_bool(enable_data_store);
const auto NUM_VCPU = std::thread::hardware_concurrency();

namespace mongo {
EloqGlobalOptions eloqGlobalOptions;

Status EloqGlobalOptions::add(moe::OptionSection* options) {
    MONGO_LOG(0) << "EloqGlobalOptions::add";
    moe::OptionSection eloqOptions("Eloq options");

    eloqOptions
        .addOptionChaining("storage.eloq.txService.ccProtocol",
                           "eloqCcProtocol",
                           moe::String,
                           "Concurrency control protocol.(OCC|OccRead|Locking)")
        .setDefault(moe::Value("OccRead"));

    return options->addSection(eloqOptions);
}

Status EloqGlobalOptions::store(const moe::Environment& params,
                                const std::vector<std::string>& args) {

    MONGO_LOG(1) << "EloqGlobalOptions::store";
    if (params.count("storage.eloq.txService.ccProtocol")) {
        const std::string& s = params["storage.eloq.txService.ccProtocol"].as<std::string>();
        if (s == "OCC") {
            ccProtocol = txservice::CcProtocol::OCC;
        } else if (s == "OccRead") {
            ccProtocol = txservice::CcProtocol::OccRead;
        } else if (s == "Locking") {
            ccProtocol = txservice::CcProtocol::Locking;
        } else {
            return Status{ErrorCodes::InvalidOptions,
                          str::stream() << s << " is not a valid CcProtocol"};
        }
    }

    // read data substrate config file for options necessary
    // for eloqdoc storage engine
    std::string& data_substrate_config = FLAGS_data_substrate_config;
    INIReader ds_config_reader(data_substrate_config);
    if (!data_substrate_config.empty() && ds_config_reader.ParseError() != 0) {
        MONGO_LOG(1) << "Failed to parse config file: " << data_substrate_config;
        return Status{ErrorCodes::InvalidOptions,
                      str::stream() << "Failed to parse config file: " << data_substrate_config};
    }

    bool enable_data_store = !CheckCommandLineFlagIsDefault("enable_data_store")
        ? FLAGS_enable_data_store
        : ds_config_reader.GetBoolean("local", "enable_data_store", FLAGS_enable_data_store);

    bool bootstrap = !CheckCommandLineFlagIsDefault("bootstrap")
        ? FLAGS_bootstrap
        : ds_config_reader.GetBoolean("local", "bootstrap", FLAGS_bootstrap);
    serverGlobalParams.bootstrap = bootstrap;
    MONGO_LOG(1) << "serverGlobalParams.bootstrap: " << serverGlobalParams.bootstrap;

    const char* field_core = "core_number";
    uint32_t core_num = FLAGS_core_number;
    if (CheckCommandLineFlagIsDefault(field_core)) {
        if (ds_config_reader.HasValue("local", field_core)) {
            core_num = ds_config_reader.GetInteger("local", field_core, 0);
            assert(core_num);
        } else {
            if (!NUM_VCPU) {
                MONGO_LOG(1) << "config is missing: " << field_core;
                return Status{ErrorCodes::InvalidOptions,
                              str::stream() << "config is missing: " << field_core};
            }
            const uint min = 1;
            if (enable_data_store) {
                core_num = std::max(min, (NUM_VCPU * 3) / 5);
                MONGO_LOG(1) << "give cpus to checkpointer " << core_num;
            } else {
                core_num = std::max(min, (NUM_VCPU * 7) / 10);
            }
            MONGO_LOG(1) << "config is automatically set: " << field_core << "=" << core_num
                         << ", vcpu=" << NUM_VCPU;
        }
    }
    serverGlobalParams.reservedThreadNum = core_num;
    MONGO_LOG(1) << "serverGlobalParams.reservedThreadNum: "
                 << serverGlobalParams.reservedThreadNum;

    return Status::OK();
}

}  // namespace mongo

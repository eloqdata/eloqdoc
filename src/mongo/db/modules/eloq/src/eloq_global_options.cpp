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
#include <filesystem>
#include <gflags/gflags.h>
#include <limits>
#include <string>

#include "mongo/platform/basic.h"

#include "mongo/base/status.h"
#include "mongo/db/modules/eloq/data_substrate/core/include/data_substrate.h"
#include "mongo/db/modules/eloq/data_substrate/core/include/glog_error_logging.h"
#include "mongo/db/modules/eloq/src/eloq_global_options.h"
#include "mongo/db/server_options.h"
#include "mongo/util/log.h"
#include "mongo/util/options_parser/constraints.h"
#include "mongo/util/options_parser/option_description.h"

DEFINE_string(data_substrate_config, "", "Data Substrate Configuration");
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
    MONGO_LOG(1) << "serverGlobalParams.logpath: " << serverGlobalParams.logpath;
    std::filesystem::path systemLogPath(serverGlobalParams.logpath);
    if (systemLogPath.has_parent_path()) {
        static std::filesystem::path logdir = systemLogPath.parent_path();
        GFLAGS_NAMESPACE::SetCommandLineOption("log_dir", logdir.c_str());
    }
    const char* tmp[] = {"eloqdb", nullptr};
    char** dummy_argv = const_cast<char**>(tmp);
    InitGoogleLogging(dummy_argv);

    DataSubstrate::Instance().Init(FLAGS_data_substrate_config);
    serverGlobalParams.bootstrap = DataSubstrate::Instance().GetCoreConfig().bootstrap;
    MONGO_LOG(1) << "serverGlobalParams.bootstrap: " << serverGlobalParams.bootstrap;
    serverGlobalParams.reservedThreadNum = DataSubstrate::Instance().GetCoreConfig().core_num;
    MONGO_LOG(1) << "serverGlobalParams.reservedThreadNum: "
                 << serverGlobalParams.reservedThreadNum;

    return Status::OK();
}

}  // namespace mongo

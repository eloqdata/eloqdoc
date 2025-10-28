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

#include "mongo/util/net/hostandport.h"
#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"
#include <cstdint>
#include <sys/types.h>

#include <cstdint>
#include <sys/types.h>

#include "mongo/db/modules/eloq/data_substrate/tx_service/include/cc_protocol.h"


namespace mongo {

namespace moe = mongo::optionenvironment;

class EloqGlobalOptions {
public:
    EloqGlobalOptions() = default;
    Status add(moe::OptionSection* options);
    Status store(const moe::Environment& params, const std::vector<std::string>& args);

    txservice::CcProtocol ccProtocol{txservice::CcProtocol::OccRead};
};

extern EloqGlobalOptions eloqGlobalOptions;
}  // namespace mongo

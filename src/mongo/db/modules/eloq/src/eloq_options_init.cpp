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
#include "mongo/platform/basic.h"

#include "mongo/util/options_parser/startup_option_init.h"

#include <iostream>

#include "mongo/db/modules/eloq/src/eloq_global_options.h"
#include "mongo/util/exit_code.h"
#include "mongo/util/options_parser/startup_options.h"

namespace mongo {

MONGO_MODULE_STARTUP_OPTIONS_REGISTER(eloqOptions)(InitializerContext* context) {
    return eloqGlobalOptions.add(&moe::startupOptions);
}

MONGO_STARTUP_OPTIONS_VALIDATE(eloqOptions)(InitializerContext* context) {
    return Status::OK();
}

MONGO_STARTUP_OPTIONS_STORE(eloqOptions)(InitializerContext* context) {
    Status ret = eloqGlobalOptions.store(moe::startupOptionsParsed, context->args());
    if (!ret.isOK()) {
        std::cerr << ret.toString() << std::endl;
        std::cerr << "try '" << context->args()[0] << " --help' for more information" << std::endl;
        ::_exit(EXIT_BADOPTIONS);
    }
    return Status::OK();
}

}  // namespace mongo

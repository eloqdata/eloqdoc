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

/**
 * @file
 * Test-only commands of the Eloq storage engine, registered only when test commands are enabled
 * (--setParameter enableTestCommands=1).
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include <string>
#include <vector>

#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/db/commands/test_commands_enabled.h"
#include "mongo/db/operation_context.h"
#include "mongo/util/log.h"

#include "mongo/db/modules/eloq/src/eloq_recovery_unit.h"

#include "mongo/db/modules/eloq/data_substrate/tx_service/include/fault/fault_inject.h"
#include "mongo/db/modules/eloq/data_substrate/tx_service/include/tx_request.h"

namespace mongo {
namespace {

/**
 * @brief Arms, disarms or queries a fault point of the data substrate, as EloqKV's FAULT_INJECT
 * command arms one.
 *
 * Forms:
 * - { eloqFaultInject: <fault name>, params: <string>, nodes: [<node id>, ...] } arms the fault.
 *   `params` is the fault's parameter string (for example "action=PANIC"); "remove" disarms the
 *   fault. Omitted, the fault is armed with no parameters, which is what a CODE_FAULT_INJECTOR
 *   point checks for. `nodes` lists the nodes to arm; omitted, the local node; [-1], every node.
 * - { eloqFaultInject: <fault name>, query: true } replies { armed: <bool> }: whether the fault is
 *   armed on this node. A fault that disarms itself when it fires reads false once it has fired.
 *
 * @note The data substrate compiles its fault points in only with WITH_FAULT_INJECT (EloqDoc
 * Debug builds). Without them an armed fault would never fire, so every form fails with
 * CommandNotSupported: a test cannot pass without exercising the fault.
 */
class EloqFaultInjectCmd : public BasicCommand {
public:
    EloqFaultInjectCmd() : BasicCommand("eloqFaultInject") {}

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kAlways;
    }

    bool supportsWriteConcern(const BSONObj& cmd) const override {
        return false;
    }

    bool adminOnly() const override {
        return true;
    }

    bool requiresAuth() const override {
        return false;
    }

    /** No privileges: the command exists only when test commands are enabled. */
    void addRequiredPrivileges(const std::string& dbname,
                               const BSONObj& cmdObj,
                               std::vector<Privilege>* out) const override {}

    std::string help() const override {
        return "arms, disarms or queries a data substrate fault point: "
               "{eloqFaultInject: <name>, params: <string>, nodes: [<node id>]} or "
               "{eloqFaultInject: <name>, query: true}";
    }

    bool run(OperationContext* opCtx,
             const std::string& dbname,
             const BSONObj& cmdObj,
             BSONObjBuilder& result) override {
        uassert(ErrorCodes::CommandNotSupported,
                "eloqFaultInject: this build has no fault points (the data substrate was built "
                "without WITH_FAULT_INJECT), so no fault can fire",
                txservice::FaultInject::CompiledIn());

        const BSONElement nameElem = cmdObj.firstElement();
        uassert(ErrorCodes::BadValue,
                "eloqFaultInject takes the fault name as a string",
                nameElem.type() == String && !nameElem.str().empty());

        if (cmdObj["query"].trueValue()) {
            uassert(ErrorCodes::BadValue,
                    "eloqFaultInject query reads the local node only; drop params and nodes",
                    cmdObj["params"].eoo() && cmdObj["nodes"].eoo());
            result.append("armed", txservice::FaultInject::Entry(nameElem.str()) != nullptr);
            return true;
        }

        std::string params;
        if (const BSONElement paramsElem = cmdObj["params"]; !paramsElem.eoo()) {
            uassert(ErrorCodes::BadValue,
                    "eloqFaultInject params must be a string",
                    paramsElem.type() == String);
            params = paramsElem.str();
        }

        std::vector<int> nodes;
        if (const BSONElement nodesElem = cmdObj["nodes"]; !nodesElem.eoo()) {
            uassert(ErrorCodes::BadValue,
                    "eloqFaultInject nodes must be an array of node ids",
                    nodesElem.type() == Array);
            for (const BSONElement& node : nodesElem.Array()) {
                uassert(ErrorCodes::BadValue,
                        "eloqFaultInject nodes must be an array of node ids",
                        node.isNumber());
                nodes.push_back(node.numberInt());
            }
        }

        log() << "eloqFaultInject name=" << nameElem.str() << " params=" << params;

        // The request travels through this operation's transaction, which the recovery unit ends
        // with the operation.
        txservice::TransactionExecution* txm = EloqRecoveryUnit::get(opCtx)->getTxm();
        const CoroutineFunctors& coro = Client::getCurrent()->coroutineFunctors();
        txservice::FaultInjectTxRequest req(
            nameElem.str(), params, nodes, coro.yieldFuncPtr, coro.resumeFuncPtr, txm);
        txm->Execute(&req);
        req.Wait();
        uassert(ErrorCodes::InternalError,
                str::stream() << "eloqFaultInject " << nameElem.str()
                              << " failed: " << req.ErrorMsg(),
                !req.IsError());
        return true;
    }
};

MONGO_REGISTER_TEST_COMMAND(EloqFaultInjectCmd);

}  // namespace
}  // namespace mongo

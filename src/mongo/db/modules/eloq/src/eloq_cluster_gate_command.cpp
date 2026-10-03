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
 * The admin command eloqClusterGate, which pauses and resumes schema changes cluster-wide while
 * change data capture bootstrap copies the catalog (docs/cdc-change-stream.md §6.2; plan D8).
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include <string>
#include <vector>

#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/privilege.h"
#include "mongo/db/auth/resource_pattern.h"
#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/db/local_thread_state.h"
#include "mongo/db/operation_context.h"
#include "mongo/util/log.h"

#include "mongo/db/modules/eloq/data_substrate/core/include/data_substrate.h"
#include "mongo/db/modules/eloq/data_substrate/tx_service/include/cluster_config_record.h"
#include "mongo/db/modules/eloq/data_substrate/tx_service/include/sharder.h"
#include "mongo/db/modules/eloq/data_substrate/tx_service/include/tx_request.h"
#include "mongo/db/modules/eloq/data_substrate/tx_service/include/tx_service.h"

namespace mongo {
namespace {

/**
 * @brief Pauses or resumes the cluster gate, which keeps new create, drop, index and collMod
 * operations out while change data capture bootstrap copies the catalog.
 *
 * Form: { eloqClusterGate: { op: "pause" | "resume", mode: "ddl" } }; mode is optional and "ddl"
 * the only one implemented ("ddl_dml" is reserved for snapshots). A pause replies
 * { pauseTs: <long> }, its commit timestamp, above every DDL committed before the gate closed; a
 * resume replies { resumeTs: <long> }. A pause of a paused gate fails with
 * ConflictingOperationInProgress: another bootstrap holds it, or an abandoned one left it. While
 * the gate is paused, schema changes fail with ClusterGatePaused, which clients retry.
 *
 * Each call is its own short transaction in the data substrate, initialized for log group 0, where
 * every gate log entry must land; the operation's own transaction is not used.
 *
 * @note It needs the eloqClusterGate action on the cluster resource, which clusterManager (and so
 * clusterAdmin and root) holds. No pause ends on its own: an operator alert on the pause's age is
 * what surfaces a forgotten one (docs/cdc-operations.md).
 */
class EloqClusterGateCmd : public BasicCommand {
public:
    EloqClusterGateCmd() : BasicCommand("eloqClusterGate") {}

    std::string help() const override {
        return "pauses or resumes schema changes cluster-wide for change data capture bootstrap: "
               "{eloqClusterGate: {op: \"pause\" | \"resume\", mode: \"ddl\"}}";
    }

    /** Every node reaches the whole cluster's gate through the data substrate. */
    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kAlways;
    }

    bool supportsWriteConcern(const BSONObj& cmd) const override {
        return false;
    }

    bool adminOnly() const override {
        return true;
    }

    void addRequiredPrivileges(const std::string& dbname,
                               const BSONObj& cmdObj,
                               std::vector<Privilege>* out) const override {
        out->push_back(
            Privilege(ResourcePattern::forClusterResource(), ActionType::eloqClusterGate));
    }

    bool run(OperationContext* opCtx,
             const std::string& dbname,
             const BSONObj& cmdObj,
             BSONObjBuilder& result) override {
        uassert(ErrorCodes::CommandNotSupported,
                "eloqClusterGate: this deployment has no log service to keep the gate in",
                DataSubstrate::Instance().GetCoreConfig().enable_wal &&
                    txservice::Sharder::Instance().GetLogAgent() != nullptr);

        const BSONElement specElem = cmdObj.firstElement();
        uassert(ErrorCodes::BadValue,
                "eloqClusterGate takes {op: \"pause\" | \"resume\", mode: \"ddl\"}",
                specElem.type() == Object);
        const BSONObj spec = specElem.Obj();
        for (const BSONElement& field : spec) {
            uassert(ErrorCodes::BadValue,
                    str::stream() << "eloqClusterGate: unknown field " << field.fieldName(),
                    field.fieldNameStringData() == "op" || field.fieldNameStringData() == "mode");
        }

        const BSONElement opElem = spec["op"];
        uassert(ErrorCodes::BadValue,
                "eloqClusterGate op must be \"pause\" or \"resume\"",
                opElem.type() == String && (opElem.str() == "pause" || opElem.str() == "resume"));
        const bool pause = opElem.str() == "pause";

        if (const BSONElement modeElem = spec["mode"]; !modeElem.eoo()) {
            uassert(ErrorCodes::BadValue,
                    "eloqClusterGate mode must be a string",
                    modeElem.type() == String);
            uassert(ErrorCodes::CommandNotSupported,
                    "eloqClusterGate mode \"ddl_dml\" is reserved for snapshots and not "
                    "implemented",
                    modeElem.str() != "ddl_dml");
            uassert(ErrorCodes::BadValue,
                    "eloqClusterGate mode must be \"ddl\"",
                    modeElem.str() == "ddl");
        }

        log() << "eloqClusterGate op=" << opElem.str();

        // A transaction of its own, pinned to log group 0: its number maps there, so its log
        // entry does too, which keeps group 0's slot the gate's only history.
        txservice::TxService* txService = DataSubstrate::Instance().GetTxService();
        const int16_t group = LocalThread::ID();
        txservice::TransactionExecution* txm =
            group >= 0 ? txService->NewTx(group) : txService->NewTx();
        const CoroutineFunctors& coro = Client::getCurrent()->coroutineFunctors();
        txservice::InitTxRequest initReq(txservice::IsolationLevel::RepeatableRead,
                                         txservice::CcProtocol::Locking,
                                         coro.yieldFuncPtr,
                                         coro.resumeFuncPtr,
                                         txm,
                                         UINT32_MAX,
                                         0);
        txm->Execute(&initReq);
        initReq.Wait();
        uassert(ErrorCodes::InternalError,
                str::stream() << "eloqClusterGate: starting its transaction failed: "
                              << initReq.ErrorMsg(),
                !initReq.IsError());

        txservice::ClusterGateTxRequest gateReq(pause ? txservice::ClusterGateOpType::kPause
                                                      : txservice::ClusterGateOpType::kResume,
                                                txservice::ClusterGate::Mode::kDdl,
                                                coro.yieldFuncPtr,
                                                coro.resumeFuncPtr,
                                                txm);
        txm->Execute(&gateReq);
        gateReq.Wait();
        // The op has committed or aborted the transaction on its own; txm is not ours any more.
        if (gateReq.IsError()) {
            uassert(ErrorCodes::ConflictingOperationInProgress,
                    "eloqClusterGate: the gate is already paused, by another bootstrap or an "
                    "abandoned one; resume it before pausing again",
                    gateReq.ErrorCode() != txservice::TxErrorCode::CLUSTER_GATE_HELD);
            uasserted(ErrorCodes::InternalError,
                      str::stream() << "eloqClusterGate " << opElem.str()
                                    << " failed: " << gateReq.ErrorMsg());
        }
        result.append(pause ? "pauseTs" : "resumeTs", static_cast<long long>(gateReq.Result()));
        return true;
    }
} eloqClusterGateCmd;

}  // namespace
}  // namespace mongo

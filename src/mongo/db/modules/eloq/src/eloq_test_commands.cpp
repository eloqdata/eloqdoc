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
 * (--setParameter enableTestCommands=1): fault injection, and views of the catalog and of the log
 * service that tests compare.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include <future>
#include <optional>
#include <string>
#include <vector>

#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/db/commands/test_commands_enabled.h"
#include "mongo/db/operation_context.h"
#include "mongo/util/log.h"

#include "mongo/db/modules/eloq/src/base/eloq_util.h"
#include "mongo/db/modules/eloq/src/eloq_recovery_unit.h"

#include "mongo/db/modules/eloq/data_substrate/core/include/data_substrate.h"
#include "mongo/db/modules/eloq/data_substrate/tx_service/include/catalog_key_record.h"
#include "mongo/db/modules/eloq/data_substrate/tx_service/include/fault/fault_inject.h"
#include "mongo/db/modules/eloq/data_substrate/tx_service/include/sharder.h"
#include "mongo/db/modules/eloq/data_substrate/tx_service/include/tx_request.h"
#include "mongo/db/modules/eloq/data_substrate/tx_service/include/txlog.h"

namespace mongo {
namespace {

/**
 * @brief What the Eloq test commands share: admin only, no write concern, no privileges, allowed
 * on any node.
 */
class EloqTestCommand : public BasicCommand {
public:
    using BasicCommand::BasicCommand;

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
};

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
class EloqFaultInjectCmd : public EloqTestCommand {
public:
    EloqFaultInjectCmd() : EloqTestCommand("eloqFaultInject") {}

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

/**
 * @brief Replies with the version of a collection's committed schema as the local catalog holds
 * it, so that a test can compare it with the timestamp the DDL was logged at.
 *
 * Form: { eloqCatalogVersion: "<db>.<collection>" }. Replies { exists: <bool>, schemaTs: <long> };
 * schemaTs is 0 when the collection does not exist.
 */
class EloqCatalogVersionCmd : public EloqTestCommand {
public:
    EloqCatalogVersionCmd() : EloqTestCommand("eloqCatalogVersion") {}

    std::string help() const override {
        return "replies with a collection's committed schema version: "
               "{eloqCatalogVersion: \"<db>.<collection>\"}";
    }

    bool run(OperationContext* opCtx,
             const std::string& dbname,
             const BSONObj& cmdObj,
             BSONObjBuilder& result) override {
        const BSONElement nsElem = cmdObj.firstElement();
        uassert(ErrorCodes::BadValue,
                "eloqCatalogVersion takes the namespace as a string",
                nsElem.type() == String && !nsElem.str().empty());
        const std::string ns = nsElem.str();
        txservice::CatalogKey catalogKey{Eloq::MongoTableToTxServiceTableName(ns, false)};
        txservice::CatalogRecord catalogRecord;
        auto [exists, err] =
            EloqRecoveryUnit::get(opCtx)->readCatalog(catalogKey, catalogRecord, false);
        uassert(ErrorCodes::InternalError,
                str::stream() << "eloqCatalogVersion: reading the catalog of " << ns
                              << " failed with tx error " << static_cast<int>(err),
                err == txservice::TxErrorCode::NO_ERROR);
        result.append("exists", exists);
        result.append("schemaTs", static_cast<long long>(exists ? catalogRecord.SchemaTs() : 0));
        return true;
    }
};

MONGO_REGISTER_TEST_COMMAND(EloqCatalogVersionCmd);

/**
 * @brief The closure of a call its caller waits for.
 */
class CallDone : public google::protobuf::Closure {
public:
    /** @brief Marks the call ended; brpc no longer touches it afterwards. */
    void Run() override {
        _ended.set_value();
    }

    /** @brief Waits until Run() has been called. */
    void Wait() {
        _endedFuture.wait();
    }

private:
    std::promise<void> _ended;                             ///< Set once, in Run().
    std::future<void> _endedFuture = _ended.get_future();  ///< What Wait() waits on.
};

/**
 * @brief Sends one CdcScan to a log group's leader and replies with the page, so that a test can
 * see what the log service holds, the DDL history above all.
 *
 * Form: { eloqCdcScan: <log group id>, fromTs: <long>, continuation: <BinData>, maxBytes: <int> },
 * with at most one of fromTs and continuation: neither scans from the beginning of complete
 * history. A group id that is not one of the cluster's log groups fails with BadValue. Replies,
 * when the call went through:
 * { status: <CdcScanResponse.Status name>, redirect: <string>, arrivalCompleteTs: <long>,
 *   hasMore: <bool>, continuation: <BinData>,
 *   data: [{commitTs, ngId, txn}],
 *   ddl: [{ddlTs, catalogTs, txn, table, opType, stage, rolledBack}] };
 * otherwise { rpcError: <string> }. Either is worth retrying: the call goes to the leader the log
 * agent has cached, and a node that has just started may not lead or serve yet.
 */
class EloqCdcScanCmd : public EloqTestCommand {
public:
    EloqCdcScanCmd() : EloqTestCommand("eloqCdcScan") {}

    std::string help() const override {
        return "sends one CdcScan to a log group: {eloqCdcScan: <log group id>, fromTs: <long>, "
               "continuation: <BinData>, maxBytes: <int>}";
    }

    bool run(OperationContext* opCtx,
             const std::string& dbname,
             const BSONObj& cmdObj,
             BSONObjBuilder& result) override {
        txservice::TxLog* log = txservice::Sharder::Instance().GetLogAgent();
        uassert(ErrorCodes::CommandNotSupported,
                "eloqCdcScan: this deployment has no log service to scan",
                DataSubstrate::Instance().GetCoreConfig().enable_wal && log != nullptr &&
                    log->LogGroupCount() > 0);

        const BSONElement groupElem = cmdObj.firstElement();
        uassert(ErrorCodes::BadValue,
                "eloqCdcScan takes the log group id as a number",
                groupElem.isNumber());
        const long long groupId = groupElem.safeNumberLong();
        uassert(ErrorCodes::BadValue,
                str::stream() << "eloqCdcScan: log group " << groupId
                              << " does not exist; there are " << log->LogGroupCount(),
                groupId >= 0 && groupId < static_cast<long long>(log->LogGroupCount()));
        txlog::CdcScanRequest request;
        request.set_log_group_id(static_cast<uint32_t>(groupId));
        const BSONElement fromElem = cmdObj["fromTs"];
        const BSONElement continuationElem = cmdObj["continuation"];
        uassert(ErrorCodes::BadValue,
                "eloqCdcScan takes at most one of fromTs and continuation",
                fromElem.eoo() || continuationElem.eoo());
        if (!fromElem.eoo()) {
            uassert(
                ErrorCodes::BadValue, "eloqCdcScan fromTs must be a number", fromElem.isNumber());
            request.set_from_ts(static_cast<uint64_t>(fromElem.safeNumberLong()));
        }
        if (!continuationElem.eoo()) {
            uassert(ErrorCodes::BadValue,
                    "eloqCdcScan continuation must be BinData",
                    continuationElem.type() == BinData);
            int len = 0;
            const char* bytes = continuationElem.binData(len);
            request.set_continuation(bytes, len);
        }
        if (const BSONElement maxBytesElem = cmdObj["maxBytes"]; !maxBytesElem.eoo()) {
            uassert(ErrorCodes::BadValue,
                    "eloqCdcScan maxBytes must be a number",
                    maxBytesElem.isNumber());
            request.set_max_bytes(static_cast<uint32_t>(maxBytesElem.numberInt()));
        }

        brpc::Controller cntl;
        cntl.set_timeout_ms(10000);
        txlog::CdcScanResponse response;
        std::optional<uint32_t> node;
        // The log agent runs the closure exactly once: when the call ends, or before CdcScan
        // returns if it fails without a call, e.g. with no channel to the group's leader. A
        // brpc::Join would wait forever then, on a call id no call ever used.
        CallDone done;
        log->CdcScan(request.log_group_id(), &cntl, request, response, &node, done);
        done.Wait();
        if (cntl.Failed()) {
            result.append("rpcError", cntl.ErrorText());
            return true;
        }

        result.append("status", txlog::CdcScanResponse_Status_Name(response.status()));
        result.append("redirect", response.redirect());
        result.append("arrivalCompleteTs", static_cast<long long>(response.arrival_complete_ts()));
        result.append("hasMore", response.has_more());
        result.appendBinData("continuation",
                             static_cast<int>(response.continuation().size()),
                             BinDataGeneral,
                             response.continuation().data());
        BSONArrayBuilder data(result.subarrayStart("data"));
        for (const txlog::CdcDataRecord& record : response.data()) {
            data.append(BSON("commitTs" << static_cast<long long>(record.commit_ts()) << "ngId"
                                        << static_cast<int>(record.cc_ng_id()) << "txn"
                                        << static_cast<long long>(record.txn_number())));
        }
        data.done();
        BSONArrayBuilder ddl(result.subarrayStart("ddl"));
        for (const txlog::CdcDdlRecord& record : response.ddl()) {
            const txlog::SchemaOpMessage& op = record.schema_op();
            ddl.append(BSON("ddlTs" << static_cast<long long>(record.ddl_ts()) << "catalogTs"
                                    << static_cast<long long>(record.catalog_ts()) << "txn"
                                    << static_cast<long long>(record.txn_number()) << "table"
                                    << op.table_name_str() << "opType"
                                    << static_cast<int>(op.table_op().op_type()) << "stage"
                                    << txlog::SchemaOpMessage_Stage_Name(op.stage()) << "rolledBack"
                                    << op.rolled_back()));
        }
        ddl.done();
        return true;
    }
};

MONGO_REGISTER_TEST_COMMAND(EloqCdcScanCmd);

}  // namespace
}  // namespace mongo

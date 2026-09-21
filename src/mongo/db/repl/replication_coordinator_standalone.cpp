/**
 *    Copyright (C) 2018 MongoDB Inc.
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
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kReplication

#include "mongo/platform/basic.h"

#include "mongo/db/repl/replication_coordinator_standalone.h"

#include "mongo/db/global_settings.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/repl/read_concern_args.h"
#include "mongo/db/repl/repl_set_config.h"
#include "mongo/db/storage/storage_engine.h"
#include "mongo/util/assert_util.h"

namespace mongo {
namespace repl {

namespace {

// Only replication-only APIs without a Status result use this helper. APIs used by
// ordinary standalone commands must preserve their non-throwing Status contract.
MONGO_COMPILER_NORETURN void replicationUnavailable() {
    uasserted(ErrorCodes::NoReplicationEnabled,
              "MongoDB replication is not available in this standalone build");
}

}  // namespace

ReplicationCoordinatorStandalone::ReplicationCoordinatorStandalone(ServiceContext* service)
    : _service(service) {}

ReplicationCoordinatorStandalone::~ReplicationCoordinatorStandalone() = default;

void ReplicationCoordinatorStandalone::startup(OperationContext* opCtx) {}

void ReplicationCoordinatorStandalone::shutdown(OperationContext* opCtx) {}

const ReplSettings& ReplicationCoordinatorStandalone::getSettings() const {
    return getGlobalReplSettings();
}

ReplicationCoordinator::Mode ReplicationCoordinatorStandalone::getReplicationMode() const {
    return ReplicationCoordinator::Mode::modeNone;
}

bool ReplicationCoordinatorStandalone::isMasterForReportingPurposes() {
    return true;
}

bool ReplicationCoordinatorStandalone::canAcceptWritesForDatabase(OperationContext* opCtx,
                                                                  StringData dbName) {
    return true;
}

bool ReplicationCoordinatorStandalone::canAcceptWritesForDatabase_UNSAFE(OperationContext* opCtx,
                                                                         StringData dbName) {
    return true;
}

bool ReplicationCoordinatorStandalone::canAcceptWritesFor(OperationContext* opCtx,
                                                          const NamespaceString& ns) {
    return true;
}

bool ReplicationCoordinatorStandalone::canAcceptWritesFor_UNSAFE(OperationContext* opCtx,
                                                                 const NamespaceString& ns) {
    return true;
}

Status ReplicationCoordinatorStandalone::checkCanServeReadsFor(OperationContext* opCtx,
                                                               const NamespaceString& ns,
                                                               bool slaveOk) {
    return Status::OK();
}

Status ReplicationCoordinatorStandalone::checkCanServeReadsFor_UNSAFE(OperationContext* opCtx,
                                                                      const NamespaceString& ns,
                                                                      bool slaveOk) {
    return Status::OK();
}

bool ReplicationCoordinatorStandalone::isInPrimaryOrSecondaryState() const {
    return false;
}

bool ReplicationCoordinatorStandalone::shouldRelaxIndexConstraints(OperationContext* opCtx,
                                                                   const NamespaceString& ns) {
    return false;
}

bool ReplicationCoordinatorStandalone::getMaintenanceMode() {
    return false;
}

WriteConcernOptions ReplicationCoordinatorStandalone::getGetLastErrorDefault() {
    return WriteConcernOptions();
}

bool ReplicationCoordinatorStandalone::isReplEnabled() const {
    return false;
}

WriteConcernOptions ReplicationCoordinatorStandalone::populateUnsetWriteConcernOptionsSyncMode(
    WriteConcernOptions wc) {
    WriteConcernOptions writeConcern(wc);
    if (writeConcern.syncMode == WriteConcernOptions::SyncMode::UNSET) {
        writeConcern.syncMode = WriteConcernOptions::SyncMode::NONE;
    }
    return writeConcern;
}

bool ReplicationCoordinatorStandalone::buildsIndexes() {
    return true;
}

OpTime ReplicationCoordinatorStandalone::getCurrentCommittedSnapshotOpTime() const {
    return OpTime();
}

void ReplicationCoordinatorStandalone::appendDiagnosticBSON(mongo::BSONObjBuilder*) {}

void ReplicationCoordinatorStandalone::appendConnectionStats(
    executor::ConnectionPoolStats* stats) const {}

MemberState ReplicationCoordinatorStandalone::getMemberState() const {
    return MemberState::RS_STARTUP;
}

Status ReplicationCoordinatorStandalone::waitForMemberState(MemberState, Milliseconds) {
    return {ErrorCodes::NoReplicationEnabled,
            "MongoDB replication is not available in this standalone build"};
}

Seconds ReplicationCoordinatorStandalone::getSlaveDelaySecs() const {
    return Seconds(0);
}

void ReplicationCoordinatorStandalone::clearSyncSourceBlacklist() {}

Status ReplicationCoordinatorStandalone::setFollowerMode(const MemberState&) {
    return {ErrorCodes::NoReplicationEnabled,
            "MongoDB replication is not available in this standalone build"};
}

ReplicationCoordinator::ApplierState ReplicationCoordinatorStandalone::getApplierState() {
    replicationUnavailable();
}

void ReplicationCoordinatorStandalone::signalDrainComplete(OperationContext*, long long) {
    replicationUnavailable();
}

Status ReplicationCoordinatorStandalone::waitForDrainFinish(Milliseconds) {
    return {ErrorCodes::NoReplicationEnabled,
            "MongoDB replication is not available in this standalone build"};
}

void ReplicationCoordinatorStandalone::signalUpstreamUpdater() {
    replicationUnavailable();
}

void ReplicationCoordinatorStandalone::setMyHeartbeatMessage(const std::string&) {
    replicationUnavailable();
}

void ReplicationCoordinatorStandalone::setMyLastAppliedOpTimeForward(const OpTime&,
                                                                     DataConsistency) {
    replicationUnavailable();
}

void ReplicationCoordinatorStandalone::setMyLastDurableOpTimeForward(const OpTime&) {
    replicationUnavailable();
}

void ReplicationCoordinatorStandalone::setMyLastAppliedOpTime(const OpTime&) {
    replicationUnavailable();
}

void ReplicationCoordinatorStandalone::setMyLastDurableOpTime(const OpTime&) {
    replicationUnavailable();
}

void ReplicationCoordinatorStandalone::resetMyLastOpTimes() {
    replicationUnavailable();
}

OpTime ReplicationCoordinatorStandalone::getMyLastAppliedOpTime() const {
    return OpTime();
}

OpTime ReplicationCoordinatorStandalone::getMyLastDurableOpTime() const {
    return OpTime();
}

Status ReplicationCoordinatorStandalone::waitUntilOpTimeForRead(
    OperationContext* opCtx, const ReadConcernArgs& readConcern) {
    // Preserve the normal modeNone validation; storage capabilities belong to the engine.
    const auto level = readConcern.getLevel();
    if (readConcern.getArgsAfterClusterTime() && level != ReadConcernLevel::kMajorityReadConcern &&
        level != ReadConcernLevel::kLocalReadConcern &&
        level != ReadConcernLevel::kSnapshotReadConcern) {
        return {ErrorCodes::BadValue,
                "Only readConcern level 'majority', 'local', or 'snapshot' is allowed when "
                "specifying afterClusterTime"};
    }
    if (readConcern.getArgsAtClusterTime() && level != ReadConcernLevel::kSnapshotReadConcern) {
        return {ErrorCodes::BadValue,
                "readConcern level 'snapshot' is required when specifying atClusterTime"};
    }
    if (level == ReadConcernLevel::kMajorityReadConcern &&
        !opCtx->getServiceContext()->getStorageEngine()->supportsReadConcernMajority()) {
        return {ErrorCodes::ReadConcernMajorityNotEnabled,
                "Storage engine does not support majority read concern"};
    }
    if (level == ReadConcernLevel::kSnapshotReadConcern &&
        !opCtx->getServiceContext()->getStorageEngine()->supportsReadConcernSnapshot()) {
        return {ErrorCodes::InvalidOptions,
                "Storage engine does not support snapshot read concern"};
    }
    if (!readConcern.getArgsAfterClusterTime() && !readConcern.getArgsOpTime() &&
        !readConcern.getArgsAtClusterTime()) {
        return Status::OK();
    }
    return waitUntilOpTimeForReadUntil(opCtx, readConcern, boost::none);
}

Status ReplicationCoordinatorStandalone::waitUntilOpTimeForReadUntil(OperationContext*,
                                                                     const ReadConcernArgs&,
                                                                     boost::optional<Date_t>) {
    return {ErrorCodes::NotAReplicaSet,
            "node needs to be a replica set member to use timestamped read concern"};
}

ReplicationCoordinator::StatusAndDuration ReplicationCoordinatorStandalone::awaitReplication(
    OperationContext*, const OpTime&, const WriteConcernOptions&) {
    return {Status::OK(), Milliseconds(0)};
}

Status ReplicationCoordinatorStandalone::stepDown(OperationContext*,
                                                  const bool,
                                                  const Milliseconds&,
                                                  const Milliseconds&) {
    // The normal shutdown command always tries stepDown and explicitly accepts NotMaster.
    return {ErrorCodes::NotMaster, "not running as a replica-set primary"};
}

OID ReplicationCoordinatorStandalone::getElectionId() {
    return OID();
}

int ReplicationCoordinatorStandalone::getMyId() const {
    return -1;
}

Status ReplicationCoordinatorStandalone::resyncData(OperationContext*, bool) {
    return {ErrorCodes::NoReplicationEnabled,
            "MongoDB replication is not available in this standalone build"};
}

StatusWith<BSONObj> ReplicationCoordinatorStandalone::prepareReplSetUpdatePositionCommand() const {
    return {ErrorCodes::NoReplicationEnabled,
            "MongoDB replication is not available in this standalone build"};
}

Status ReplicationCoordinatorStandalone::processReplSetGetStatus(BSONObjBuilder*,
                                                                 ReplSetGetStatusResponseStyle) {
    return {ErrorCodes::NoReplicationEnabled,
            "MongoDB replication is not available in this standalone build"};
}

void ReplicationCoordinatorStandalone::fillIsMasterForReplSet(IsMasterResponse*) {
    replicationUnavailable();
}

void ReplicationCoordinatorStandalone::appendSlaveInfoData(BSONObjBuilder*) {}

ReplSetConfig ReplicationCoordinatorStandalone::getConfig() const {
    replicationUnavailable();
}

void ReplicationCoordinatorStandalone::processReplSetGetConfig(BSONObjBuilder*) {
    replicationUnavailable();
}

void ReplicationCoordinatorStandalone::processReplSetMetadata(const rpc::ReplSetMetadata&) {}

void ReplicationCoordinatorStandalone::cancelAndRescheduleElectionTimeout() {}

Status ReplicationCoordinatorStandalone::setMaintenanceMode(bool) {
    // MaintenanceModeSetter tolerates a failed Status, but not an exception.
    return {ErrorCodes::NoReplicationEnabled,
            "can only set maintenance mode on replica set members"};
}

Status ReplicationCoordinatorStandalone::processReplSetSyncFrom(OperationContext*,
                                                                const HostAndPort&,
                                                                BSONObjBuilder*) {
    return {ErrorCodes::NoReplicationEnabled,
            "MongoDB replication is not available in this standalone build"};
}

Status ReplicationCoordinatorStandalone::processReplSetFreeze(int, BSONObjBuilder*) {
    return {ErrorCodes::NoReplicationEnabled,
            "MongoDB replication is not available in this standalone build"};
}

Status ReplicationCoordinatorStandalone::processHeartbeat(const ReplSetHeartbeatArgs&,
                                                          ReplSetHeartbeatResponse*) {
    return {ErrorCodes::NoReplicationEnabled,
            "MongoDB replication is not available in this standalone build"};
}

Status ReplicationCoordinatorStandalone::processReplSetReconfig(OperationContext*,
                                                                const ReplSetReconfigArgs&,
                                                                BSONObjBuilder*) {
    return {ErrorCodes::NoReplicationEnabled,
            "MongoDB replication is not available in this standalone build"};
}

Status ReplicationCoordinatorStandalone::processReplSetInitiate(OperationContext*,
                                                                const BSONObj&,
                                                                BSONObjBuilder*) {
    return {ErrorCodes::NoReplicationEnabled,
            "MongoDB replication is not available in this standalone build"};
}

Status ReplicationCoordinatorStandalone::abortCatchupIfNeeded() {
    return {ErrorCodes::NoReplicationEnabled,
            "MongoDB replication is not available in this standalone build"};
}

Status ReplicationCoordinatorStandalone::processReplSetFresh(const ReplSetFreshArgs&,
                                                             BSONObjBuilder*) {
    return {ErrorCodes::NoReplicationEnabled,
            "MongoDB replication is not available in this standalone build"};
}

Status ReplicationCoordinatorStandalone::processReplSetElect(const ReplSetElectArgs&,
                                                             BSONObjBuilder*) {
    return {ErrorCodes::NoReplicationEnabled,
            "MongoDB replication is not available in this standalone build"};
}

Status ReplicationCoordinatorStandalone::processReplSetUpdatePosition(const UpdatePositionArgs&,
                                                                      long long*) {
    return {ErrorCodes::NoReplicationEnabled,
            "MongoDB replication is not available in this standalone build"};
}

std::vector<HostAndPort> ReplicationCoordinatorStandalone::getHostsWrittenTo(const OpTime&, bool) {
    return {};
}

std::vector<HostAndPort> ReplicationCoordinatorStandalone::getOtherNodesInReplSet() const {
    return {};
}

Status ReplicationCoordinatorStandalone::checkIfWriteConcernCanBeSatisfied(
    const WriteConcernOptions&) const {
    return {ErrorCodes::NoReplicationEnabled, "not running with --replSet"};
}

Status ReplicationCoordinatorStandalone::checkReplEnabledForCommand(BSONObjBuilder*) {
    return Status(ErrorCodes::NoReplicationEnabled, "not running with --replSet");
}

HostAndPort ReplicationCoordinatorStandalone::chooseNewSyncSource(const OpTime&) {
    replicationUnavailable();
}

void ReplicationCoordinatorStandalone::blacklistSyncSource(const HostAndPort&, Date_t) {
    replicationUnavailable();
}

void ReplicationCoordinatorStandalone::resetLastOpTimesFromOplog(OperationContext*,
                                                                 DataConsistency) {
    replicationUnavailable();
}

bool ReplicationCoordinatorStandalone::shouldChangeSyncSource(
    const HostAndPort&, const rpc::ReplSetMetadata&, boost::optional<rpc::OplogQueryMetadata>) {
    replicationUnavailable();
}

void ReplicationCoordinatorStandalone::advanceCommitPoint(const OpTime&) {
    replicationUnavailable();
}

OpTime ReplicationCoordinatorStandalone::getLastCommittedOpTime() const {
    return OpTime();
}

Status ReplicationCoordinatorStandalone::processReplSetRequestVotes(OperationContext*,
                                                                    const ReplSetRequestVotesArgs&,
                                                                    ReplSetRequestVotesResponse*) {
    return {ErrorCodes::NoReplicationEnabled,
            "MongoDB replication is not available in this standalone build"};
}

void ReplicationCoordinatorStandalone::prepareReplMetadata(const BSONObj&,
                                                           const OpTime&,
                                                           BSONObjBuilder*) const {}

bool ReplicationCoordinatorStandalone::isV1ElectionProtocol() const {
    return false;
}

bool ReplicationCoordinatorStandalone::getWriteConcernMajorityShouldJournal() {
    return false;
}

Status ReplicationCoordinatorStandalone::processHeartbeatV1(const ReplSetHeartbeatArgsV1&,
                                                            ReplSetHeartbeatResponse*) {
    return {ErrorCodes::NoReplicationEnabled,
            "MongoDB replication is not available in this standalone build"};
}

void ReplicationCoordinatorStandalone::summarizeAsHtml(ReplSetHtmlSummary*) {
    replicationUnavailable();
}

long long ReplicationCoordinatorStandalone::getTerm() {
    return OpTime::kUninitializedTerm;
}

Status ReplicationCoordinatorStandalone::updateTerm(OperationContext*, long long) {
    return {ErrorCodes::BadValue, "cannot supply 'term' without active replication"};
}

void ReplicationCoordinatorStandalone::waitUntilSnapshotCommitted(OperationContext*,
                                                                  const Timestamp&) {
    replicationUnavailable();
}

size_t ReplicationCoordinatorStandalone::getNumUncommittedSnapshots() {
    return 0;
}

void ReplicationCoordinatorStandalone::dropAllSnapshots() {}

Status ReplicationCoordinatorStandalone::stepUpIfEligible(bool skipDryRun) {
    return {ErrorCodes::NoReplicationEnabled,
            "MongoDB replication is not available in this standalone build"};
}

ReplSettings::IndexPrefetchConfig ReplicationCoordinatorStandalone::getIndexPrefetchConfig() const {
    return _indexPrefetchConfig.load();
}

void ReplicationCoordinatorStandalone::setIndexPrefetchConfig(
    const ReplSettings::IndexPrefetchConfig config) {
    _indexPrefetchConfig.store(config);
}

void ReplicationCoordinatorStandalone::signalDropPendingCollectionsRemovedFromStorage() {}

}  // namespace repl
}  // namespace mongo

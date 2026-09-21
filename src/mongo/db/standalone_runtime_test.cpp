#include "mongo/platform/basic.h"

#include "mongo/db/global_settings.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/repl/read_concern_args.h"
#include "mongo/db/repl/replication_coordinator_standalone.h"
#include "mongo/unittest/unittest.h"

namespace mongo {
namespace {

TEST(StandaloneRuntime, UsesGlobalReplicationSettings) {
    repl::ReplicationCoordinatorStandalone coordinator(nullptr);
    ASSERT_TRUE(&coordinator.getSettings() == &getGlobalReplSettings());
}

TEST(StandaloneRuntime, DoesNotAdvertiseMongoReplication) {
    repl::ReplicationCoordinatorStandalone coordinator(nullptr);
    ASSERT_FALSE(coordinator.isReplEnabled());
    ASSERT_EQUALS(repl::ReplicationCoordinator::modeNone, coordinator.getReplicationMode());
    ASSERT_TRUE(coordinator.isMasterForReportingPurposes());
    ASSERT_EQUALS(ErrorCodes::NoReplicationEnabled,
                  coordinator.checkReplEnabledForCommand(nullptr).code());
}

TEST(StandaloneRuntime, AcceptsLocalReadsAndWrites) {
    repl::ReplicationCoordinatorStandalone coordinator(nullptr);
    NamespaceString ns("test.collection");
    ASSERT_TRUE(coordinator.canAcceptWritesFor(nullptr, ns));
    ASSERT_OK(coordinator.checkCanServeReadsFor(nullptr, ns, false));
    ASSERT_TRUE(coordinator.buildsIndexes());
    ASSERT_FALSE(coordinator.shouldRelaxIndexConstraints(nullptr, ns));
}

TEST(StandaloneRuntime, PreservesExplicitDurabilityRequest) {
    repl::ReplicationCoordinatorStandalone coordinator(nullptr);
    WriteConcernOptions concern;
    concern.syncMode = WriteConcernOptions::SyncMode::JOURNAL;
    ASSERT_TRUE(WriteConcernOptions::SyncMode::JOURNAL ==
                coordinator.populateUnsetWriteConcernOptionsSyncMode(concern).syncMode);
    concern.syncMode = WriteConcernOptions::SyncMode::FSYNC;
    ASSERT_TRUE(WriteConcernOptions::SyncMode::FSYNC ==
                coordinator.populateUnsetWriteConcernOptionsSyncMode(concern).syncMode);
    concern.syncMode = WriteConcernOptions::SyncMode::UNSET;
    ASSERT_TRUE(WriteConcernOptions::SyncMode::NONE ==
                coordinator.populateUnsetWriteConcernOptionsSyncMode(concern).syncMode);
}

TEST(StandaloneRuntime, DoesNotWaitForNonexistentReplicas) {
    repl::ReplicationCoordinatorStandalone coordinator(nullptr);
    ASSERT_OK(coordinator.awaitReplication(nullptr, repl::OpTime(), WriteConcernOptions()).status);
    ASSERT_EQUALS(ErrorCodes::NoReplicationEnabled,
                  coordinator.checkIfWriteConcernCanBeSatisfied(WriteConcernOptions()).code());
}

TEST(StandaloneRuntime, AllowsNormalShutdownCommandToSkipStepDown) {
    repl::ReplicationCoordinatorStandalone coordinator(nullptr);
    ASSERT_EQUALS(ErrorCodes::NotMaster,
                  coordinator.stepDown(nullptr, false, Seconds(0), Seconds(120)).code());
}

TEST(StandaloneRuntime, MaintenanceModeReturnsStatusWithoutThrowing) {
    repl::ReplicationCoordinatorStandalone coordinator(nullptr);
    // ServiceEntryPointCommon's MaintenanceModeSetter deliberately ignores these errors.
    ASSERT_EQUALS(ErrorCodes::NoReplicationEnabled, coordinator.setMaintenanceMode(true).code());
    ASSERT_EQUALS(ErrorCodes::NoReplicationEnabled, coordinator.setMaintenanceMode(false).code());
    ASSERT_FALSE(coordinator.getMaintenanceMode());
}

TEST(StandaloneRuntime, TermOnNonReplicationCommandsReturnsBadValue) {
    repl::ReplicationCoordinatorStandalone coordinator(nullptr);
    // Both find and getMore validate a supplied term through this Status-returning API.
    ASSERT_EQUALS(ErrorCodes::BadValue, coordinator.updateTerm(nullptr, 1).code());
    ASSERT_EQUALS(ErrorCodes::BadValue,
                  coordinator.updateTerm(nullptr, repl::OpTime::kUninitializedTerm).code());
    ASSERT_TRUE(coordinator.getTerm() == repl::OpTime::kUninitializedTerm);
}

TEST(StandaloneRuntime, IndexPrefetchServerParameterRoundTripsWithoutReplication) {
    repl::ReplicationCoordinatorStandalone coordinator(nullptr);
    using Config = repl::ReplSettings::IndexPrefetchConfig;
    ASSERT_TRUE(coordinator.getIndexPrefetchConfig() == Config::PREFETCH_ALL);
    for (auto config : {Config::PREFETCH_NONE, Config::PREFETCH_ID_ONLY, Config::PREFETCH_ALL}) {
        coordinator.setIndexPrefetchConfig(config);
        ASSERT_TRUE(coordinator.getIndexPrefetchConfig() == config);
    }
    ASSERT_FALSE(coordinator.isReplEnabled());
}

TEST(StandaloneRuntime, ReplicationOnlyStatusAPIsReturnErrorsWithoutThrowing) {
    repl::ReplicationCoordinatorStandalone coordinator(nullptr);
    ASSERT_EQUALS(ErrorCodes::NoReplicationEnabled,
                  coordinator.waitForMemberState(repl::MemberState::RS_SECONDARY,
                                                 Milliseconds(0)).code());
    ASSERT_EQUALS(ErrorCodes::NoReplicationEnabled,
                  coordinator.setFollowerMode(repl::MemberState::RS_SECONDARY).code());
    ASSERT_EQUALS(ErrorCodes::NoReplicationEnabled,
                  coordinator.waitForDrainFinish(Milliseconds(0)).code());
    ASSERT_EQUALS(ErrorCodes::NoReplicationEnabled, coordinator.resyncData(nullptr, false).code());
    ASSERT_EQUALS(ErrorCodes::NoReplicationEnabled,
                  coordinator.prepareReplSetUpdatePositionCommand().getStatus().code());
    ASSERT_EQUALS(ErrorCodes::NoReplicationEnabled, coordinator.abortCatchupIfNeeded().code());
    ASSERT_EQUALS(ErrorCodes::NoReplicationEnabled, coordinator.stepUpIfEligible(false).code());
}

TEST(StandaloneRuntime, ReplicationOnlyVoidAPIsReportUnavailable) {
    repl::ReplicationCoordinatorStandalone coordinator(nullptr);
    ASSERT_THROWS_CODE(coordinator.signalUpstreamUpdater(),
                       DBException,
                       ErrorCodes::NoReplicationEnabled);
}

TEST(StandaloneRuntime, AcceptsUntimestampedLocalReadConcern) {
    repl::ReplicationCoordinatorStandalone coordinator(nullptr);
    ASSERT_OK(coordinator.waitUntilOpTimeForRead(nullptr, repl::ReadConcernArgs()));
    ASSERT_OK(coordinator.waitUntilOpTimeForRead(
        nullptr, repl::ReadConcernArgs(repl::ReadConcernLevel::kAvailableReadConcern)));
}

TEST(StandaloneRuntime, RejectsReplicaSetTimestampWaits) {
    repl::ReplicationCoordinatorStandalone coordinator(nullptr);
    repl::ReadConcernArgs concern(repl::OpTime(Timestamp(1, 1), 1),
                                  repl::ReadConcernLevel::kLocalReadConcern);
    ASSERT_EQUALS(ErrorCodes::NotAReplicaSet,
                  coordinator.waitUntilOpTimeForRead(nullptr, concern).code());
}

}  // namespace
}  // namespace mongo

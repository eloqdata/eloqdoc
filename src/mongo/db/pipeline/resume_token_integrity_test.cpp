/**
 *    Copyright (C) 2026-present EloqData Inc.
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
 */

/**
 * A change event's _id is its resume token, and the batch loops record the _id of every
 * delivered document as progress. These tests cover the end-of-pipeline check that fails the
 * cursor when a user stage removed or rewrote that _id (docs/cdc-change-stream.md, §5.3 step 4).
 *
 * Every case runs through a real PipelineProxyStage, because that is the single point the check
 * lives at and the only point every document passes through, stashed ones included. Simulating
 * the check in the test instead would let the two drift apart silently.
 */

#include "mongo/platform/basic.h"

#include <deque>
#include <vector>

#include "mongo/bson/bsonobj.h"
#include "mongo/db/exec/pipeline_proxy.h"
#include "mongo/db/exec/working_set.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/document.h"
#include "mongo/db/pipeline/document_source_mock.h"
#include "mongo/db/pipeline/pipeline.h"
#include "mongo/db/pipeline/pipeline_d.h"
#include "mongo/db/pipeline/post_batch_resume_token_provider.h"
#include "mongo/unittest/unittest.h"

namespace mongo {
namespace {

/**
 * Stands in for the change-stream source: emits canned events and remembers the _id of the last
 * one, exactly what the real stages report through PostBatchResumeTokenProvider.
 */
class ResumeTokenSourceMock final : public DocumentSourceMock,
                                    public PostBatchResumeTokenProvider {
public:
    ResumeTokenSourceMock(std::deque<GetNextResult> events,
                          const boost::intrusive_ptr<ExpressionContext>& expCtx)
        : DocumentSourceMock(std::move(events), expCtx) {}

    GetNextResult getNext() override {
        GetNextResult next = DocumentSourceMock::getNext();
        if (next.isAdvanced()) {
            _lastEmitted = next.getDocument()["_id"].getDocument().toBson();
        }
        return next;
    }

    BSONObj getPostBatchResumeToken() const override {
        return _lastEmitted;
    }

    BSONObj getLastEmittedResumeToken() const override {
        return _lastEmitted;
    }

private:
    BSONObj _lastEmitted;
};

BSONObj token(StringData data) {
    return BSON("_data" << data);
}

Document event(StringData data, int key) {
    return Document(BSON("_id" << token(data) << "operationType"
                               << "insert"
                               << "ns"
                               << BSON("db"
                                       << "test"
                                       << "coll"
                                       << "c")
                               << "documentKey"
                               << BSON("_id" << key)
                               << "fullDocument"
                               << BSON("_id" << key << "x" << key)));
}

class ResumeTokenIntegrityTest : public AggregationContextFixture {
protected:
    std::unique_ptr<Pipeline, PipelineDeleter> makePipeline(
        std::vector<BSONObj> userStages, std::deque<DocumentSource::GetNextResult> events) {
        auto pipeline = uassertStatusOK(Pipeline::parse(userStages, getExpCtx()));
        pipeline->addInitialSource(new ResumeTokenSourceMock(std::move(events), getExpCtx()));
        return pipeline;
    }

    /**
     * Pulls documents through a real PipelineProxyStage, which is the one place the check runs
     * and therefore the only honest way to test it: every document leaves a pipeline through
     * that stage, stashed ones included.
     */
    class Driver {
    public:
        Driver(OperationContext* opCtx, std::unique_ptr<Pipeline, PipelineDeleter> pipeline)
            : _opCtx(opCtx), _stage(opCtx, std::move(pipeline), &_ws) {}

        ~Driver() {
            // The stage owns the pipeline and must dispose it, as PlanExecutor does.
            _stage.dispose(_opCtx);
        }

        boost::optional<BSONObj> next() {
            WorkingSetID id = WorkingSet::INVALID_ID;
            const PlanStage::StageState state = _stage.work(&id);
            if (state == PlanStage::IS_EOF) {
                return boost::none;
            }
            invariant(state == PlanStage::ADVANCED);
            return _ws.get(id)->obj.value();
        }

    private:
        OperationContext* _opCtx;
        WorkingSet _ws;
        PipelineProxyStage _stage;
    };

    std::unique_ptr<Driver> drive(std::vector<BSONObj> userStages,
                                  std::deque<DocumentSource::GetNextResult> events) {
        return std::make_unique<Driver>(getExpCtx()->opCtx,
                                        makePipeline(std::move(userStages), std::move(events)));
    }
};

//
// Pass-through cases.
//

TEST_F(ResumeTokenIntegrityTest, UntouchedIdPasses) {
    auto driver = drive({}, {event("01", 1)});
    auto doc = driver->next();
    ASSERT(doc);
    ASSERT_BSONOBJ_EQ((*doc)["_id"].Obj(), token("01"));
}

TEST_F(ResumeTokenIntegrityTest, ProjectionKeepingIdPasses) {
    auto driver = drive({BSON("$project" << BSON("_id" << 1 << "ns" << 1))},
                                 {event("01", 1)});
    auto doc = driver->next();
    ASSERT(doc);
    ASSERT_BSONOBJ_EQ((*doc)["_id"].Obj(), token("01"));
    ASSERT_FALSE(doc->hasField("fullDocument"));
}

TEST_F(ResumeTokenIntegrityTest, AddFieldsElsewherePasses) {
    auto driver = drive({BSON("$addFields" << BSON("tag"
                                                   << "copied"))},
                        {event("01", 1)});
    auto doc = driver->next();
    ASSERT(doc);
    ASSERT_BSONOBJ_EQ((*doc)["_id"].Obj(), token("01"));
    ASSERT_EQ((*doc)["tag"].String(), "copied");
}

TEST_F(ResumeTokenIntegrityTest, MatchFilteringEventsChecksAgainstLastEmitted) {
    // The first event is filtered out; the delivered document derives from the second, and the
    // check must compare against the second event's token, not the first.
    auto driver = drive({BSON("$match" << BSON("documentKey._id" << 2))},
                                 {event("01", 1), event("02", 2)});
    auto doc = driver->next();
    ASSERT(doc);
    ASSERT_BSONOBJ_EQ((*doc)["_id"].Obj(), token("02"));
    ASSERT_FALSE(driver->next());
}

TEST_F(ResumeTokenIntegrityTest, PipelineWithoutTokenProviderIsNotChecked) {
    // An ordinary aggregation may do what it likes with _id.
    auto pipeline = uassertStatusOK(
        Pipeline::parse({BSON("$project" << BSON("_id" << 0 << "x" << 1))}, getExpCtx()));
    pipeline->addInitialSource(DocumentSourceMock::create(Document(BSON("_id" << 1 << "x" << 2))));
    Driver driver(getExpCtx()->opCtx, std::move(pipeline));
    auto doc = driver.next();
    ASSERT(doc);
    ASSERT_FALSE(doc->hasField("_id"));
}

//
// Removal.
//

TEST_F(ResumeTokenIntegrityTest, ProjectionRemovingIdFails) {
    auto driver = drive({BSON("$project" << BSON("_id" << 0 << "ns" << 1))},
                                 {event("01", 1)});
    ASSERT_THROWS_CODE(
        driver->next(), AssertionException, ErrorCodes::ChangeStreamFatalError);
}

TEST_F(ResumeTokenIntegrityTest, ReplaceRootDroppingIdFails) {
    auto driver = drive({BSON("$replaceRoot" << BSON("newRoot"
                                                              << "$fullDocument"))},
                                 {event("01", 1)});
    ASSERT_THROWS_CODE(
        driver->next(), AssertionException, ErrorCodes::ChangeStreamFatalError);
}

//
// Replacement.
//

TEST_F(ResumeTokenIntegrityTest, AddFieldsReplacingIdFails) {
    auto driver = drive({BSON("$addFields" << BSON("_id"
                                                            << "$documentKey"))},
                                 {event("01", 1)});
    ASSERT_THROWS_CODE(
        driver->next(), AssertionException, ErrorCodes::ChangeStreamFatalError);
}

TEST_F(ResumeTokenIntegrityTest, ComputedProjectionReplacingIdFails) {
    auto driver = drive({BSON("$project" << BSON("_id"
                                                          << "$documentKey._id"
                                                          << "ns"
                                                          << 1))},
                                 {event("01", 1)});
    ASSERT_THROWS_CODE(
        driver->next(), AssertionException, ErrorCodes::ChangeStreamFatalError);
}

TEST_F(ResumeTokenIntegrityTest, ReplacingIdWithAnotherEventsTokenFails) {
    // A structurally valid token that is not this event's is still a corruption; only a byte
    // comparison against the emitted token catches it.
    auto driver = drive({BSON("$addFields" << BSON("_id" << token("02")))},
                                 {event("01", 1)});
    ASSERT_THROWS_CODE(
        driver->next(), AssertionException, ErrorCodes::ChangeStreamFatalError);
}

//
// Nested modification.
//

TEST_F(ResumeTokenIntegrityTest, AddFieldsModifyingNestedDataFails) {
    auto driver = drive({BSON("$addFields" << BSON("_id._data"
                                                            << "ff"))},
                                 {event("01", 1)});
    ASSERT_THROWS_CODE(
        driver->next(), AssertionException, ErrorCodes::ChangeStreamFatalError);
}

TEST_F(ResumeTokenIntegrityTest, AddFieldsAddingNestedFieldFails) {
    // Even an addition inside _id changes the bytes drivers would send back as the token.
    auto driver = drive({BSON("$addFields" << BSON("_id.extra" << 1))}, {event("01", 1)});
    ASSERT_THROWS_CODE(
        driver->next(), AssertionException, ErrorCodes::ChangeStreamFatalError);
}

//
// Through the executor stage that every cursor actually runs.
//

TEST_F(ResumeTokenIntegrityTest, PipelineProxyStagePassesUntouchedId) {
    WorkingSet ws;
    PipelineProxyStage stage(
        getExpCtx()->opCtx,
        makePipeline({BSON("$project" << BSON("_id" << 1 << "ns" << 1))}, {event("01", 1)}),
        &ws);
    WorkingSetID id = WorkingSet::INVALID_ID;
    ASSERT_EQ(stage.work(&id), PlanStage::ADVANCED);
    ASSERT_BSONOBJ_EQ(ws.get(id)->obj.value()["_id"].Obj(), token("01"));
    // As PlanExecutor does: the stage owns the pipeline and must dispose it before destruction.
    stage.dispose(getExpCtx()->opCtx);
}

TEST_F(ResumeTokenIntegrityTest, PipelineProxyStageRejectsModifiedId) {
    WorkingSet ws;
    PipelineProxyStage stage(getExpCtx()->opCtx,
                             makePipeline({BSON("$addFields" << BSON("_id"
                                                                     << "$documentKey"))},
                                          {event("01", 1)}),
                             &ws);
    WorkingSetID id = WorkingSet::INVALID_ID;
    ASSERT_THROWS_CODE(stage.work(&id), AssertionException, ErrorCodes::ChangeStreamFatalError);
    stage.dispose(getExpCtx()->opCtx);
}

}  // namespace
}  // namespace mongo

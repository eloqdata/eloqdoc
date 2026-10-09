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
 * @file
 * EloqDoc's $changeStream stage: the single pipeline stage in front of the user's stages
 * (docs/cdc-change-stream.md §5.1, §5.2, §5.3; plan C1, C9).
 */

#pragma once

#include <list>
#include <memory>

#include "mongo/db/modules/eloq/src/cdc/cdc_event_stream.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/document_sources_gen.h"
#include "mongo/db/pipeline/post_batch_resume_token_provider.h"

namespace mongo {

/**
 * @brief The stage a $changeStream expands into on EloqDoc, followed only by MongoDB's
 * close-cursor stage and the user's own stages.
 *
 * It wraps a CdcEventStream, which fetches, merges, decodes and formats the events as plain C++
 * components; to the pipeline it is one source stage. Being both the first stage and the last one
 * that writes an event's _id, it answers both of PostBatchResumeTokenProvider's questions: the
 * watermark (or the opening token) and the _id it last emitted.
 *
 * Each getNext() pulls one event. The batch's deadline follows the source's contract: an awaitData
 * getMore waits until its maxTimeMS (awaitDataState), and the aggregate's first batch does not
 * wait at all, so the opening reply returns at once. At the deadline the stage reports EOF, never
 * an error, so the batch loop records the watermark.
 */
class DocumentSourceEloqChangeStream final : public DocumentSource,
                                             public PostBatchResumeTokenProvider {
public:
    /**
     * @brief Creates the stage over an opened event stream.
     *
     * @param expCtx The pipeline's expression context.
     * @param stream The stream, already opened (fresh or from a token).
     * @param spec The $changeStream spec to serialize: the client's, with the opening token as the
     * resume point, so that re-parsing the pipeline reopens at the same place without waiting.
     */
    DocumentSourceEloqChangeStream(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                   std::unique_ptr<CdcEventStream> stream,
                                   Document spec);

    /** Removes the stream from the server's registry of open streams (serverStatus). */
    ~DocumentSourceEloqChangeStream() override;

    GetNextResult getNext() final;

    const char* getSourceName() const final;

    StageConstraints constraints(Pipeline::SplitState pipeState) const final;

    Value serialize(boost::optional<ExplainOptions::Verbosity> explain = boost::none) const final;

    BSONObj getPostBatchResumeToken() const final;

    BSONObj getLastEmittedResumeToken() const final;

private:
    /**
     * @brief The deadline of the batch being built: the awaitData getMore's, or now for the
     * aggregate's first batch.
     *
     * @return The deadline.
     */
    Date_t _batchDeadline() const;

    std::unique_ptr<CdcEventStream> _stream;
    const Document _spec;
    BSONObj _lastEmitted;  ///< The _id of the last event handed to the user's stages.
    uint64_t _registryId;  ///< The stream's id in CdcStreamRegistry.
};

/**
 * @brief Builds the stages of an EloqDoc $changeStream: checks that the deployment supports
 * change streams, opens the stream and returns its stage (§5.1).
 *
 * A stream opened without a token waits inside the command, yielding, until every log group has
 * completed a checkpoint round, bounded by the command's maxTimeMS or else cdc_open_wait_ms; a
 * stream opened from a token never waits.
 *
 * @param spec The parsed $changeStream spec.
 * @param expCtx The pipeline's expression context.
 * @return The stages; createFromBson() adds the close-cursor stage.
 * @throws ChangeStreamStartNotReady if no start position could be fixed in time.
 * @throws CommandNotSupported if the deployment cannot provide change streams.
 */
std::list<boost::intrusive_ptr<DocumentSource>> makeEloqChangeStreamStages(
    const DocumentSourceChangeStreamSpec& spec,
    const boost::intrusive_ptr<ExpressionContext>& expCtx);

/**
 * @brief Makes $changeStream expand into EloqDoc's stage (makeEloqChangeStreamStages) instead of
 * the oplog-based chain; called when the storage engine is registered. Idempotent.
 */
void registerEloqChangeStreams();

}  // namespace mongo

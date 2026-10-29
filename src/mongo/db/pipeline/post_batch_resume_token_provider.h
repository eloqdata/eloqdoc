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
 * The interface through which the batch loops of aggregate and getMore learn a change stream's
 * resume position (docs/cdc-change-stream.md, §5.3 step 4 and §5.6).
 */

#pragma once

#include "mongo/bson/bsonobj.h"

namespace mongo {

/**
 * @brief Implemented by a pipeline's first stage when the stream it produces has a resumable
 * position that the client must be told about even when a batch carries no documents.
 *
 * EloqDoc's change-stream source implements it (docs/cdc-change-stream.md, §5.3 step 4 and
 * §5.6). The batch loops of aggregate and getMore never call getPostBatchResumeToken() while
 * documents are being appended; they read each appended document's own _id instead. They call
 * it in three places:
 * - once at cursor registration, when it must already be the stream's opening token (the
 *   watermark for the start position, or the token the client supplied with
 *   resumeAfter/startAfter), because the opening reply of an aggregate with batchSize 0 is built
 *   without pulling anything;
 * - when the pipeline reports IS_EOF, when it is the watermark covering everything the stage has
 *   produced so far;
 * - when the pipeline closes the cursor with CloseChangeStream after the stream's invalidate,
 *   when it is the invalidate's token, which a user stage may have filtered out of the batch:
 *   the last token delivered would restart the stream into the same invalidate.
 */
class PostBatchResumeTokenProvider {
public:
    virtual ~PostBatchResumeTokenProvider() = default;

    /**
     * @brief The token that is valid once every document produced so far has been delivered.
     *
     * @return The token; never empty once the stream is open.
     */
    virtual BSONObj getPostBatchResumeToken() const = 0;

    /**
     * @brief The _id that the stream's own stages assigned to the most recent document they
     * handed to the user's stages.
     *
     * The synthesized 'invalidate' document counts: whichever internal stage last emitted a
     * document must be reflected here. The batch loops record each delivered document's _id as
     * progress, so a user stage that rewrites _id would corrupt the resume position. Every stage
     * allowed after a change stream is streaming and yields at most one output per input, so the
     * document leaving the pipeline always derives from this most recently emitted one;
     * PipelineProxyStage compares the two _ids byte for byte and fails the cursor with
     * ChangeStreamFatalError on any difference.
     *
     * @return The _id, or an empty object if no document has been handed over yet.
     */
    virtual BSONObj getLastEmittedResumeToken() const = 0;
};

}  // namespace mongo

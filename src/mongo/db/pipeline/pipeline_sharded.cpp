/**
 * Copyright (c) 2011 10gen Inc.
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/pipeline/pipeline.h"

#include "mongo/db/pipeline/cluster_aggregation_planner.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/util/assert_util.h"

namespace mongo {

// Only builds with MongoDB sharding link this translation unit. The common Pipeline
// implementation can be linked without referencing the distributed aggregation planner.
std::unique_ptr<Pipeline, PipelineDeleter> Pipeline::splitForSharded() {
    invariant(!isSplitForShards());
    invariant(!isSplitForMerge());

    // Create and initialize the shard spec we'll return. We start with an empty pipeline on the
    // shards and all work being done in the merger. Optimizations can move operations between
    // the pipelines to be more efficient.
    std::unique_ptr<Pipeline, PipelineDeleter> shardPipeline(new Pipeline(pCtx),
                                                             PipelineDeleter(pCtx->opCtx));

    cluster_aggregation_planner::performSplitPipelineOptimizations(shardPipeline.get(), this);
    shardPipeline->_splitState = SplitState::kSplitForShards;
    _splitState = SplitState::kSplitForMerge;

    stitch();

    return shardPipeline;
}

}  // namespace mongo

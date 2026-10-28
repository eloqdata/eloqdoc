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

#include "mongo/platform/basic.h"

#include "mongo/db/modules/eloq/src/cdc/document_source_eloq_change_stream.h"

#include "mongo/base/error_codes.h"
#include "mongo/db/client.h"
#include "mongo/db/modules/eloq/data_substrate/core/include/data_substrate.h"
#include "mongo/db/modules/eloq/data_substrate/tx_service/include/sharder.h"
#include "mongo/db/modules/eloq/data_substrate/tx_service/include/tx_service.h"
#include "mongo/db/modules/eloq/src/cdc/cdc_scan_transport.h"
#include "mongo/db/modules/eloq/src/cdc/cdc_stream_registry.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/pipeline/document_source_change_stream.h"
#include "mongo/db/pipeline/resume_token.h"
#include "mongo/db/query/find_common.h"
#include "mongo/db/server_parameters.h"
#include "mongo/util/fail_point_service.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

/**
 * How long a change stream opened without a token may wait, when its command has no maxTimeMS,
 * for every log group to complete a checkpoint round (§5.1). Settable at runtime.
 */
MONGO_EXPORT_SERVER_PARAMETER(cdc_open_wait_ms, int, 30000)
    ->withValidator([](const int& potentialNewValue) {
        if (potentialNewValue <= 0) {
            return Status(ErrorCodes::BadValue, "cdc_open_wait_ms must be positive");
        }
        return Status::OK();
    });

/**
 * Test only (configureFailPoint, with test commands enabled): the aggregate that opens a change
 * stream waits up to {waitMs: <n>} for its scans instead of returning at once, so its first batch
 * can carry events and be cut by its batchSize or by the reply's size, which the batch loop in
 * run_aggregate.cpp otherwise never sees (docs/cdc-change-stream.md §10 item 9).
 */
MONGO_FAIL_POINT_DEFINE(eloqChangeStreamFirstBatchWait);

namespace {

/** The log service's own cap on a scan page (kMaxCdcScanPageBytes in its cdc_scan.cpp). */
constexpr int kMaxScanPageBytes = 32 * 1024 * 1024;

}  // namespace

/**
 * The page size, in bytes, a change stream asks the log service for in each scan (§5.8). The
 * logger stops filling a page before the record that would take it past this size (a page's first
 * record always goes in), and the cursor reserves this much of its buffer per scan in flight. Read
 * when a stream opens, so a change applies to streams opened afterwards. Settable at runtime.
 */
MONGO_EXPORT_SERVER_PARAMETER(cdc_scan_max_bytes,
                              int,
                              static_cast<int>(CdcScanSourceOptions().maxBytesPerScan))
    ->withValidator([](const int& potentialNewValue) {
        if (potentialNewValue <= 0 || potentialNewValue > kMaxScanPageBytes) {
            return Status(ErrorCodes::BadValue,
                          str::stream() << "cdc_scan_max_bytes must be between 1 and "
                                        << kMaxScanPageBytes << ", the log service's cap");
        }
        return Status::OK();
    });

namespace {

/** The largest read buffer, dedup set and long-poll wait a server parameter accepts. */
constexpr long long kMaxCursorBufferBytes = 4LL * 1024 * 1024 * 1024;
constexpr long long kMaxDedupSetBytes = 1LL * 1024 * 1024 * 1024;
constexpr int kMaxScanWaitMs = 10 * 60 * 1000;

}  // namespace

/**
 * A change stream's read buffer, in bytes (§5.8): records received and not yet consumed, plus
 * cdc_scan_max_bytes reserved for each scan in flight. A new scan is issued only while that count
 * is below it, except for the log group whose boundary holds the others back. Read when a stream
 * opens. Settable at runtime.
 */
MONGO_EXPORT_SERVER_PARAMETER(cdc_cursor_buffer_bytes,
                              long long,
                              CdcScanSourceOptions().bufferBytes)
    ->withValidator([](const long long& potentialNewValue) {
        if (potentialNewValue <= 0 || potentialNewValue > kMaxCursorBufferBytes) {
            return Status(ErrorCodes::BadValue,
                          str::stream() << "cdc_cursor_buffer_bytes must be between 1 and "
                                        << kMaxCursorBufferBytes);
        }
        return Status::OK();
    });

/**
 * The bytes of (transaction, namespace, key) entries one log group's dedup set holds at one
 * timestamp (§5.5, §5.8). Past it further entries go unrecorded, so a forward-write duplicate
 * among them may be emitted twice, an idempotent repeat; 0 records nothing. Read when a stream
 * opens. Settable at runtime.
 */
MONGO_EXPORT_SERVER_PARAMETER(cdc_dedup_set_bytes, long long, CdcEventStreamOptions().dedupSetBytes)
    ->withValidator([](const long long& potentialNewValue) {
        if (potentialNewValue < 0 || potentialNewValue > kMaxDedupSetBytes) {
            return Status(ErrorCodes::BadValue,
                          str::stream()
                              << "cdc_dedup_set_bytes must be between 0 and " << kMaxDedupSetBytes);
        }
        return Status::OK();
    });

/**
 * The longest a change stream asks the log service to hold one scan open while nothing new has
 * arrived (the long poll, §5.8); a getMore's own remaining time bounds it too, and the log service
 * caps it with its cdc_scan_max_park_ms. Read when a stream opens. Settable at runtime.
 */
MONGO_EXPORT_SERVER_PARAMETER(
    cdc_scan_max_wait_ms,
    int,
    static_cast<int>(durationCount<Milliseconds>(CdcScanSourceOptions().maxWait)))
    ->withValidator([](const int& potentialNewValue) {
        if (potentialNewValue <= 0 || potentialNewValue > kMaxScanWaitMs) {
            return Status(ErrorCodes::BadValue,
                          str::stream()
                              << "cdc_scan_max_wait_ms must be between 1 and " << kMaxScanWaitMs);
        }
        return Status::OK();
    });

namespace {

/**
 * @brief Checks that this deployment can provide the guarantees a change stream rests on
 * (§5.1): redo records exist, and a log service holds them.
 *
 * The log state itself is checked by the first scan: one that keeps no history (MEMORY) answers
 * every scan with ENOMETHOD, which fails the stream with CommandNotSupported.
 *
 * @return The tx service's log agent.
 * @throws CommandNotSupported otherwise.
 */
txservice::TxLog* supportedLog() {
    uassert(ErrorCodes::CommandNotSupported,
            "$changeStream requires the write-ahead log, which this deployment has disabled; "
            "there are no redo records to read changes from",
            DataSubstrate::Instance().GetCoreConfig().enable_wal);
    txservice::TxLog* log = txservice::Sharder::Instance().GetLogAgent();
    uassert(ErrorCodes::CommandNotSupported,
            "$changeStream requires a log service, and this deployment has none",
            log != nullptr && log->LogGroupCount() > 0);
    return log;
}

/**
 * @brief Decodes the token a client supplied to resume from.
 *
 * @param token {_data: <hex>}, as an event's _id carries it.
 * @param option The option that carried it, for the error message.
 * @return The token.
 * @throws BadValue if it is not an EloqDoc resume token.
 */
CdcResumeToken parseToken(const BSONObj& token, StringData option) {
    return uassertStatusOKWithContext(CdcResumeToken::parse(token),
                                      str::stream() << "invalid '" << option << "' token");
}

}  // namespace

DocumentSourceEloqChangeStream::DocumentSourceEloqChangeStream(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    std::unique_ptr<CdcEventStream> stream,
    Document spec)
    : DocumentSource(expCtx), _stream(std::move(stream)), _spec(std::move(spec)) {
    const Value all = _spec[DocumentSourceChangeStreamSpec::kAllChangesForClusterFieldName];
    const bool deployment = all.getType() == Bool && all.getBool();
    const bool database = !deployment && expCtx->ns.isCollectionlessAggregateNS();
    _registryId = CdcStreamRegistry::get().add(deployment     ? "deployment"
                                                   : database ? "database"
                                                              : "collection",
                                               deployment     ? ""
                                                   : database ? expCtx->ns.db().toString()
                                                              : expCtx->ns.ns(),
                                               &_stream->stats());
}

DocumentSourceEloqChangeStream::~DocumentSourceEloqChangeStream() {
    CdcStreamRegistry::get().remove(_registryId);
}

DocumentSource::GetNextResult DocumentSourceEloqChangeStream::getNext() {
    OperationContext* opCtx = pExpCtx->opCtx;
    boost::optional<Document> event =
        _stream->next(_batchDeadline(), opCtx->getClient()->coroutineFunctors());
    if (!event) {
        return GetNextResult::makeEOF();
    }
    // What the batch loop will find in the delivered document, byte for byte, unless a user
    // stage rewrote it (PipelineProxyStage checks).
    _lastEmitted = (*event)["_id"].getDocument().toBson();
    return std::move(*event);
}

const char* DocumentSourceEloqChangeStream::getSourceName() const {
    return DocumentSourceChangeStream::kStageName.rawData();
}

DocumentSource::StageConstraints DocumentSourceEloqChangeStream::constraints(
    Pipeline::SplitState pipeState) const {
    StageConstraints constraints(StreamType::kStreaming,
                                 PositionRequirement::kFirst,
                                 HostTypeRequirement::kNone,
                                 DiskUseRequirement::kNoDiskUse,
                                 FacetRequirement::kNotAllowed,
                                 TransactionRequirement::kNotAllowed,
                                 ChangeStreamRequirement::kChangeStreamStage);
    constraints.isIndependentOfAnyCollection = pExpCtx->ns.isCollectionlessAggregateNS();
    // The stage reads the log service itself; nothing feeds it documents.
    constraints.requiresInputDocSource = false;
    return constraints;
}

Value DocumentSourceEloqChangeStream::serialize(
    boost::optional<ExplainOptions::Verbosity> explain) const {
    return Value(Document{{getSourceName(), Value(_spec)}});
}

BSONObj DocumentSourceEloqChangeStream::getPostBatchResumeToken() const {
    // The batch loops ask at IS_EOF, when the stream is drained; once at registration, before
    // anything is pulled; and when the stream closes after its invalidate, whose token is then the
    // only one to restart from, even if a user stage filtered the event out.
    if (_stream->invalidated()) {
        return _stream->invalidateToken();
    }
    return _stream->drained() ? _stream->watermark().toBSON() : _stream->openingToken();
}

BSONObj DocumentSourceEloqChangeStream::getLastEmittedResumeToken() const {
    return _lastEmitted;
}

Date_t DocumentSourceEloqChangeStream::_batchDeadline() const {
    // An awaitData getMore has no operation deadline; its maxTimeMS (default 1 s) is here. The
    // aggregate that opens the stream never sets it, so its first batch does not wait.
    const AwaitDataState& state = awaitDataState(pExpCtx->opCtx);
    if (state.shouldWaitForInserts) {
        return state.waitForInsertsDeadline;
    }
    MONGO_FAIL_POINT_BLOCK(eloqChangeStreamFirstBatchWait, wait) {
        return Date_t::now() + Milliseconds(wait.getData()["waitMs"].numberInt());
    }
    return Date_t::now();
}

std::list<boost::intrusive_ptr<DocumentSource>> makeEloqChangeStreamStages(
    const DocumentSourceChangeStreamSpec& spec,
    const boost::intrusive_ptr<ExpressionContext>& expCtx) {
    uassert(ErrorCodes::InvalidOptions,
            "EloqDoc change streams start from a resume token, not an operation time; use "
            "'resumeAfter' or 'startAfter'",
            !spec.getStartAtOperationTime() && !spec.getResumeAfterClusterTimeDeprecated());

    boost::optional<CdcResumeToken> token;
    if (const auto& resumeAfter = spec.getResumeAfter()) {
        token = parseToken(
            resumeAfter->toDocument(ResumeToken::SerializationFormat::kHexString).toBson(),
            DocumentSourceChangeStreamSpec::kResumeAfterFieldName);
        // As in MongoDB: the stream an invalidate ended can only be started after it (§5.1).
        uassert(ErrorCodes::InvalidResumeToken,
                "An invalidate token cannot be used with 'resumeAfter'; use 'startAfter'",
                token->kind() != CdcResumeToken::Kind::kInvalidate);
    } else if (const auto& startAfter = spec.getStartAfter()) {
        token = parseToken(*startAfter, DocumentSourceChangeStreamSpec::kStartAfterFieldName);
    }

    const CdcStreamScope scope = spec.getAllChangesForCluster() ? CdcStreamScope::deployment()
        : expCtx->ns.isCollectionlessAggregateNS() ? CdcStreamScope::database(expCtx->ns.db())
                                                   : CdcStreamScope::collection(expCtx->ns.ns());
    CdcEventStreamOptions options;
    options.source.maxBytesPerScan = static_cast<uint32_t>(cdc_scan_max_bytes.load());
    options.source.bufferBytes = cdc_cursor_buffer_bytes.load();
    options.source.maxWait = Milliseconds(cdc_scan_max_wait_ms.load());
    options.dedupSetBytes = cdc_dedup_set_bytes.load();
    auto stream = std::make_unique<CdcEventStream>(
        std::make_shared<TxLogCdcScanTransport>(supportedLog()), scope, options);

    if (token) {
        stream->openAt(*token);
    } else {
        OperationContext* opCtx = expCtx->opCtx;
        const Milliseconds wait(cdc_open_wait_ms.load());
        const Date_t start = Date_t::now();
        const Date_t deadline = opCtx->hasDeadline() ? opCtx->getDeadline() : start + wait;
        if (!stream->openFresh(deadline, opCtx->getClient()->coroutineFunctors())) {
            const long long waitedMs = durationCount<Milliseconds>(Date_t::now() - start);
            const long long roundMs = 1000LL *
                DataSubstrate::Instance().GetTxService()->GetCheckpointer()->CheckpointInterval();
            const char* limit = opCtx->hasDeadline() ? "maxTimeMS" : "cdc_open_wait_ms";
            str::stream message;
            message << "The change stream could not fix its start position within " << waitedMs
                    << " ms: some log group has not completed a checkpoint round yet, and "
                       "checkpoint rounds run every "
                    << roundMs / 1000 << " s (checkpointer_interval). ";
            if (waitedMs <= roundMs) {
                message << "This open does not wait for a whole round; retry it with " << limit
                        << " above " << roundMs << " ms";
            } else {
                message << "That is expected only right after cluster start-up; retry the open. "
                           "If it keeps failing, the checkpointer is not completing rounds; see "
                           "the server log";
            }
            uasserted(ErrorCodes::ChangeStreamStartNotReady, message);
        }
    }

    // Serialized with the opening token as its resume point, so that re-parsing the pipeline
    // (debug builds round-trip every pipeline) reopens at the same place without waiting. Built
    // field by field: the spec's own serializer cannot write a resumeAfter token.
    MutableDocument serialized;
    serialized.addField(DocumentSourceChangeStreamSpec::kFullDocumentFieldName,
                        Value(spec.getFullDocument()));
    if (spec.getAllChangesForCluster()) {
        serialized.addField(DocumentSourceChangeStreamSpec::kAllChangesForClusterFieldName,
                            Value(true));
    }
    serialized.addField(spec.getStartAfter()
                            ? DocumentSourceChangeStreamSpec::kStartAfterFieldName
                            : DocumentSourceChangeStreamSpec::kResumeAfterFieldName,
                        Value(stream->openingToken()));

    // DocumentSourceChangeStream::createFromBson() adds the stage that closes a collection-level
    // stream's cursor on the pull after its invalidate event (§5.2).
    std::list<boost::intrusive_ptr<DocumentSource>> stages;
    stages.push_back(boost::intrusive_ptr<DocumentSource>(
        new DocumentSourceEloqChangeStream(expCtx, std::move(stream), serialized.freeze())));
    return stages;
}

void registerEloqChangeStreams() {
    if (!DocumentSourceChangeStream::hasStageFactory()) {
        DocumentSourceChangeStream::setStageFactory(makeEloqChangeStreamStages);
    }
}

}  // namespace mongo

/**
 *    Copyright (C) 2018-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/s/query/cluster_client_cursor_impl_rtree_range.h"

#include <memory>

#include "mongo/db/curop.h"
#include "mongo/s/query/router_stage_limit.h"
#include "mongo/s/query/router_stage_merge.h"
#include "mongo/s/query/router_stage_remove_metadata_fields.h"
#include "mongo/s/query/router_stage_skip.h"
#include "mongo/s/rtree/rtree_globle.h"

namespace mongo
{

    static CounterMetric mongosCursorStatsTotalOpened("mongos.cursor.totalOpened");
    static CounterMetric mongosCursorStatsMoreThanOneBatch("mongos.cursor.moreThanOneBatch");

    ClusterClientCursorGuard RTreeRangeClusterClientCursorImpl::make(
        OperationContext *opCtx,
        std::shared_ptr<executor::TaskExecutor> executor,
        ClusterClientCursorParams &&params)
    {
        std::unique_ptr<ClusterClientCursor> cursor(new RTreeRangeClusterClientCursorImpl(
            opCtx, std::move(executor), std::move(params), opCtx->getLogicalSessionId()));
        return ClusterClientCursorGuard(opCtx, std::move(cursor));
    }

    ClusterClientCursorGuard RTreeRangeClusterClientCursorImpl::make(OperationContext *opCtx,
                                                                     std::unique_ptr<RouterExecStage> root,
                                                                     ClusterClientCursorParams &&params)
    {
        std::unique_ptr<ClusterClientCursor> cursor(new RTreeRangeClusterClientCursorImpl(
            opCtx, std::move(root), std::move(params), opCtx->getLogicalSessionId()));
        return ClusterClientCursorGuard(opCtx, std::move(cursor));
    }

    /**
     * Overload make function
     */
    ClusterClientCursorGuard RTreeRangeClusterClientCursorImpl::make(OperationContext *opCtx,
                                                                     std::string dbName,
                                                                     std::string collectionName,
                                                                     mongo::BSONObj InputGeometry,
                                                                     int queryType,
                                                                     ClusterClientCursorParams &&params)
    {
        std::unique_ptr<ClusterClientCursor> cursor(
            new RTreeRangeClusterClientCursorImpl(opCtx, dbName, collectionName, InputGeometry, queryType, std::move(params)));
        return ClusterClientCursorGuard(opCtx, std::move(cursor));
    }

    RTreeRangeClusterClientCursorImpl::RTreeRangeClusterClientCursorImpl(OperationContext *opCtx,
                                                                         std::shared_ptr<executor::TaskExecutor> executor,
                                                                         ClusterClientCursorParams &&params,
                                                                         boost::optional<LogicalSessionId> lsid)
        : _params(std::move(params)),
          _root(buildMergerPlan(opCtx, std::move(executor), &_params)),
          _lsid(lsid),
          _opCtx(opCtx),
          _createdDate(opCtx->getServiceContext()->getPreciseClockSource()->now()),
          _lastUseDate(_createdDate),
          _queryHash(CurOp::get(opCtx)->debug().queryHash)
    {
        dassert(!_params.compareWholeSortKeyOnRouter ||
                SimpleBSONObjComparator::kInstance.evaluate(
                    _params.sortToApplyOnRouter == AsyncResultsMerger::kWholeSortKeySortPattern));
        mongosCursorStatsTotalOpened.increment();
    }

    RTreeRangeClusterClientCursorImpl::RTreeRangeClusterClientCursorImpl(OperationContext *opCtx,
                                                                         std::unique_ptr<RouterExecStage> root,
                                                                         ClusterClientCursorParams &&params,
                                                                         boost::optional<LogicalSessionId> lsid)
        : _params(std::move(params)),
          _root(std::move(root)),
          _lsid(lsid),
          _opCtx(opCtx),
          _createdDate(opCtx->getServiceContext()->getPreciseClockSource()->now()),
          _lastUseDate(_createdDate),
          _queryHash(CurOp::get(opCtx)->debug().queryHash)
    {
        dassert(!_params.compareWholeSortKeyOnRouter ||
                SimpleBSONObjComparator::kInstance.evaluate(
                    _params.sortToApplyOnRouter == AsyncResultsMerger::kWholeSortKeySortPattern));
        mongosCursorStatsTotalOpened.increment();
    }

    /**
     * Overload constructor
     */
    RTreeRangeClusterClientCursorImpl::RTreeRangeClusterClientCursorImpl(OperationContext *opCtx,
                                                                         std::string dbName,
                                                                         std::string collectionName,
                                                                         mongo::BSONObj InputGeometry,
                                                                         int queryType,
                                                                         ClusterClientCursorParams &&params) : _params(std::move(params))
    {
        if (queryType == 0) //$geoIntersects
        {
            _rtreeRangeQueryCursor = IM.GeoSearchIntersects(opCtx, dbName, collectionName, InputGeometry);
            _isRtreeCursorOK = true;
        }
        if (queryType == 1) //$geoWithin
        {
            _rtreeRangeQueryCursor = IM.GeoSearchWithin(opCtx, dbName, collectionName, InputGeometry);
            _isRtreeCursorOK = true;
        }
    }

    RTreeRangeClusterClientCursorImpl::~RTreeRangeClusterClientCursorImpl()
    {
        if (_nBatchesReturned > 1)
            mongosCursorStatsMoreThanOneBatch.increment();
    }

    StatusWith<ClusterQueryResult> RTreeRangeClusterClientCursorImpl::next()
    {
        invariant(_opCtx);
        const auto interruptStatus = _opCtx->checkForInterruptNoAssert();
        if (!interruptStatus.isOK())
        {
            _maxTimeMSExpired |= (interruptStatus.code() == ErrorCodes::MaxTimeMSExpired);
            return interruptStatus;
        }

        // First return stashed results, if there are any.
        if (!_stash.empty())
        {
            auto front = std::move(_stash.front());
            _stash.pop();
            ++_numReturnedSoFar;
            return {front};
        }

        auto next = _rtreeRangeQueryCursor->Next();
        if (!next.isEmpty())
        {
            ++_numReturnedSoFar;
        }
        else
        {
            if (_isRtreeCursorOK)
            {
                _isRtreeCursorOK = false;
                _rtreeRangeQueryCursor->FreeCursor();
            }
        }
        return next;
    }

    void RTreeRangeClusterClientCursorImpl::kill(OperationContext *opCtx)
    {
        if (_isRtreeCursorOK)
        {
            _isRtreeCursorOK = false;
            _rtreeRangeQueryCursor->FreeCursor();
        }
        _root->kill(opCtx);
    }

    void RTreeRangeClusterClientCursorImpl::reattachToOperationContext(OperationContext *opCtx)
    {
        _opCtx = opCtx;
        _root->reattachToOperationContext(opCtx);
    }

    void RTreeRangeClusterClientCursorImpl::detachFromOperationContext()
    {
        _opCtx = nullptr;
        _root->detachFromOperationContext();
    }

    OperationContext *RTreeRangeClusterClientCursorImpl::getCurrentOperationContext() const
    {
        return _opCtx;
    }

    bool RTreeRangeClusterClientCursorImpl::isTailable() const
    {
        return _params.tailableMode != TailableModeEnum::kNormal;
    }

    bool RTreeRangeClusterClientCursorImpl::isTailableAndAwaitData() const
    {
        return _params.tailableMode == TailableModeEnum::kTailableAndAwaitData;
    }

    BSONObj RTreeRangeClusterClientCursorImpl::getOriginatingCommand() const
    {
        return _params.originatingCommandObj;
    }

    const PrivilegeVector &RTreeRangeClusterClientCursorImpl::getOriginatingPrivileges() const &
    {
        return _params.originatingPrivileges;
    }

    bool RTreeRangeClusterClientCursorImpl::partialResultsReturned() const
    {
        // We may have timed out in this layer, or within the plan tree waiting for results from shards.
        return (_maxTimeMSExpired && _params.isAllowPartialResults) || _root->partialResultsReturned();
    }

    std::size_t RTreeRangeClusterClientCursorImpl::getNumRemotes() const
    {
        return _root->getNumRemotes();
    }

    BSONObj RTreeRangeClusterClientCursorImpl::getPostBatchResumeToken() const
    {
        return _root->getPostBatchResumeToken();
    }

    long long RTreeRangeClusterClientCursorImpl::getNumReturnedSoFar() const
    {
        return _numReturnedSoFar;
    }

    void RTreeRangeClusterClientCursorImpl::queueResult(const ClusterQueryResult &result)
    {
        auto resultObj = result.getResult();
        if (resultObj)
        {
            invariant(resultObj->isOwned());
        }
        _stash.push(result);
    }

    void RTreeRangeClusterClientCursorImpl::setExhausted(bool isExhausted)
    {
        _isExhausted = isExhausted;
    }

    bool RTreeRangeClusterClientCursorImpl::remotesExhausted()
    {
        return _root->remotesExhausted();
    }

    Status RTreeRangeClusterClientCursorImpl::setAwaitDataTimeout(Milliseconds awaitDataTimeout)
    {
        return _root->setAwaitDataTimeout(awaitDataTimeout);
    }

    boost::optional<LogicalSessionId> RTreeRangeClusterClientCursorImpl::getLsid() const
    {
        return _lsid;
    }

    boost::optional<TxnNumber> RTreeRangeClusterClientCursorImpl::getTxnNumber() const
    {
        return _params.txnNumber;
    }

    Date_t RTreeRangeClusterClientCursorImpl::getCreatedDate() const
    {
        return _createdDate;
    }

    Date_t RTreeRangeClusterClientCursorImpl::getLastUseDate() const
    {
        return _lastUseDate;
    }

    void RTreeRangeClusterClientCursorImpl::setLastUseDate(Date_t now)
    {
        _lastUseDate = std::move(now);
    }

    boost::optional<uint32_t> RTreeRangeClusterClientCursorImpl::getQueryHash() const
    {
        return _queryHash;
    }

    std::uint64_t RTreeRangeClusterClientCursorImpl::getNBatches() const
    {
        return _nBatchesReturned;
    }

    void RTreeRangeClusterClientCursorImpl::incNBatches()
    {
        ++_nBatchesReturned;
    }

    APIParameters RTreeRangeClusterClientCursorImpl::getAPIParameters() const
    {
        return _params.apiParameters;
    }

    boost::optional<ReadPreferenceSetting> RTreeRangeClusterClientCursorImpl::getReadPreference() const
    {
        return _params.readPreference;
    }

    boost::optional<repl::ReadConcernArgs> RTreeRangeClusterClientCursorImpl::getReadConcern() const
    {
        return _params.readConcern;
    }

    std::unique_ptr<RouterExecStage> RTreeRangeClusterClientCursorImpl::buildMergerPlan(
        OperationContext *opCtx,
        std::shared_ptr<executor::TaskExecutor> executor,
        ClusterClientCursorParams *params)
    {
        const auto skip = params->skipToApplyOnRouter;
        const auto limit = params->limit;

        std::unique_ptr<RouterExecStage> root =
            std::make_unique<RouterStageMerge>(opCtx, executor, params->extractARMParams());

        if (skip)
        {
            root = std::make_unique<RouterStageSkip>(opCtx, std::move(root), *skip);
        }

        if (limit)
        {
            root = std::make_unique<RouterStageLimit>(opCtx, std::move(root), *limit);
        }

        const bool hasSort = !params->sortToApplyOnRouter.isEmpty();
        if (hasSort)
        {
            // Strip out the sort key after sorting.
            root = std::make_unique<RouterStageRemoveMetadataFields>(
                opCtx, std::move(root), StringDataSet{AsyncResultsMerger::kSortKeyField});
        }

        return root;
    }

} // namespace mongo

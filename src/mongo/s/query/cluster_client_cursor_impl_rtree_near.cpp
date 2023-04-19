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

#include "mongo/s/query/cluster_client_cursor_impl_rtree_near.h"

#include <memory>

#include "mongo/db/curop.h"
#include "mongo/s/query/router_stage_limit.h"
#include "mongo/s/query/router_stage_merge.h"
#include "mongo/s/query/router_stage_remove_metadata_fields.h"
#include "mongo/s/query/router_stage_skip.h"

namespace mongo
{

    ClusterClientCursorGuard RTreeNearClusterClientCursorImpl::make(
        OperationContext *opCtx,
        std::shared_ptr<executor::TaskExecutor> executor,
        ClusterClientCursorParams &&params)
    {
        std::unique_ptr<ClusterClientCursor> cursor(new RTreeNearClusterClientCursorImpl(
            opCtx, std::move(executor), std::move(params), opCtx->getLogicalSessionId()));
        return ClusterClientCursorGuard(opCtx, std::move(cursor));
    }

    ClusterClientCursorGuard RTreeNearClusterClientCursorImpl::make(OperationContext *opCtx,
                                                                    std::unique_ptr<RouterExecStage> root,
                                                                    ClusterClientCursorParams &&params)
    {
        std::unique_ptr<ClusterClientCursor> cursor(new RTreeNearClusterClientCursorImpl(
            opCtx, std::move(root), std::move(params), opCtx->getLogicalSessionId()));
        return ClusterClientCursorGuard(opCtx, std::move(cursor));
    }

    /**
     * Overloaded version of make function
     */
    ClusterClientCursorGuard RTreeNearClusterClientCursorImpl::make(OperationContext *opCtx,
                                                                    std::shared_ptr<executor::TaskExecutor> executor,
                                                                    std::string dbName,
                                                                    std::string collectionName,
                                                                    double ctx,
                                                                    double cty,
                                                                    double rMin,
                                                                    double rMax,
                                                                    ClusterClientCursorParams &&params)
    {
        std::unique_ptr<ClusterClientCursor> cursor(
            new RTreeNearClusterClientCursorImpl(opCtx, std::move(executor), dbName, collectionName, ctx, cty, rMin, rMax, std::move(params)));
        return ClusterClientCursorGuard(opCtx, std::move(cursor));
    }

    RTreeNearClusterClientCursorImpl::RTreeNearClusterClientCursorImpl(OperationContext *opCtx,
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
    }

    RTreeNearClusterClientCursorImpl::RTreeNearClusterClientCursorImpl(OperationContext *opCtx,
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
    }

    RTreeNearClusterClientCursorImpl::RTreeNearClusterClientCursorImpl(OperationContext *txn,
                                                                       std::shared_ptr<executor::TaskExecutor> executor,
                                                                       std::string DB_NAME,
                                                                       std::string COLLECTION_NAME,
                                                                       double ctx,
                                                                       double cty,
                                                                       double rMin,
                                                                       double rMax,
                                                                       ClusterClientCursorParams &&params) : _params(std::move(params)), _root(buildMergerPlan(txn, std::move(executor), &_params)), _opCtx(txn), _lsid(txn->getLogicalSessionId())
    {
        _rtreeGeoNearCursor = IM.GeoSearchNear(txn, DB_NAME, COLLECTION_NAME, ctx, cty, rMin, rMax);
        _isRtreeCursorOK = true;
    }

    RTreeNearClusterClientCursorImpl::~RTreeNearClusterClientCursorImpl()
    {
    }

    StatusWith<ClusterQueryResult> RTreeNearClusterClientCursorImpl::next()
    {
        invariant(_opCtx);
        const auto interruptStatus = _opCtx->checkForInterruptNoAssert();
        if (!interruptStatus.isOK())
        {
            _maxTimeMSExpired |= (interruptStatus.code() == ErrorCodes::MaxTimeMSExpired);
            return interruptStatus;
        }
        std::cout << "We made it into the next function call in RtreeNearClusterClientCursorImpl\n";
        // First return stashed results, if there are any.
        if (!_stash.empty())
        {
            auto front = std::move(_stash.front());
            _stash.pop();
            ++_numReturnedSoFar;
            return {front};
        }
        
        auto next = _rtreeGeoNearCursor->Next();
        if (!next.isEmpty())
        {
            ++_numReturnedSoFar;
        } else
        {
            if (_isRtreeCursorOK)
            {
                _isRtreeCursorOK = false;
                _rtreeGeoNearCursor->FreeCursor();
                return ClusterQueryResult();
            }
        }

        return ClusterQueryResult(next);
    }

    void RTreeNearClusterClientCursorImpl::kill(OperationContext *opCtx)
    {
        if (_isRtreeCursorOK)
        {
            _isRtreeCursorOK = false;
            _rtreeGeoNearCursor->FreeCursor();
        }
        _root->kill(opCtx);
    }

    void RTreeNearClusterClientCursorImpl::reattachToOperationContext(OperationContext *opCtx)
    {
        _opCtx = opCtx;
        _root->reattachToOperationContext(opCtx);
    }

    void RTreeNearClusterClientCursorImpl::detachFromOperationContext()
    {
        _opCtx = nullptr;
        _root->detachFromOperationContext();
    }

    OperationContext *RTreeNearClusterClientCursorImpl::getCurrentOperationContext() const
    {
        return _opCtx;
    }

    bool RTreeNearClusterClientCursorImpl::isTailable() const
    {
        return _params.tailableMode != TailableModeEnum::kNormal;
    }

    bool RTreeNearClusterClientCursorImpl::isTailableAndAwaitData() const
    {
        return _params.tailableMode == TailableModeEnum::kTailableAndAwaitData;
    }

    BSONObj RTreeNearClusterClientCursorImpl::getOriginatingCommand() const
    {
        return _params.originatingCommandObj;
    }

    const PrivilegeVector &RTreeNearClusterClientCursorImpl::getOriginatingPrivileges() const &
    {
        return _params.originatingPrivileges;
    }

    bool RTreeNearClusterClientCursorImpl::partialResultsReturned() const
    {
        // We may have timed out in this layer, or within the plan tree waiting for results from shards.
        return (_maxTimeMSExpired && _params.isAllowPartialResults) || _root->partialResultsReturned();
    }

    std::size_t RTreeNearClusterClientCursorImpl::getNumRemotes() const
    {
        return _root->getNumRemotes();
    }

    BSONObj RTreeNearClusterClientCursorImpl::getPostBatchResumeToken() const
    {
        return _root->getPostBatchResumeToken();
    }

    long long RTreeNearClusterClientCursorImpl::getNumReturnedSoFar() const
    {
        return _numReturnedSoFar;
    }

    void RTreeNearClusterClientCursorImpl::queueResult(const ClusterQueryResult &result)
    {
        auto resultObj = result.getResult();
        if (resultObj)
        {
            invariant(resultObj->isOwned());
        }
        _stash.push(result);
    }

    void RTreeNearClusterClientCursorImpl::setExhausted(bool isExhausted)
    {
        _isExhausted = isExhausted;
    }

    bool RTreeNearClusterClientCursorImpl::remotesExhausted()
    {
        return _root->remotesExhausted();
    }

    Status RTreeNearClusterClientCursorImpl::setAwaitDataTimeout(Milliseconds awaitDataTimeout)
    {
        return _root->setAwaitDataTimeout(awaitDataTimeout);
    }

    boost::optional<LogicalSessionId> RTreeNearClusterClientCursorImpl::getLsid() const
    {
        return _lsid;
    }

    boost::optional<TxnNumber> RTreeNearClusterClientCursorImpl::getTxnNumber() const
    {
        return _params.txnNumber;
    }

    Date_t RTreeNearClusterClientCursorImpl::getCreatedDate() const
    {
        return _createdDate;
    }

    Date_t RTreeNearClusterClientCursorImpl::getLastUseDate() const
    {
        return _lastUseDate;
    }

    void RTreeNearClusterClientCursorImpl::setLastUseDate(Date_t now)
    {
        _lastUseDate = std::move(now);
    }

    boost::optional<uint32_t> RTreeNearClusterClientCursorImpl::getQueryHash() const
    {
        return _queryHash;
    }

    std::uint64_t RTreeNearClusterClientCursorImpl::getNBatches() const
    {
        return _nBatchesReturned;
    }

    void RTreeNearClusterClientCursorImpl::incNBatches()
    {
        ++_nBatchesReturned;
    }

    APIParameters RTreeNearClusterClientCursorImpl::getAPIParameters() const
    {
        return _params.apiParameters;
    }

    boost::optional<ReadPreferenceSetting> RTreeNearClusterClientCursorImpl::getReadPreference() const
    {
        return _params.readPreference;
    }

    boost::optional<repl::ReadConcernArgs> RTreeNearClusterClientCursorImpl::getReadConcern() const
    {
        return _params.readConcern;
    }

    std::unique_ptr<RouterExecStage> RTreeNearClusterClientCursorImpl::buildMergerPlan(
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

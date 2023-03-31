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

#include "mongo/s/query/cluster_client_cursor_impl_modified.h"

#include <memory>

#include "mongo/db/curop.h"
#include "mongo/s/query/router_stage_limit.h"
#include "mongo/s/query/router_stage_merge.h"
#include "mongo/s/query/router_stage_remove_metadata_fields.h"
#include "mongo/s/query/router_stage_skip.h"
#include "mongo/s/grid.h"
#include "mongo/db/s/config/sharding_catalog_manager.h"

namespace mongo
{

    static CounterMetric mongosCursorStatsTotalOpened("mongos.cursor.totalOpened");
    static CounterMetric mongosCursorStatsMoreThanOneBatch("mongos.cursor.moreThanOneBatch");

    ClusterClientCursorGuard ClusterClientCursorImpl::make(
        OperationContext *opCtx,
        std::shared_ptr<executor::TaskExecutor> executor,
        ClusterClientCursorParams &&params)
    {
        std::unique_ptr<ClusterClientCursor> cursor(new ClusterClientCursorImpl(
            opCtx, std::move(executor), std::move(params), opCtx->getLogicalSessionId()));
        return ClusterClientCursorGuard(opCtx, std::move(cursor));
    }

    ClusterClientCursorGuard ClusterClientCursorImpl::make(OperationContext *opCtx,
                                                           std::unique_ptr<RouterExecStage> root,
                                                           ClusterClientCursorParams &&params)
    {
        std::unique_ptr<ClusterClientCursor> cursor(new ClusterClientCursorImpl(
            opCtx, std::move(root), std::move(params), opCtx->getLogicalSessionId()));
        return ClusterClientCursorGuard(opCtx, std::move(cursor));
    }

    ClusterClientCursorGuard ClusterClientCursorImpl::make(OperationContext *opCtx,
                                                           const char *ns,
                                                           BSONObj &jsobj,
                                                           BSONObjBuilder &anObjBuilder,
                                                           ClusterClientCursorParams &&params,
                                                           int queryOptions)
    {
        std::unique_ptr<ClusterClientCursor> cursor(
            new ClusterClientCursorImpl(opCtx, ns, jsobj, anObjBuilder, std::move(params), queryOptions));
        return ClusterClientCursorGuard(opCtx, std::move(cursor));
    }

    ClusterClientCursorImpl::ClusterClientCursorImpl(OperationContext *opCtx,
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

    ClusterClientCursorImpl::ClusterClientCursorImpl(OperationContext *opCtx,
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

    ClusterClientCursorImpl::ClusterClientCursorImpl(OperationContext *opCtx,
                                                     const char *ns,
                                                     BSONObj &jsobj,
                                                     BSONObjBuilder &anObjBuilder,
                                                     ClusterClientCursorParams &&params,
                                                     int queryOptions) : _params(std::move(params)), _isRtree(true)
    {
        BSONElement e = jsobj.firstElement();
        std::string dbname = nsToDatabase(ns);
        std::string collName = e.String();
        std::string columnName;
        BSONObj query_condition;
        std::string commandName = e.fieldName(); // cmd name
        // log() << "ns:" << q.ns << ", collname: " << collName << endl;
        BSONObjBuilder bdr;
        // Whether the collection creates rtree index
        // bool is_rtree = false;
        // Whether the field that query is registered
        bool is_registered = false;
        // Whether the cmd is geowithin
        bool is_command_geowithin = false;
        // Whether the cmd is geoIntersects
        bool is_command_geointersects = false;
        // Whether the cmd is geonear
        bool is_command_geonear = false;
        // Whether the query type is polygon
        bool is_type_polygon = false;
        // Whether the query type is point
        bool is_type_point = false;

        bdr.append("namespace", dbname + "." + collName);
        auto database_status = Grid::get(opCtx)->catalogCache()->getDatabase(opCtx, dbname);
        uassertStatusOK(database_status.getStatus());
        // std::shared_ptr<DBConfig> conf = database_status.getValue();
        // BSONObj geometadata = conf->getGeometry(opCtx, bdr.obj());
        BSONObj geometadata = ShardingCatalogManager::get(opCtx)->getGeometry(opCtx, bdr.obj());
        // if (!geometadata.isEmpty())
        // is_rtree = geometadata["INDEX_TYPE"].Int() != 0 ? true : false;
        columnName = geometadata["column_name"].str();
        BSONElement geowithincomm;
        BSONObj geometry;
        BSONObj type;
        BSONObj filter = jsobj["filter"].Obj();

        if (!filter.isEmpty() && columnName.compare(std::string(filter.firstElement().fieldName())) == 0 && filter.firstElement().isABSONObj())
        {
            // log() << "cao:" << filter.firstElement().Obj();
            is_registered = true;
            geowithincomm = filter.firstElement().Obj().firstElement();
            if ("$geoWithin" == std::string(geowithincomm.fieldName()))
            {
                is_command_geowithin = true;
                // log() << "geowithin : " << string(geowithincomm.fieldName()) << endl; ;
                geometry = geowithincomm.Obj();
                // log() << "geometry cmd:" << geometry << endl;
                // log() << "geometry cmd name:" << geometry.firstElement().fieldName() << endl;
                if (geometry.firstElement().fieldName() != NULL && "$geometry" == std::string(geometry.firstElement().fieldName()))
                {
                    // log() << "last step";
                    query_condition = geometry["$geometry"].Obj();
                    if ("Polygon" == query_condition["type"].str() || "MultiPolygon" == query_condition["type"].str())
                        is_type_polygon = true;
                }
            }
            else if ("$geoIntersects" == std::string(geowithincomm.fieldName()))
            {
                is_command_geointersects = true;
                geometry = geowithincomm.Obj();
                query_condition = geometry["$geometry"].Obj();
            }
            else if ("$near" == std::string(geowithincomm.fieldName()))
            {
                is_command_geonear = true;
                geometry = geowithincomm.Obj();
                if (geometry.firstElement().fieldName() != NULL && (geometry.hasField("$geometry")))
                {
                    double maxDistance = 100;
                    if (geometry.hasField("$maxDistance"))
                        maxDistance = geometry["$maxDistance"].numberDouble();
                    double minDistance = 0;
                    if (geometry.hasField("$minDistance"))
                        minDistance = geometry["$minDistance"].numberDouble();

                    // log()<<"maxDistance:"<<maxDistance;
                    // log()<<"minDistance:"<<minDistance;
                    BSONObjBuilder query_condition_builder;
                    query_condition_builder.appendElements(geometry["$geometry"].Obj());
                    query_condition_builder.append("$maxDistance", maxDistance);
                    query_condition_builder.append("$minDistance", minDistance);
                    query_condition = query_condition_builder.obj();
                    if ("Point" == query_condition["type"].str())
                        is_type_point = true;
                }
            }
        }
        if (is_registered && is_command_geowithin && is_type_polygon)
        {
            commandName = "geoWithinSearch";
            BSONObjBuilder cmdObj;
            cmdObj.append("collection", collName);
            cmdObj.append("condition", query_condition);
            jsobj = cmdObj.obj();
        }
        if (is_registered && is_command_geointersects)
        {
            commandName = "geoIntersectSearch";
            BSONObjBuilder cmdObj;
            cmdObj.append("collection", collName);
            cmdObj.append("condition", query_condition);
            jsobj = cmdObj.obj();
        }
        if (is_registered && is_command_geonear && is_type_point)
        {
            commandName = "geoNearSearch";
            BSONObjBuilder cmdObj;
            cmdObj.append("collection", collName);
            cmdObj.append("condition", query_condition);
            jsobj = cmdObj.obj();
        }
        // log()<<"before run, check the command name:"<<commandName;
        auto _command = e.type() ? CommandHelpers::findCommand(commandName) : NULL;

        // // if (jsobj.getBoolField("help"))
        // // {
        // //     std::stringstream help;
        // //     help << "help for: " << commandName << " ";
        // //     _command->generateHelpResponse(opCtx, help);
        // //     anObjBuilder.append("help", help.str());
        // //     anObjBuilder.append("lockType", _command->isWriteCommandForConfigServer() ? 1 : 0);
        // //     // appendCommandStatus(anObjBuilder, true, "");
        // //     return;
        // // }
        /*
       _command->_commandsExecuted.increment();
       if (_command->shouldAffectCommandCounter()) {
           globalOpCounters.gotCommand();
       }
       */
        std::string errmsg;
        bool ok = false;
        try
        {
            ok = _command(opCtx, dbname, jsobj, queryOptions, errmsg, anObjBuilder);
        }
        catch (const DBException &e)
        {
            anObjBuilder.resetToEmpty();
            const int code = e.code();

            // Codes for StaleConfigException
            if (code == ErrorCodes::StaleConfig)
            {
                throw;
            }

            errmsg = e.what();
            anObjBuilder.append("code", code);
        }
        /*
        if (!ok) {
            _command->_commandsFailed.increment();
        }
        */
        // appendCommandStatus(anObjBuilder, ok, errmsg);
    }

    ClusterClientCursorImpl::~ClusterClientCursorImpl()
    {
        if (_nBatchesReturned > 1)
            mongosCursorStatsMoreThanOneBatch.increment();
    }

    StatusWith<ClusterQueryResult> ClusterClientCursorImpl::next()
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

        auto next = _root->next();
        if (next.isOK() && !next.getValue().isEOF())
        {
            ++_numReturnedSoFar;
        }
        // Record if we just got a MaxTimeMSExpired error.
        _maxTimeMSExpired |= (next.getStatus().code() == ErrorCodes::MaxTimeMSExpired);
        return next;
    }

    void ClusterClientCursorImpl::kill(OperationContext *opCtx)
    {
        _root->kill(opCtx);
    }

    void ClusterClientCursorImpl::reattachToOperationContext(OperationContext *opCtx)
    {
        _opCtx = opCtx;
        _root->reattachToOperationContext(opCtx);
    }

    void ClusterClientCursorImpl::detachFromOperationContext()
    {
        _opCtx = nullptr;
        _root->detachFromOperationContext();
    }

    OperationContext *ClusterClientCursorImpl::getCurrentOperationContext() const
    {
        return _opCtx;
    }

    bool ClusterClientCursorImpl::isTailable() const
    {
        return _params.tailableMode != TailableModeEnum::kNormal;
    }

    bool ClusterClientCursorImpl::isTailableAndAwaitData() const
    {
        return _params.tailableMode == TailableModeEnum::kTailableAndAwaitData;
    }

    BSONObj ClusterClientCursorImpl::getOriginatingCommand() const
    {
        return _params.originatingCommandObj;
    }

    const PrivilegeVector &ClusterClientCursorImpl::getOriginatingPrivileges() const &
    {
        return _params.originatingPrivileges;
    }

    bool ClusterClientCursorImpl::partialResultsReturned() const
    {
        // We may have timed out in this layer, or within the plan tree waiting for results from shards.
        return (_maxTimeMSExpired && _params.isAllowPartialResults) || _root->partialResultsReturned();
    }

    std::size_t ClusterClientCursorImpl::getNumRemotes() const
    {
        return _root->getNumRemotes();
    }

    BSONObj ClusterClientCursorImpl::getPostBatchResumeToken() const
    {
        return _root->getPostBatchResumeToken();
    }

    long long ClusterClientCursorImpl::getNumReturnedSoFar() const
    {
        return _numReturnedSoFar;
    }

    void ClusterClientCursorImpl::queueResult(const ClusterQueryResult &result)
    {
        auto resultObj = result.getResult();
        if (resultObj)
        {
            invariant(resultObj->isOwned());
        }
        _stash.push(result);
    }

    void ClusterClientCursorImpl::setExhausted(bool isExhausted)
    {
        _isExhausted = isExhausted;
    }

    bool ClusterClientCursorImpl::remotesExhausted()
    {
        return _root->remotesExhausted();
    }

    Status ClusterClientCursorImpl::setAwaitDataTimeout(Milliseconds awaitDataTimeout)
    {
        return _root->setAwaitDataTimeout(awaitDataTimeout);
    }

    boost::optional<LogicalSessionId> ClusterClientCursorImpl::getLsid() const
    {
        return _lsid;
    }

    boost::optional<TxnNumber> ClusterClientCursorImpl::getTxnNumber() const
    {
        return _params.txnNumber;
    }

    Date_t ClusterClientCursorImpl::getCreatedDate() const
    {
        return _createdDate;
    }

    Date_t ClusterClientCursorImpl::getLastUseDate() const
    {
        return _lastUseDate;
    }

    void ClusterClientCursorImpl::setLastUseDate(Date_t now)
    {
        _lastUseDate = std::move(now);
    }

    boost::optional<uint32_t> ClusterClientCursorImpl::getQueryHash() const
    {
        return _queryHash;
    }

    std::uint64_t ClusterClientCursorImpl::getNBatches() const
    {
        return _nBatchesReturned;
    }

    void ClusterClientCursorImpl::incNBatches()
    {
        ++_nBatchesReturned;
    }

    APIParameters ClusterClientCursorImpl::getAPIParameters() const
    {
        return _params.apiParameters;
    }

    boost::optional<ReadPreferenceSetting> ClusterClientCursorImpl::getReadPreference() const
    {
        return _params.readPreference;
    }

    boost::optional<repl::ReadConcernArgs> ClusterClientCursorImpl::getReadConcern() const
    {
        return _params.readConcern;
    }

    std::unique_ptr<RouterExecStage> ClusterClientCursorImpl::buildMergerPlan(
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

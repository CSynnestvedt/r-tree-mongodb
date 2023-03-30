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

#include "mongo/platform/basic.h"

#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/privilege.h"
#include "mongo/db/commands.h"
#include "mongo/logv2/log.h"
#include "mongo/s/cluster_commands_helpers.h"
#include "mongo/s/grid.h"
#include "mongo/s/rtree/rtree_globle.h"
#include "mongo/s/rtree/rtree_cursor.h"
#include "mongo/s/rtree/rtree_range_query_cursor.h"




#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kCommand


namespace mongo {
namespace {

class CmdGeoIntersectSearch : public BasicCommand {
public:
    CmdGeoIntersectSearch() : BasicCommand("geoIntersectSearch") {}

    std::string help() const override {
        return "searches for objects within the specified range";
    }

    bool supportsWriteConcern(const BSONObj& cmd) const override {
        return false;
    }

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kAlways;
    }

    bool adminOnly() const override {
        return false;
    }

    Status checkAuthForOperation(OperationContext* opCtx,
                                 const DatabaseName&,
                                 const BSONObj&) const override {
        auto* as = AuthorizationSession::get(opCtx->getClient());
        if (!as->isAuthorizedForActionsOnResource(ResourcePattern::forClusterResource(),
                                                  ActionType::getShardMap)) {
            return {ErrorCodes::Unauthorized, "unauthorized"};
        }

        return Status::OK();
    }

    bool run(OperationContext* opCtx,
            const DatabaseName& dbName,
            const BSONObj& cmdObj,
            BSONObjBuilder& result
    ) {

        int stat;
        try {
            if (!pIndexManagerIO->IsConnected())
                pIndexManagerIO->connectMyself();
            if (!pRTreeIO->IsConnected())
                pRTreeIO->connectMyself();


            string collection = cmdObj["collection"].str();
            returnCursor = IM.GeoSearchIntersects(opCtx, dbName.db() ,collection,cmdObj["condition"].Obj()).get();
            // int key_count = returnCursor->Count();
            // log()<<"numbers of OID"<<key_count;

            BSONObjBuilder query;
            BSONObjBuilder filter;
            BSONArrayBuilder query_criteria;
            auto cursorNext = returnCursor->Next();
            while (!cursorNext.isEmpty())
            {
                query_criteria.append(cursorNext["_id"]);

                cursorNext = returnCursor->Next();
            }
            BSONObjBuilder in_query;
            in_query.append("$in",query_criteria.arr());
            filter.append("_id",in_query.obj());
            query.append("find",collection);
            query.append("filter",filter.obj());
            BSONObj show_query = query.obj();
            // log()<<"at the very last, show the query:"<<show_query;


            const NamespaceString nss(parseNs(dbName, cmdObj));
            // log()<<"check collection name in CmdgeoWithinSearch:"<< ns; 
            const auto cri = uassertStatusOK(Grid::get(opCtx)->catalogCache()->getCollectionRoutingInfo(opCtx, nss));

            auto cmdToBeSent = query.obj();

            auto shardResponses = scatterGatherVersionedTargetByRoutingTable(
                opCtx,
                nss.db(),
                nss,
                cri,
                CommandHelpers::filterCommandRequestForPassthrough(
                    applyReadWriteConcern(opCtx, this, cmdToBeSent)),
                ReadPreferenceSetting(ReadPreference::PrimaryPreferred),
                Shard::RetryPolicy::kNoRetry,
                BSONObj() /* query */,
                BSONObj() /* collation */
            );

            std::string errmsg;
            const bool ok = appendRawResponses(opCtx, &errmsg, &result, std::move(shardResponses)).responseOK;
            CommandHelpers::appendSimpleCommandStatus(result, ok, errmsg);

        if (ok) {
            LOGV2(40002, "GeoWithinCursor returned results successfully", "namespace"_attr = nss);
        }
        return ok;
            
        }
        catch (DBException& e) {
            stat = -1;
            int code = e.code();

            stringstream ss;
            std::string errMsg;
            ss << "exception: " << e.what();
            errMsg = ss.str();
            result.append("code", code);
        }
        bool ok = (stat == 1) ? true : false;
        return ok;
    }

    int rtreeDataMore(int nCount,std::queue<BSONObj>& results)
    {
        //log()<<"jinrule datamore"<<endl;
        if(nCount<1)
          return 0;
        for(int i = 0;i<nCount;i++)
        {
            //log()<<"will insert data-com"<<endl;
            BSONObj obj = returnCursor->Next();
            if(obj.isEmpty())
               break;
            results.push(obj);
        }
        return results.size();
    }

    void freeCursor() 
    {
        returnCursor->FreeCursor();
    }

    // Slaves can't perform writes.
    bool slaveOk() const { return false; }

     // all grid commands are designed not to lock
    virtual bool isWriteCommandForConfigServer() const {
        return false;
    }
private:
    RTreeRangeQueryCursor* returnCursor;

} geoIntersectSearch;

}  // namespace
}  // namespace mongo

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
#include "mongo/s/query/cluster_find.h"


#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kCommand


namespace mongo {
namespace {

class CmdGeoNearSearch : public BasicCommand {
public:
    CmdGeoNearSearch() : BasicCommand("geoNearSearch") {}

    std::string help() const override {
        return "searches for objects which is near the geoobject";
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
        bool apiStrict = APIParameters::get(opCtx).getAPIStrict().value_or(false);
        NamespaceString nss(dbName);
        auto opMsgRequest = OpMsgRequest::fromDBAndBody(nss.db(), cmdObj).body;
        auto findCommand = query_request_helper::makeFromFindCommand(
            opMsgRequest, nss, apiStrict);

        const boost::intrusive_ptr<ExpressionContext> expCtx;
        auto cq = uassertStatusOK(
            CanonicalQuery::canonicalize(opCtx,
                                         std::move(findCommand),
                                         false,
                                         expCtx,
                                         ExtensionsCallbackNoop(),
                                         MatchExpressionParser::kBanAllSpecialFeatures));
        std::vector<BSONObj> results;
        CursorId cursorId; 
        cursorId = ClusterFind::runQuery(opCtx, nss, cmdObj, *cq, ReadPreferenceSetting::get(opCtx), &results);
        if (cursorId) return true;
        return false;
    }

    // Slaves can't perform writes.
    bool slaveOk() const { return false; }

     // all grid commands are designed not to lock
    virtual bool isWriteCommandForConfigServer() const {
        return false;
    }
} geoNearSearch;

}  // namespace
}  // namespace mongo

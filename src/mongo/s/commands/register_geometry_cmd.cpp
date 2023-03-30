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

class CmdRegisterGeometry :public BasicCommand
{
public:
    CmdRegisterGeometry() :
        BasicCommand("registerGeometry") {
    }

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
            BSONObjBuilder& result) {
        int stat=0;
        std::string errmsg;
        try {
             // log()<<"cmd:"<<cmdObj<<"    -- TTTTO test driver>>>>>>>>>>>>>>>>>>";
            if (!pIndexManagerIO->IsConnected())
                pIndexManagerIO->connectMyself();
            if (!pRTreeIO->IsConnected())
                pRTreeIO->connectMyself();
            double gtype=0;
            if (cmdObj.hasField("gtype"))
               gtype = cmdObj["gtype"].numberInt();
            double srid=4326;
            if (cmdObj.hasField("srid"))
               srid = cmdObj["srid"].numberInt();
            double crstype=0;
            if (cmdObj.hasField("crs_type"))
               crstype = cmdObj["crs_type"].numberInt();
            double tolerance=0;
            if (cmdObj.hasField("tolerance"))
               tolerance = cmdObj["tolerance"].Double();

            if (cmdObj.hasField("field"))
                stat = IM.RegisterGeometry(opCtx, dbName.db(), cmdObj["collectionName"].str(), cmdObj["field"].str(), gtype,srid, crstype, tolerance);
            else
                errmsg = "Please type the field you want to build an index on";
        }
        catch (DBException& e) {
            int code = e.code();
            stringstream ss;
            ss << "exception: " << e.what();
            errmsg = ss.str();
            result.append("code", code);
        }
        if (-1 == stat)
            errmsg = "This geomrtry field has been registered.";
        if (-2 == stat)
            errmsg = "illegal gtype,it should be 0 -7.";
        if (-3 == stat)
            errmsg = "Tolerance must be greater than 0";
        if (-4 == stat)
            errmsg = "crstype must be 0, 1 or 2";
        bool ok = (stat == 1) ? true : false;
        return ok;
    }

    // Slaves can't perform writes.
    bool slaveOk() const { return false; }

   // Safer as off by default, can slowly enable as we add more tests
    virtual bool passOptions() const {
        return false;
    }

    // all grid commands are designed not to lock
    virtual bool isWriteCommandForConfigServer() const {
        return false;
    }
} registerGeometryCmd;
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

#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/commands.h"
#include "mongo/db/create_indexes_gen.h"
#include "mongo/db/timeseries/timeseries_commands_conversion_helper.h"
#include "mongo/logv2/log.h"
#include "mongo/rpc/get_status_from_command_result.h"
#include "mongo/s/cluster_commands_helpers.h"
#include "mongo/s/cluster_ddl.h"
#include "mongo/s/collection_routing_info_targeter.h"
#include "mongo/s/grid.h"
#include "mongo/s/rtree/rtree_globle.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kCommand

namespace mongo
{
    namespace
    {

        constexpr auto kRawFieldName = "raw"_sd;
        constexpr auto kWriteConcernErrorFieldName = "writeConcernError"_sd;
        constexpr auto kTopologyVersionFieldName = "topologyVersion"_sd;

        class CreateIndexesCmd : public BasicCommandWithRequestParser<CreateIndexesCmd>
        {
        public:
            using Request = CreateIndexesCommand;
            using Reply = CreateIndexesReply;

            const std::set<std::string> &apiVersions() const final
            {
                return kApiVersions1;
            }

            AllowedOnSecondary secondaryAllowed(ServiceContext *) const final
            {
                return AllowedOnSecondary::kNever;
            }

            bool adminOnly() const final
            {
                return false;
            }

            Status checkAuthForOperation(OperationContext *opCtx,
                                         const DatabaseName &dbName,
                                         const BSONObj &cmdObj) const override
            {
                auto *as = AuthorizationSession::get(opCtx->getClient());
                if (!as->isAuthorizedForActionsOnResource(parseResourcePattern(dbName.db(), cmdObj),
                                                          ActionType::createIndex))
                {
                    return {ErrorCodes::Unauthorized, "unauthorized"};
                }

                return Status::OK();
            }

            bool supportsWriteConcern(const BSONObj &cmd) const final
            {
                return true;
            }

            bool allowedInTransactions() const final
            {
                return true;
            }

            bool runWithRequestParser(OperationContext *opCtx,
                                      const DatabaseName &dbName,
                                      const BSONObj &cmdObj,
                                      const RequestParser &,
                                      BSONObjBuilder &output) final
            {
                const NamespaceString nss(CommandHelpers::parseNsCollectionRequired(dbName, cmdObj));
                LOGV2_DEBUG(22750,
                            1,
                            "createIndexes: {namespace} cmd: {command}",
                            "CMD: createIndexes",
                            "namespace"_attr = nss,
                            "command"_attr = redact(cmdObj));

                // TODO SERVER-67798 Change cluster::createDatabase to use DatabaseName
                cluster::createDatabase(opCtx, dbName.toStringWithTenantId());

                // Is the command for Rree
                bool is_rtree = false;

                std::string errmsg;
                BSONObj index = cmdObj["indexes"].Array()[0].Obj()["key"].Obj();
                // log() << "index:"<< index;
                std::set<std::string> fields;
                auto fieldNames = index.getFieldNames<std::set<std::string>>();
                // log() << "index:" << fields.size();
                for (std::set<std::string>::iterator it = fields.begin(); it != fields.end(); ++it)
                {
                    // log() << index[*it].toString(false);
                    // log() << index[*it].toString(false).compare("\"rtree\"");
                    if (index[*it].toString(false).compare("\"rtree\"") == 0)
                    {
                        auto columnname = *it;
                        // log() << columename << endl;
                        is_rtree = true;
                        return createRtreeIndex(opCtx, dbName.db(), cmdObj, columnname, errmsg, output);
                    }
                }

                auto targeter = CollectionRoutingInfoTargeter(opCtx, nss);
                auto routingInfo = targeter.getRoutingInfo();
                auto cmdToBeSent = cmdObj;
                if (targeter.timeseriesNamespaceNeedsRewrite(nss))
                {
                    cmdToBeSent = timeseries::makeTimeseriesCommand(
                        cmdToBeSent, nss, getName(), CreateIndexesCommand::kIsTimeseriesNamespaceFieldName);
                }

                auto shardResponses = scatterGatherVersionedTargetByRoutingTable(
                    opCtx,
                    nss.db(),
                    targeter.getNS(),
                    routingInfo,
                    CommandHelpers::filterCommandRequestForPassthrough(
                        applyReadWriteConcern(opCtx, this, cmdToBeSent)),
                    ReadPreferenceSetting(ReadPreference::PrimaryOnly),
                    Shard::RetryPolicy::kNoRetry,
                    BSONObj() /* query */,
                    BSONObj() /* collation */);

                std::string errmsg;
                const bool ok =
                    appendRawResponses(opCtx, &errmsg, &output, std::move(shardResponses)).responseOK;
                CommandHelpers::appendSimpleCommandStatus(output, ok, errmsg);

                if (ok)
                {
                    LOGV2(5706400, "Indexes created", "namespace"_attr = nss);
                }

                return ok;
            }

            /**
             * Response should either be "ok" and contain just 'raw' which is a dictionary of
             * CreateIndexesReply (with optional 'ok' and 'writeConcernError' fields).
             * or it should be "not ok" and contain an 'errmsg' and possibly a 'writeConcernError'.
             * 'code' & 'codeName' are permitted in either scenario, but non-zero 'code' indicates "not ok".
             */
            void validateResult(const BSONObj &result) final
            {
                auto ctx = IDLParserContext("createIndexesReply");
                if (checkIsErrorStatus(result, ctx))
                {
                    return;
                }

                StringDataSet ignorableFields({kWriteConcernErrorFieldName,
                                               ErrorReply::kOkFieldName,
                                               kTopologyVersionFieldName,
                                               kRawFieldName});
                Reply::parse(ctx, result.removeFields(ignorableFields));
                if (!result.hasField(kRawFieldName))
                {
                    return;
                }

                const auto &rawData = result[kRawFieldName];
                if (!ctx.checkAndAssertType(rawData, Object))
                {
                    return;
                }

                auto rawCtx = IDLParserContext(kRawFieldName, &ctx);
                for (const auto &element : rawData.Obj())
                {
                    if (!rawCtx.checkAndAssertType(element, Object))
                    {
                        return;
                    }

                    const auto &shardReply = element.Obj();
                    if (!checkIsErrorStatus(shardReply, ctx))
                    {
                        Reply::parse(ctx, shardReply.removeFields(ignorableFields));
                    }
                }
            }

            const AuthorizationContract *getAuthorizationContract() const final
            {
                return &::mongo::CreateIndexesCommand::kAuthorizationContract;
            }

            bool createRtreeIndex(OperationContext *opCtx,
                                  const std::string &dbName,
                                  const BSONObj &cmdObj,
                                  const std::string &columnname,
                                  std::string &errmsg,
                                  BSONObjBuilder &output)
            {
                if (!pIndexManagerIO->IsConnected())
                    pIndexManagerIO->connectMyself();
                if (!pRTreeIO->IsConnected())
                    pRTreeIO->connectMyself();
                BSONObjBuilder rtreepara;
                rtreepara.append("collectionName", cmdObj["createIndexes"].str());

                rtreepara.append("columnName", columnname);
                rtreepara.append("indextype", 1);
                BSONObj indexes = cmdObj["indexes"].Array()[0].Obj()["key"].Obj();
                indexes.hasField("maxnode") ? rtreepara.append("maxnode", indexes["maxnode"].Double()) : rtreepara.append("maxnode", 32.0);
                indexes.hasField("maxleaf") ? rtreepara.append("maxleaf", indexes["maxleaf"].Double()) : rtreepara.append("maxleaf", 32.0);
                BSONObj rtreeparaobj = rtreepara.obj();
                // std::string errmsg;
                int stat = 0;
                try
                {
                    stat = IM.PrepareIndex(opCtx, dbName, rtreeparaobj["collectionName"].str(), rtreeparaobj["columnName"].str(), rtreeparaobj["indextype"].Int(), rtreeparaobj["maxnode"].Double(), rtreeparaobj["maxleaf"].Double());
                }
                catch (DBException &e)
                {
                    int code = e.code();
                    stringstream ss;
                    ss << "exception: " << e.what();
                    errmsg = ss.str();
                    output.append("code", code);
                }
                if (-1 == stat)
                    errmsg = "The index type you assigned is not 1(rtree index).";
                if (-2 == stat)
                    errmsg = "Please run registerGeometry command before creating rtree index.";
                if (-3 == stat)
                    errmsg = "the index has already existed.";
                bool ok = (stat == 1) ? true : false;
                return ok;
            }

        } createIndexesCmd;

    } // namespace
} // namespace mongo

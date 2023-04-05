/**
 *    Copyright (C) 2015 LIESMARS, Wuhan University.
 *    Financially supported by Wuda Geoinfamatics Co. ,Ltd.
 *    Author:  Xiang Longgang, Wang Dehao , Shao Xiaotian
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

#pragma once

#include "mongo/client/connpool.h"
#include "mongo/db/operation_context.h"
#include "mongo/s/cluster_commands_helpers.h"
#include "mongo/s/collection_routing_info_targeter.h"
#include "mongo/s/grid.h"

// Originally, this function is contained in rtree_io.h, However, there always appears some link errors that several functions in rtree_io.h are redefined.
// The reason is that both transaction.cpp and commands_public.cpp reference rtree_io.h, consequently, transaction.obj and commands_public.obj both contain aforementioned functions.
// In view that Transaction.cpp solely use one function RunWriteCommand, I just extract it out of RTreeIO.h.

using namespace mongo;
using namespace std;

namespace rtree_index
{
	enum writeOpt
	{
		INSERT = 0,
		UPDATE = 1,
		REMOVE = 2,
		DROP = 3
	};
	static bool RunWriteCommand(OperationContext *opCtx, string dbName, string collname, BSONObj cmdObj, writeOpt opt, BSONObjBuilder &result)
	{

		auto status = Grid::get(opCtx)->catalogCache()->getDatabase(opCtx, dbName);
		uassertStatusOK(status.getStatus());
		auto nss = NamespaceString(dbName, collname);
		// auto targeter = CollectionRoutingInfoTargeter(opCtx, nss);
		// auto routingInfo = targeter.getRoutingInfo();

		BSONObj cmdToBeSent;
		if (opt == INSERT)
		{

			BSONObjBuilder insertObj;
			insertObj.append("insert", collname);
			BSONArrayBuilder docArr;
			docArr.append(cmdObj);
			insertObj.append("documents", docArr.arr());
			insertObj.append("ordered", true);
			cmdToBeSent = insertObj.obj();
			cmdToBeSent = CommandHelpers::appendMajorityWriteConcern(cmdToBeSent);
		}
		else if (opt == UPDATE)
		{
			BSONObjBuilder updateObj;
			updateObj.append("update", collname);
			BSONObjBuilder update;
			BSONObj filter = cmdObj.getObjectField("query");
			BSONObj upd = cmdObj.getObjectField("update");
			update.append("q", filter);
			update.append("u", upd);
			update.append("multi", false);
			update.append("upsert", false);
			BSONArrayBuilder docArr;
			docArr.append(update.obj());
			updateObj.append("updates", docArr.arr());
			updateObj.append("ordered", true);
			cmdToBeSent = updateObj.obj();
		}
		else if (opt == REMOVE)
		{
			BSONObjBuilder deleteObj;
			deleteObj.append("delete", collname);
			BSONObjBuilder deletedoc;
			deletedoc.append("q", cmdObj);
			deletedoc.append("limit", 1);
			BSONArrayBuilder docArr;
			docArr.append(deletedoc.obj());
			deleteObj.append("deletes", docArr.arr());
			deleteObj.append("ordered", true);
			cmdToBeSent = deleteObj.obj();
		}
		else if (opt == DROP)
		{
			// bool ok;
			string s = "";
			string &errmsg = s;
			BSONObjBuilder dropcmd;
			dropcmd.append("drop", collname);
			cmdToBeSent = dropcmd.obj();
		}

		std::cout << "The command to be sent: " << cmdToBeSent.toString() << "\n";

		// Force a refresh of the cached database metadata from the config server.
		// const auto swDbMetadata =
		//	Grid::get(opCtx)->catalogCache()->getDatabaseWithRefresh(opCtx, dbName);
		std::cout << "Step -1\n";
		// Next line causes SegFault, figure out why
		auto hostAndPort = repl::ReplicationCoordinator::get(opCtx)->getCurrentPrimaryHostAndPort();
		std::cout << "Step 0 \n";
		if (hostAndPort.empty())
		{
			uasserted(ErrorCodes::PrimarySteppedDown, "No primary exists currently");
		}
		std::cout << "Step 1 \n";
		auto conn = std::make_unique<ScopedDbConnection>(hostAndPort.toString());

		if (auth::isInternalAuthSet())
		{
			uassertStatusOK(conn->get()->authenticateInternalUser());
		}
		std::cout << "Step 2 \n";
		DBClientBase *client = conn->get();
		ScopeGuard guard([&]
						 { conn->done(); });
		std::cout << "Step 2 and 1/2 \n";
		try
		{
			BSONObj resObj;
			std::cout << "Step 3 \n";
			if (!client->runCommand(DatabaseName(dbName), cmdToBeSent, resObj))
			{
				uassertStatusOK(getStatusFromCommandResult(resObj));
			}
			std::cout << "Step 4 \n";
			return getStatusFromCommandResult(resObj).isOK();
		}
		catch (...)
		{
			guard.dismiss();
			conn->kill();
			throw;
		}

		// auto shardResponses = scatterGatherVersionedTargetByRoutingTable(
		// 	opCtx,
		// 	nss.db(),
		// 	targeter.getNS(),
		// 	routingInfo,
		// 	cmdToBeSent,
		// 	ReadPreferenceSetting(ReadPreference::PrimaryOnly),
		// 	Shard::RetryPolicy::kNoRetry,
		// 	BSONObj() /* query */,
		// 	BSONObj() /* collation */);

		// std::string errmsg;
		// BSONObjBuilder output;
		// const bool ok =
		// 	appendRawResponses(opCtx, &errmsg, &output, std::move(shardResponses)).responseOK;
		// std::cout << "The output: " << output.obj().toString() << "\n";
	}
}
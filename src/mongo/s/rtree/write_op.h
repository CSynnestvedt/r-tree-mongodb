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
#include "mongo/db/request_execution_context.h"
#include "mongo/rpc/factory.h"
#include "mongo/s/chunk_manager.h"
#include "mongo/s/cluster_commands_helpers.h"
#include "mongo/s/cluster_ddl.h"
#include "mongo/s/collection_routing_info_targeter.h"
#include "mongo/s/grid.h"
#include "mongo/s/commands/strategy.h"


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
		auto nss = NamespaceString(dbName, collname);

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

		auto dbStatus = Grid::get(opCtx)->catalogCache()->getDatabase(opCtx, dbName).getValue();
		ShardId shardId = dbStatus->getPrimary();

		cmdToBeSent = CommandHelpers::appendMajorityWriteConcern(cmdToBeSent);
		cmdToBeSent = appendDbVersionIfPresent(cmdToBeSent, dbStatus);
		cmdToBeSent = appendShardVersion(cmdToBeSent, ShardVersion::UNSHARDED());

		std::vector<AsyncRequestsSender::Request> requests;
		requests.emplace_back(std::move(shardId), cmdToBeSent);


		auto responses =
        gatherResponses(opCtx,
                        dbName,
                        ReadPreferenceSetting(ReadPreference::PrimaryOnly),
        				Shard::RetryPolicy::kIdempotent,
                        requests);
		auto status = responses.front().swResponse.getStatus();
		std::cout << "Status write op in write_op.h: " << status.toString() << "\n";
		return status.isOK();
	}
}
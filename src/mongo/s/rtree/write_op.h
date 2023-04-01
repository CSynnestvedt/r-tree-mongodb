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

#include "mongo/db/operation_context.h"
#include "mongo/db/dbdirectclient.h"


//Originally, this function is contained in rtree_io.h, However, there always appears some link errors that several functions in rtree_io.h are redefined.
//The reason is that both transaction.cpp and commands_public.cpp reference rtree_io.h, consequently, transaction.obj and commands_public.obj both contain aforementioned functions.
//In view that Transaction.cpp solely use one function RunWriteCommand, I just extract it out of RTreeIO.h.

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
	static bool RunWriteCommand(OperationContext* txn,string dbname, string collname, BSONObj cmdObj, writeOpt opt, BSONObjBuilder& result)
	{
		bool ok = false;
		auto nss = NamespaceString(dbname + "." + collname);
		DBDirectClient dbClient(txn);
		
		if (opt == INSERT)
		{
			BSONObjBuilder insertObj;
			insertObj.append("insert", collname);
			BSONArrayBuilder docArr;
			docArr.append(cmdObj);
			insertObj.append("documents", docArr.arr());
			insertObj.append("ordered", true);

			auto request = OpMsgRequest::fromDBAndBody(nss.db(), insertObj.obj());

			rpc::UniqueReply reply = dbClient.runCommand(std::move(request));
			auto insertStatus = getStatusFromWriteCommandReply(reply->getCommandReply());
			/*insert*/
			// ok = c->run(txn,dbname, objRef, 0, errmsg, result);
			
			return insertStatus.isOK();
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

			auto request = OpMsgRequest::fromDBAndBody(nss.db(), updateObj.obj());
			rpc::UniqueReply reply = dbClient.runCommand(std::move(request));
			auto insertStatus = getStatusFromCommandResult(reply->getCommandReply());

			return insertStatus.isOK();
		}
		else if (opt==REMOVE)
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
			
			auto request = OpMsgRequest::fromDBAndBody(nss.db(), deleteObj.obj());
			rpc::UniqueReply reply = dbClient.runCommand(std::move(request));
			auto insertStatus = getStatusFromCommandResult(reply->getCommandReply());
			return insertStatus.isOK();
		}
		else if (opt == DROP)
		{
			// bool ok;
			string s="";
			string  & errmsg=s;
			BSONObjBuilder dropcmd;
			dropcmd.append("drop", collname);

			auto request = OpMsgRequest::fromDBAndBody(nss.db(), dropcmd.obj());
			rpc::UniqueReply reply = dbClient.runCommand(std::move(request));
			auto insertStatus = getStatusFromCommandResult(reply->getCommandReply());
			return insertStatus.isOK();
		}
		return ok;
	}
}
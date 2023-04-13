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

#include "index_manager_core.h"
#include "mongo/db/operation_context.h"
#include "mongo/s/commands/shard_collection_gen.h"

#include <iostream>
using namespace std;
namespace index_manager
{

	bool nearcompare(KeywithDis o1, KeywithDis o2)
	{
		if (o1.distance < o2.distance)
		{
			return true;
		}
		else
		{
			return false;
		}
	}

	IndexManagerBase::IndexManagerBase()
	{
	}

	IndexManagerBase::IndexManagerBase(MongoIndexManagerIO *userIndexManagerIO, MongoIO *userRtreeIO)
	{
		_IO = userIndexManagerIO;
		_RtreeIO = userRtreeIO;
		_Rtree.IO = _RtreeIO;
	}

	int IndexManagerBase::RegisterGeometry(OperationContext *opCtx, string dbName, string collectionName, string columnName, int sdoGType, int sdoSRID, int crsType, double tolerance)
	{

		if (_IO->basicGeoMetadataExists(opCtx, dbName, collectionName))
		{
			return -1;
		}

		if (sdoGType < 0 || sdoGType > 7)
		{
			return -2;
		}
		if (tolerance < 0)
		{
			return -3;
		}
		if (crsType < 0 || crsType > 2)
		{
			return -4;
		}

		if (_IO->basicInsertGeoMetadata(opCtx, dbName, collectionName, columnName, 0, MBR(0, 0, 0, 0), sdoGType, sdoSRID, crsType, tolerance))
		{

			return 1;
		}
		return 0;
	}

	int IndexManagerBase::PrepareIndex(OperationContext *opCtx, string dbName, string collectionName, string columnName, int indexType, int maxNode, int maxLeaf)
	{
		// Creating Rtree spatial collection
		BSONObjBuilder bob;
		bob.append("create", "rtree_" + collectionName);
		BSONObj cmdToSend = bob.obj();
		cmdToSend = CommandHelpers::appendMajorityWriteConcern(cmdToSend);
		auto dbStatus = Grid::get(opCtx)->catalogCache()->getDatabase(opCtx, dbName).getValue();
		ShardId shardId = dbStatus->getPrimary();
		appendDbVersionIfPresent(cmdToSend, dbStatus);
		appendShardVersion(cmdToSend, ShardVersion::UNSHARDED());

		std::vector<AsyncRequestsSender::Request> requests;
		requests.emplace_back(std::move(shardId), cmdToSend);
		// std::cout<<"NS in Operation context: (PrepareIndex)"<<opCtx->getNS()<<endl;

		std::cout << "Creating collection " << dbName << ".rtree_" << collectionName << "\n";
		auto responses =
			gatherResponses(opCtx,
							dbName,
							ReadPreferenceSetting(ReadPreference::PrimaryOnly),
							Shard::RetryPolicy::kIdempotent,
							requests);
		auto status = responses.front().swResponse.getStatus();
		std::cout << "Status from rtree collection: " << status.toString() << "\n";
		std::cout << "The data from swResponse rtree: " << responses.front().swResponse.getValue().data.toString() << "\n";
		auto catalogCache = Grid::get(opCtx)->catalogCache();
		catalogCache->invalidateShardOrEntireCollectionEntryForShardedCollection(
			NamespaceString(dbName, "rtree_" + collectionName), boost::none, shardId);

		// Creating system.buckets.rtree_SC collection
		BSONObjBuilder bobBucket;
		bobBucket.append("create", "system.buckets.rtree_" + collectionName);
		BSONObj cmdToSendBucket = bobBucket.obj();
		dbStatus = Grid::get(opCtx)->catalogCache()->getDatabase(opCtx, dbName).getValue();
		shardId = dbStatus->getPrimary();
		cmdToSendBucket = CommandHelpers::appendMajorityWriteConcern(cmdToSendBucket);
		appendDbVersionIfPresent(cmdToSendBucket, dbStatus);
		appendShardVersion(cmdToSendBucket, ShardVersion::UNSHARDED());

		std::vector<AsyncRequestsSender::Request> requestsBucket;
		requestsBucket.emplace_back(std::move(shardId), cmdToSendBucket);

		std::cout << "Creating collection " << dbName << "system.buckets.rtree_" << collectionName << "\n";
		auto responsesBucket =
			gatherResponses(opCtx,
							dbName,
							ReadPreferenceSetting(ReadPreference::PrimaryOnly),
							Shard::RetryPolicy::kIdempotent,
							requestsBucket);
		catalogCache->invalidateShardOrEntireCollectionEntryForShardedCollection(
			NamespaceString(dbName, "system.buckets.rtree_" + collectionName), boost::none, shardId);
		auto bucketStatus = responsesBucket.front().swResponse.getStatus();
		std::cout << "Status from bucket collection: " << bucketStatus.toString() << "\n";

		if (indexType <= 0 || indexType > 2)
		{
			return -1;
		}
		if (!_IO->basicGeoMetadataExists(opCtx, dbName, collectionName))
		{
			return -2;
		}
		else
		{
			if (_IO->rtreeIndexExists(opCtx, dbName, collectionName))
			{
				return -3;
			}
			if (indexType == 1)
			{

				std::cout << "Inside IndexManager PrepareIndex with db: " << dbName << "\n";
				Transaction *t = new CreateIndexTransaction(opCtx, dbName);
				mongo::OID tempkey;
				_IO->rtreeInsertIndexMetaData(t, collectionName, maxNode, maxLeaf, tempkey);
				mongo::OID nullKey;
				_Rtree.ReConfigure(maxNode, maxLeaf, nullKey, dbName, collectionName);

				mongo::OID Root;
				_Rtree.InsertRoot(opCtx, Root);
				t->InsertDone(3, "rtree_" + collectionName, rtree_index::INSERT, "InsertRoot");

				std::cout << "\nWe made it past the insert root stage before segfaulting like a boss\n";
				_IO->basicInitStorageTraverse(opCtx, dbName, collectionName);
				t->UpdateDone(4, "rtree_" + collectionName, rtree_index::UPDATE, "begin building Rtree index on existing data");
				std::cout << "\nWe made it past the init storage traverse and update done\n";
				mongo::OID oneKey;
				MBR oneMBR;
				int count = 0;

				while (_IO->basicStorageTraverseNext(oneMBR, oneKey))
				{
					Branch b;
					b.ChildKey = oneKey;
					b.HasData = true;
					b.mbr = oneMBR;
					_Rtree.Insert(opCtx, Root, b, 0);
					if (count % 100 == 0)
						std::cout << count << "  " << Root << "\r";
					count++;
				}
				std::cout << "\nWe made it past the storage traversal loop\n";
				t->UpdateDone(5, "rtree_" + collectionName, rtree_index::UPDATE, "finish building Rtree index on existing data");

				_IO->rteeModifyRootKey(opCtx, dbName, collectionName, Root);
				std::cout << "\nWe made it past last rtreeModifyRootKey\n";
				t->UpdateDone(6, "config.meta_geom", rtree_index::UPDATE, "Update Root Key in meta_geom");
				delete t;
				std::cout << "\nWe made it all the way to the end\n";
				return 1;
			}
			else
			{
				return -4;
			}
			return 0;
		}
		return false;
	}

	/*
	   1: OK
	   0: Something wrong
	*/

	int IndexManagerBase::ValidateGeometry(OperationContext *opCtx, string dbName, string collectionName)
	{
		_IO->basicInitStorageTraverse(opCtx, dbName, collectionName);
		mongo::OID oneKey;
		MBR oneMBR;

		// Easy Verify,Robust Verify please use mongo:: GeoJSON parser
		while (true)
		{
			int flag = _IO->basicStorageTraverseNext(oneMBR, oneKey);
			if (flag == 1)
			{
				continue;
			}
			if (flag == 0)
			{
				break;
			}
			if (flag == -1)
			{
				return 0;
			}
		}
		return 1;
	}

	std::unique_ptr<RTreeRangeQueryCursor> IndexManagerBase::GeoSearchWithin(OperationContext *opCtx, string dbName, string collectionName, mongo::BSONObj InputGeometry)
	{
		vector<mongo::OID> ResultKeys;
		vector<bool> lazyIntersects;
		vector<mongo::OID> RefinedResultKeys;
		int index_type = _IO->basicGetIndexType(opCtx, dbName, collectionName);
		// log()<<"index_type:"<<index_type;
		if (index_type > 0)
		{
			if (index_type == 1)
			{
				mongo::OID RootKey;
				int maxNode = 0;
				int maxLeaf = 0;
				string cn;
				if (_IO->rtreeSetInputParamsIfExists(opCtx, dbName, collectionName, RootKey, maxNode, maxLeaf, cn))
				{
					// log()<<maxNode<<","<<maxLeaf;
					_Rtree.ReConfigure(maxNode, maxLeaf, RootKey, dbName, collectionName);
					// parseGeometry
					geos::geom::Geometry *pGeometry = NULL;
					if (_IO->parseGeometry(InputGeometry, pGeometry)) // parseSuccess
					{
						Node RootNode = _RtreeIO->Basic_Find_One_Node(RootKey);
						std::unique_ptr<RTreeRangeQueryCursor> returnCursor(new RTreeRangeQueryCursor(maxNode, RootNode, _RtreeIO, _IO, pGeometry, dbName, collectionName, cn, 0));
						returnCursor->InitCursor();
						return std::move(returnCursor);
					}
					delete pGeometry; // free the memory of geos geos geometry
				}
			}
		}
		std::unique_ptr<RTreeRangeQueryCursor> returnCursor;
		return returnCursor;
	}

	bool IndexManagerBase::GeoSearchWithinWithoutRefining(OperationContext *opCtx, string dbName, string collectionName, mongo::BSONObj InputGeometry, vector<mongo::OID> &results)
	{
		vector<bool> lazyIntersects;
		int index_type = _IO->basicGetIndexType(opCtx, dbName, collectionName);
		// log()<<"IndexType:"<<index_type;
		// log()<<"filter:"<<InputGeometry;
		if (index_type > 0)
		{
			if (index_type == 1)
			{
				mongo::OID RootKey;
				int maxNode = 0;
				int maxLeaf = 0;
				string cn;
				if (_IO->rtreeSetInputParamsIfExists(opCtx, dbName, collectionName, RootKey, maxNode, maxLeaf, cn))
				{
					// log()<<maxNode<<","<<maxLeaf;
					_Rtree.ReConfigure(maxNode, maxLeaf, RootKey, dbName, collectionName);
					// parseGeometry
					geos::geom::Geometry *pGeometry = NULL;
					if (_IO->parseGeometry(InputGeometry, pGeometry)) // parseSuccess
					{
						if (pGeometry->getGeometryType() == "MultiPolygon" || pGeometry->getGeometryType() == "Polygon")
						{
							_Rtree.Search(pGeometry, results, lazyIntersects);
						}
						else
						{
							cout << "$GeoWithIn only supports \"Polygon\" and \"MutiPolygon\"" << endl;
							return false;
						}
					}
					delete pGeometry; // free the memory of geos geos geometry
				}
			}
		}
		// RTreeCursor<Key> returnCursor(RefinedResultKeys, dbName, collectionName);
		return true;
	}

	std::unique_ptr<RTreeRangeQueryCursor> IndexManagerBase::GeoSearchIntersects(OperationContext *opCtx, string dbName, string collectionName, mongo::BSONObj InputGeometry)
	{
		vector<mongo::OID> ResultKeys;
		vector<bool> lazyIntersects;
		vector<mongo::OID> RefinedResultKeys;
		int index_type = _IO->basicGetIndexType(opCtx, dbName, collectionName);
		// log()<<"index_type:"<<index_type;
		if (index_type > 0)
		{
			if (index_type == 1)
			{
				mongo::OID RootKey;
				int maxNode = 0;
				int maxLeaf = 0;
				string cn;
				if (_IO->rtreeSetInputParamsIfExists(opCtx, dbName, collectionName, RootKey, maxNode, maxLeaf, cn))
				{
					// log()<<maxNode<<","<<maxLeaf;
					_Rtree.ReConfigure(maxNode, maxLeaf, RootKey, dbName, collectionName);
					// parseGeometry
					geos::geom::Geometry *pGeometry = NULL;
					if (_IO->parseGeometry(InputGeometry, pGeometry)) // parseSuccess
					{
						Node RootNode = _RtreeIO->Basic_Find_One_Node(RootKey);
						std::unique_ptr<RTreeRangeQueryCursor> returnCursor(new RTreeRangeQueryCursor(maxNode, RootNode, _RtreeIO, _IO, pGeometry, dbName, collectionName, cn, 1));
						returnCursor->InitCursor();
						return std::move(returnCursor);
					}
					/*
					 *  Please Note We should delete pGeometry
					 *  After finshing using the RTreeRangeQueryCursor
					 *  use RTreeRangeQueryCursor->FreeCursor() when  cursor is exhausted
					 */
				}
			}
		}
		std::unique_ptr<RTreeRangeQueryCursor> returnCursor;
		return returnCursor;
	}

	std::unique_ptr<RTreeGeoNearCursor> IndexManagerBase::GeoSearchNear(OperationContext *opCtx, string dbName, string collectionName, double ctx, double cty, double rMin, double rMax)
	{
		vector<mongo::OID> ResultKeys;
		vector<bool> lazyIntersects;
		vector<mongo::OID> RefinedResultKeys;
		vector<KeywithDis> toBeSort;
		int index_type = _IO->basicGetIndexType(opCtx, dbName, collectionName);
		if (index_type > 0)
		{
			if (index_type == 1)
			{
				mongo::OID RootKey;
				int maxNode = 0;
				int maxLeaf = 0;
				string cn;
				if (_IO->rtreeSetInputParamsIfExists(opCtx, dbName, collectionName, RootKey, maxNode, maxLeaf, cn))
				{
					_RtreeIO->Configure(dbName, collectionName, maxNode, maxLeaf);
					Node rootNode = _RtreeIO->Basic_Find_One_Node(RootKey);
					GeoNearSearchNode *RootN = new GeoNearSearchNode(0, 9999999, 111, rootNode);
					std::unique_ptr<RTreeGeoNearCursor> TestCursor(new RTreeGeoNearCursor(maxNode, RootN, _RtreeIO, _IO, ctx, cty, rMin, rMax, dbName, collectionName, cn));
					TestCursor->InitCursor();
					return std::move(TestCursor);
				}
			}
		}
		std::unique_ptr<RTreeGeoNearCursor> returnCursor;
		return returnCursor;
	}

	int IndexManagerBase::DeleteGeoObjByKey(OperationContext *opCtx, string dbName, string collectionName, mongo::OID key2delete)
	{
		mongo::OID RootKey;
		int maxNode, maxLeaf;
		maxNode = 0;
		maxLeaf = 0;
		string cn;
		_IO->rtreeSetInputParamsIfExists(opCtx, dbName, collectionName, RootKey, maxNode, maxLeaf, cn);
		_Rtree.ReConfigure(maxNode, maxLeaf, RootKey, dbName, collectionName);
		MBR m;
		_IO->rtreeSetDataMBR(dbName, collectionName, m, key2delete, cn);
		_Rtree.DeleteNode(opCtx, RootKey, key2delete, m);
		_IO->rteeModifyRootKey(opCtx, dbName, collectionName, RootKey);
		_IO->basicDeleteNodeById(opCtx, dbName, collectionName, key2delete);

		return 1;
	}

	int IndexManagerBase::DeleteIntersectedGeoObj(OperationContext *opCtx, string dbName, string collectionName, mongo::BSONObj InputGeometry)
	{
		int index_type = _IO->basicGetIndexType(opCtx, dbName, collectionName);
		vector<mongo::OID> RefinedResultKeys;
		if (index_type > 0)
		{
			if (index_type == 1)
			{
				vector<mongo::OID> ResultKeys;
				vector<bool> lazyIntersects;
				mongo::OID RootKey;
				string cn;
				int maxNode = 0;
				int maxLeaf = 0;
				if (_IO->rtreeSetInputParamsIfExists(opCtx, dbName, collectionName, RootKey, maxNode, maxLeaf, cn))
				{
					// parseGeometry
					geos::geom::Geometry *pGeometry = NULL;
					if (_IO->parseGeometry(InputGeometry, pGeometry)) // parseSuccess
					{
						_Rtree.ReConfigure(maxNode, maxLeaf, RootKey, dbName, collectionName);
						_Rtree.Search(pGeometry, ResultKeys, lazyIntersects);
						for (unsigned int i = 0; i < ResultKeys.size(); i++)
						{
							if (_IO->geoVerifyIntersect(ResultKeys[i], pGeometry, lazyIntersects[i], dbName, collectionName, cn))
							{
								RefinedResultKeys.push_back(ResultKeys[i]);
							}
						}
						for (unsigned int i = 0; i < RefinedResultKeys.size(); i++)
						{
							MBR datambr;
							_IO->rtreeSetDataMBR(dbName, collectionName, datambr, RefinedResultKeys[i], cn);
							_Rtree.DeleteNode(opCtx, RootKey, RefinedResultKeys[i], datambr);
							_IO->rteeModifyRootKey(opCtx, dbName, collectionName, RootKey);
							_IO->basicDeleteNodeById(opCtx, dbName, collectionName, RefinedResultKeys[i]);
						}
					}
				}
			}
			return 1;
		}
		return 0;
	}

	int IndexManagerBase::DeleteContainedGeoObj(OperationContext *opCtx, string dbName, string collectionName, mongo::BSONObj InputGeometry)
	{
		// log()<<"DeleteContainedGeoObj";
		int index_type = _IO->basicGetIndexType(opCtx, dbName, collectionName);
		// log()<<"index_type:"<<index_type;
		vector<mongo::OID> RefinedResultKeys;
		if (index_type > 0)
		{
			if (index_type == 1)
			{
				vector<mongo::OID> ResultKeys;
				vector<bool> lazyIntersects;
				mongo::OID RootKey;
				string cn;
				int maxNode = 0;
				int maxLeaf = 0;
				if (_IO->rtreeSetInputParamsIfExists(opCtx, dbName, collectionName, RootKey, maxNode, maxLeaf, cn))
				{
					// parseGeometry
					geos::geom::Geometry *pGeometry = NULL;
					if (_IO->parseGeometry(InputGeometry, pGeometry)) // parseSuccess
					{
						if (pGeometry->getGeometryType() == "MultiPolygon" || pGeometry->getGeometryType() == "Polygon")
						{
							_Rtree.ReConfigure(maxNode, maxLeaf, RootKey, dbName, collectionName);
							_Rtree.Search(pGeometry, ResultKeys, lazyIntersects);
							for (unsigned int i = 0; i < ResultKeys.size(); i++)
							{
								if (_IO->geoVerifyContains(ResultKeys[i], pGeometry, lazyIntersects[i], dbName, collectionName, cn))
								{
									RefinedResultKeys.push_back(ResultKeys[i]);
								}
							}
							for (unsigned int i = 0; i < RefinedResultKeys.size(); i++)
							{
								MBR datambr;
								_IO->rtreeSetDataMBR(dbName, collectionName, datambr, RefinedResultKeys[i], cn);
								_Rtree.DeleteNode(opCtx, RootKey, RefinedResultKeys[i], datambr);
								_IO->rteeModifyRootKey(opCtx, dbName, collectionName, RootKey);
								_IO->basicDeleteNodeById(opCtx, dbName, collectionName, RefinedResultKeys[i]);
							}
						}
					}
				}
			}
			return 1;
		}
		return 0;
	}

	int IndexManagerBase::DropIndex(OperationContext *opCtx, string dbName, string COLLECTIONNAME)
	{
		int index_type = _IO->basicGetIndexType(opCtx, dbName, COLLECTIONNAME);
		if (index_type <= 0)
			return 0;
		if (_IO->rtreeDeleteIndex(opCtx, dbName, COLLECTIONNAME))
			return 1;
		return 0;
	}

	/*
		0: no index found, drop collection directly
		1: OK
		-1: unknown reason
	*/

	int IndexManagerBase::DropCollection(OperationContext *opCtx, string dbName, string COLLECTIONNAME)
	{
		int index_type = _IO->basicGetIndexType(opCtx, dbName, COLLECTIONNAME);
		if (index_type <= 0)
		{
			_IO->basicDropStorage(opCtx, dbName, COLLECTIONNAME);
			_IO->basicDeleteGeoMetadata(opCtx, dbName, COLLECTIONNAME);
			return 0;
		}
		else
		{
			_IO->rtreeDeleteIndex(opCtx, dbName, COLLECTIONNAME);
			_IO->basicDropStorage(opCtx, dbName, COLLECTIONNAME);
			_IO->basicDeleteGeoMetadata(opCtx, dbName, COLLECTIONNAME);
			return 1;
		}
		return -1;
	}

	int IndexManagerBase::RepairIndex(string dbName, string collectionName)
	{

		return 1;
	}

	/*
	  -1:unsupported index type
	  0:faild because of meteData problem
	  1:success
	*/

	int IndexManagerBase::InsertIndexedDoc(OperationContext *opCtx, string dbName, string collectionName, mongo::BSONObj AtomData, BSONObjBuilder &result)
	{
		std::cout << "We made it in into InsertIndexedDoc \n";
		int index_type = _IO->basicGetIndexType(opCtx, dbName, collectionName);
		if (index_type == 1)
		{
			mongo::OID RootKey;
			int maxNode = 0;
			int maxLeaf = 0;
			string cn;
			if (_IO->rtreeSetInputParamsIfExists(opCtx, dbName, collectionName, RootKey, maxNode, maxLeaf, cn))
			{
				_Rtree.ReConfigure(maxNode, maxLeaf, RootKey, dbName, collectionName);

				MBR m;
				if (_IO->rtreeSetDataMBR(AtomData, cn, m))
				{
					// store Data 1st because data is more important
					mongo::OID dataKeyAI = AtomData["_id"].OID();
					// log() << "Data2Insert:"<<AtomData << endl;
					// _IO->basicInsertOneNode(opCtx,dbName, collectionName, AtomData, dataKeyAI, result);
					Branch b;
					b.ChildKey = dataKeyAI;
					b.HasData = true;
					b.mbr = m;
					_Rtree.Insert(opCtx, RootKey, b, 0);

					_IO->rteeModifyRootKey(opCtx, dbName, collectionName, RootKey);
					return 1;
				}
			}
			return 0;
		}
		return -1;
	}

	bool IndexManagerBase::InitalizeManager(MongoIndexManagerIO *userIndexManagerIO, MongoIO *userRtreeIO)
	{
		_IO = userIndexManagerIO;
		_RtreeIO = userRtreeIO;
		_Rtree.IO = _RtreeIO;
		return true;
	}

}
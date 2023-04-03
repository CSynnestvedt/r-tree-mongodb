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

#include "index_manager_io.h"
#include "mongo/s/catalog/type_indexmetadata.h"
#include "mongo/s/catalog/type_geometadata.h"
#include "write_op.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/grid.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault
#include "mongo/logv2/log.h"

#include <iostream>

using namespace std;
using namespace rtree_index;
using namespace mongo;
using namespace geometry_parser;

namespace index_manager
{
	stdx::mutex _INDEXMANAGERIO_mu;

	MongoIndexManagerIO::MongoIndexManagerIO(DBClientBase *USER_CONN)
	{
		_conn = 0;
		//_conn = USER_CONN;
	}

	bool MongoIndexManagerIO::connectMyself()
	{
		std::string errmsg;
		string url = "localhost:" + boost::lexical_cast<string>(serverGlobalParams.port);
		ConnectionString cs = ConnectionString::parse(url).getValue();
		// ConnectionString cs(ConnectionString::parse(connectionString));
		if (!cs.isValid())
		{
			cout << "error parsing url: " << errmsg << endl;
			return false;
		}
		// std::string logMessage = "see what the connection string is: " + std::to_string(cs) + "\n"
		// LOGV2(40001, logMessage);
		_conn = cs.connect(errmsg).getValue().get();
		if (!_conn)
		{
			cout << "couldn't connect: " << errmsg << endl;
			return false;
		}

		return true;
	}

	bool MongoIndexManagerIO::isConnected()
	{
		if (_conn != 0 && this->_conn->isStillConnected())
			return true;
		else
			return false;
	}

	mongo::OID MongoIndexManagerIO::Basic_Generate_Key()
	{
		return mongo::OID::gen();
	}

	mongo::BSONObj MongoIndexManagerIO::basicFindNodeById(string dbName, string storageName, mongo::OID Data2Fetch)
	{
		/*findOne*/
		BSONObjBuilder bdr;
		bdr.append("_id", Data2Fetch);
		/*
		 * please note that this _conn for indexmanagerIO
		 * is shared by many threads. just like RTreeIO _conn
		 * so lock it 1st
		 */
		BSONObj returnBSONOBJ;
		{
			stdx::lock_guard<stdx::mutex> CONN_lock(_INDEXMANAGERIO_mu);
			if (!isConnected())
			{
				connectMyself();
			}
			NamespaceString ns = NamespaceString(dbName + "." + storageName);
			returnBSONOBJ = _conn->findOne(ns, bdr.obj());
		}
		return returnBSONOBJ;
	}

	bool MongoIndexManagerIO::basicInsertOneNode(OperationContext *opCtx, string dbName, string storageName, mongo::BSONObj atomData, mongo::OID &AtomKey, BSONObjBuilder &result)
	{
		BSONObjBuilder bdr;
		AtomKey = atomData["_id"].OID();
		// bdr.append("_id", AtomKey);
		bdr.appendElements(atomData);
		/*insert*/
		bool ok;
		ok = RunWriteCommand(opCtx, dbName, storageName, bdr.obj(), INSERT, result);
		//_conn->insert(dbName + "." + storageName, atomData);
		return 1;
	}

	bool MongoIndexManagerIO::basicDropStorage(OperationContext *opCtx, string dbName, string storageName)
	{
		/*drop*/
		BSONObj empty;
		BSONObjBuilder bdr;
		BSONObjBuilder &bdrRef = bdr;
		return RunWriteCommand(opCtx, dbName, storageName, empty, DROP, bdrRef);
	}

	int MongoIndexManagerIO::basicDeleteNodeById(OperationContext *opCtx, string dbName, string storageName, mongo::OID key2delete)
	{
		BSONObjBuilder bdr;
		bdr.append("_id", key2delete);
		/*remove*/
		//_conn->remove(_dbName + "." + storageName, bdr.obj());
		bool ok;

		BSONObjBuilder newBdr;
		BSONObjBuilder &newBdrRef = newBdr;
		ok = RunWriteCommand(opCtx, dbName, storageName, bdr.obj(), REMOVE, newBdrRef);
		return 1;
	}

	int MongoIndexManagerIO::basicGetIndexType(OperationContext *opCtx, string dbName, string storageName)
	{
		BSONObjBuilder bdr;
		bdr.append("datanamespace", dbName + "." + storageName);

		/*findOne*/
		// auto status = grid.catalogCache()->getDatabase(opCtx, dbName);
		auto status = Grid::get(opCtx)->catalogCache()->getDatabase(opCtx, dbName);
		uassertStatusOK(status.getStatus());

		// DBConfigPtr conf = grid.getDBConfig(dbName, false);
		BSONObj oneGeoMeta = Grid::get(opCtx)->shardRegistry()->getGeometry(opCtx, bdr.obj());

		if (oneGeoMeta.isEmpty())
		{
			return -1;
		}
		else
		{
			int index_type = oneGeoMeta["index_type"].Int();
			return index_type;
		}
		return -1;
	}

	bool MongoIndexManagerIO::basicStorageExists(string dbName, string storageName)
	{
		/*EXIST*/
		stdx::lock_guard<stdx::mutex> CONN_lock(_INDEXMANAGERIO_mu);
		if (!isConnected())
		{
			connectMyself();
		}
		if (_conn->exists(dbName + "." + storageName))
		{
			return true;
		}
		else
		{
			return false;
		}
	}

	int MongoIndexManagerIO::basicInsertGeoMetadata(OperationContext *opCtx, string dbName, string storageName, string columnName, int INDEX_TYPE, MBR m, int GTYPE, int SRID, int CRS_TYPE, double TOLERANCE)
	{
		BSONObjBuilder bdr;
		bdr.append("datanamespace", dbName + "." + storageName);
		bdr.append("column_name", columnName);
		bdr.append("index_type", INDEX_TYPE);
		OID tempOID;
		bdr.append("index_info", tempOID);
		BSONArrayBuilder mbrbdr;
		mbrbdr.append(m.MinX);
		mbrbdr.append(m.MinY);
		mbrbdr.append(m.MaxX);
		mbrbdr.append(m.MaxY);
		bdr.append("mbr", mbrbdr.arr());
		bdr.append("gtype", GTYPE);
		bdr.append("srid", SRID);
		bdr.append("crs_type", CRS_TYPE);
		bdr.append("tolerance", TOLERANCE);

		// insert
		auto status = Grid::get(opCtx)->catalogCache()->getDatabase(opCtx, dbName);
		uassertStatusOK(status.getStatus());
		bool result = Grid::get(opCtx)->shardRegistry()->registerGeometry(opCtx, bdr.obj());
		return result;
	}

	int MongoIndexManagerIO::basicDeleteGeoMetadata(OperationContext *opCtx, string dbName, string storageName)
	{
		BSONObjBuilder querybdr;
		querybdr.append("datanamespace", dbName + "." + storageName);
		// auto status = grid.catalogCache()->getDatabase(opCtx, dbName);
		auto status = Grid::get(opCtx)->catalogCache()->getDatabase(opCtx, dbName);
		uassertStatusOK(status.getStatus());
		Grid::get(opCtx)->shardRegistry()->deleteGeometry(opCtx, querybdr.obj());
		// shared_ptr<DBConfig> conf = status.getValue();
		// DBConfigPtr conf = grid.getDBConfig(dbName, false);
		// conf->deleteGeometry(opCtx,querybdr.obj());
		return 1;
	}

	int MongoIndexManagerIO::basicModifyIndexType(OperationContext *opCtx, string dbName, string storageName, int Type2Modify)
	{
		BSONObjBuilder condition;
		condition.append("datanamespace", dbName + "." + storageName);
		BSONObjBuilder setBuilder;
		BSONObjBuilder setConditionBuilder;
		setConditionBuilder.append("index_type", Type2Modify);
		setBuilder.append("$set", setConditionBuilder.obj());

		// BSONObjBuilder cmdObj;
		// cmdObj.append("query", condition.obj());
		// cmdObj.append("update", setBuilder.obj());
		// bool ok;
		// ok = RunWriteCommand(dbName, GeoMeteDataName, cmdObj.obj(), UPDATE);
		// auto status = grid.catalogCache()->getDatabase(opCtx, dbName);
		auto status = Grid::get(opCtx)->catalogCache()->getDatabase(opCtx, dbName);
		uassertStatusOK(status.getStatus());
		auto conf = Grid::get(opCtx)->shardRegistry();
		conf->updateGeometry(opCtx, condition.obj(), setBuilder.obj());
		/*updata*/
		//_conn->update(_dbName + "." + GeoMeteDataName, Query(condition.obj()), setBuilder.obj());
		if (Type2Modify == 0)
		{
			BSONObjBuilder condition1;
			condition1.append("datanamespace", dbName + "." + storageName);
			BSONObjBuilder setBuilder1;
			BSONObjBuilder setConditionBuilder1;
			mongo::OID nullOID;
			setConditionBuilder1.append("index_info", nullOID);
			setBuilder1.append("$set", setConditionBuilder1.obj());

			/*updata*/
			conf->updateGeometry(opCtx, condition.obj(), setBuilder1.obj());
		}

		return 1;
	}

	int MongoIndexManagerIO::basicModifyIndexMetadataKey(OperationContext *opCtx, string dbName, string storageName, mongo::OID Key2Modify)
	{

		BSONObjBuilder condition;
		condition.append("datanamespace", dbName + "." + storageName);
		BSONObjBuilder setBuilder;
		BSONObjBuilder setConditionBuilder;
		setConditionBuilder.append("index_info", Key2Modify);
		setBuilder.append("$set", setConditionBuilder.obj());
		// BSONObjBuilder cmdObj;
		// cmdObj.append("query", condition.obj());
		// cmdObj.append("update", setBuilder.obj());
		// bool ok;rtreeInsertIndexMetaData
		// ok = RunWriteCommand(dbName, GeoMeteDataName, cmdObj.obj(), UPDATE);
		// auto status = grid.catalogCache()->getDatabase(opCtx, dbName);
		auto status = Grid::get(opCtx)->catalogCache()->getDatabase(opCtx, dbName);
		uassertStatusOK(status.getStatus());
		Grid::get(opCtx)->shardRegistry()->updateGeometry(opCtx, condition.obj(), setBuilder.obj());

		// shared_ptr<DBConfig> conf = status.getValue();
		// conf->updateGeometry(opCtx,condition.obj(), setBuilder.obj());
		/*updata*/
		//_conn->update(_dbName + "." + GeoMeteDataName, Query(condition.obj()), setBuilder.obj());

		return 1;
	}

	bool MongoIndexManagerIO::basicGeoMetadataExists(OperationContext *opCtx, string dbName, string storageName)
	{
		BSONObjBuilder bdr;
		bdr.append("datanamespace", dbName + "." + storageName);
		;
		auto status = Grid::get(opCtx)->catalogCache()->getDatabase(opCtx, dbName);
		uassertStatusOK(status.getStatus());

		return Grid::get(opCtx)->shardRegistry()->checkGeoExist(opCtx, bdr.obj());
	}

	int MongoIndexManagerIO::rtreeInsertIndexMetaData(Transaction *t, string storageName, int MAX_NODE, int MAX_LEAF, mongo::OID RootKey)
	{
		BSONObjBuilder bdr;
		mongo::OID Index_INFO_OID = OID::gen();
		bdr.append("_id", Index_INFO_OID);
		bdr.append("max_node", MAX_NODE);
		bdr.append("max_leaf", MAX_LEAF);
		bdr.append("index_root", RootKey);
		string dbName = t->getDBName();
		OperationContext *opCtx = t->getOperationContext();

		auto status = Grid::get(opCtx)->catalogCache()->getDatabase(opCtx, dbName);
		uassertStatusOK(status.getStatus());
		auto conf = Grid::get(opCtx)->shardRegistry();
		conf->insertIndexMetadata(opCtx, bdr.obj());
		t->InsertDone(0, "config.meta_rtree", rtree_index::INSERT, "insertIndexMetadata");
		basicModifyIndexMetadataKey(opCtx, dbName, storageName, Index_INFO_OID);
		t->UpdateDone(1, "config.meta_rtree", rtree_index::UPDATE, "basicModifyIndexMetadataKey");
		basicModifyIndexType(opCtx, dbName, storageName, 1);
		t->UpdateDone(2, "config.meta_rtree", rtree_index::UPDATE, "basicModifyIndexType");
		return 1;
	}

	bool MongoIndexManagerIO::rtreeSetInputParamsIfExists(OperationContext *opCtx, string dbName, string storageName, mongo::OID &theRoot, int &MAX_NODE, int &MAX_LEAF, string &columnName)
	{
		BSONObjBuilder bdr;
		bdr.append("datanamespace", dbName + "." + storageName);

		/*findOne*/
		// auto status = grid.catalogCache()->getDatabase(opCtx, dbName);
		auto status = Grid::get(opCtx)->catalogCache()->getDatabase(opCtx, dbName);
		uassertStatusOK(status.getStatus());
		auto conf = Grid::get(opCtx)->shardRegistry();
		BSONObj Geo = conf->getGeometry(opCtx, bdr.obj());
		// BSONObj Geo = _conn->findOne(dbName+"."+GeoMeteDataName,bdr.obj());
		// log() << "Geoooooo:" << Geo << endl;
		columnName = Geo["column_name"].String();

		if (Geo["index_type"].Int() == 1)
		{
			mongo::OID index_info_oid = Geo["index_info"].OID();
			BSONObjBuilder indexquerybdr;
			indexquerybdr.append("_id", index_info_oid);

			/*findOne*/
			BSONObj index_info = conf->getIndexMetadata(opCtx, indexquerybdr.obj());

			theRoot = index_info["index_root"].OID();
			MAX_NODE = index_info["max_node"].Int();
			MAX_LEAF = index_info["max_leaf"].Int();
		}
		return true;
	}

	int MongoIndexManagerIO::rteeModifyRootKey(OperationContext *opCtx, string dbName, string storageName, mongo::OID newRootKey)
	{
		BSONObjBuilder bdr;
		bdr.append("datanamespace", dbName + "." + storageName);

		/*findOne*/
		// BSONObj Geo = _conn->findOne(dbName + "." + GeoMeteDataName, bdr.obj());
		// auto status = grid.catalogCache()->getDatabase(opCtx, dbName);
		auto status = Grid::get(opCtx)->catalogCache()->getDatabase(opCtx, dbName);
		uassertStatusOK(status.getStatus());
		auto conf = Grid::get(opCtx)->shardRegistry();
		BSONObj Geo = conf->getGeometry(opCtx, bdr.obj());

		BSONObjBuilder condition;
		condition.append("_id", Geo["index_info"].OID());
		BSONObjBuilder setBuilder;
		BSONObjBuilder setConditionBuilder;
		setConditionBuilder.append("index_root", newRootKey);
		setBuilder.append("$set", setConditionBuilder.obj());

		//	BSONObjBuilder cmdObj;
		//	cmdObj.append("query", condition.obj());
		//	cmdObj.append("update", setBuilder.obj());

		conf->updateIndexMetadata(opCtx, condition.obj(), setBuilder.obj());
		// bool ok;
		// ok = RunWriteCommand(dbName, RTreeIndexMetaData, cmdObj.obj(), UPDATE);

		/*updata*/
		//_conn->update(_dbName + "." + RTreeIndexMetaData, Query(condition.obj()), setBuilder.obj());

		return 1;
	}

	bool MongoIndexManagerIO::rtreeDeleteIndex(OperationContext *opCtx, string dbName, string storageName)
	{
		BSONObjBuilder bdr;
		bdr.append("datanamespace", dbName + "." + storageName);

		/*findOne*/
		// BSONObj Geo = _conn->findOne(dbName + "." + GeoMeteDataName, bdr.obj());
		// auto status = grid.catalogCache()->getDatabase(opCtx, dbName);
		auto status = Grid::get(opCtx)->catalogCache()->getDatabase(opCtx, dbName);
		uassertStatusOK(status.getStatus());
		auto conf = Grid::get(opCtx)->shardRegistry();
		BSONObj Geo = conf->getGeometry(opCtx, bdr.obj());

		BSONObjBuilder condition;
		condition.append("_id", Geo["index_info"].OID());
		/*remove*/
		//_conn->remove(_dbName + "." + RTreeIndexMetaData, condition.obj());
		// ok = RunWriteCommand(dbName, RTreeIndexMetaData, condition.obj(), REMOVE);
		conf->deleteIndexMetadata(opCtx, condition.obj());

		/*Exist*/
		{
			stdx::lock_guard<stdx::mutex> CONN_lock(_INDEXMANAGERIO_mu);
			if (!isConnected())
			{
				connectMyself();
			}
			if (_conn->exists(dbName + "." + "rtree_" + storageName))
			{
				/*Drop*/
				_conn->dropCollection(dbName + "." + "rtree_" + storageName);
			}
		}

		basicDeleteGeoMetadata(opCtx, dbName, storageName);

		return 1;
	}

	bool MongoIndexManagerIO::rtreeIndexExists(OperationContext *opCtx, string dbName, string storageName)
	{
		BSONObjBuilder bdr;
		bdr.append("datanamespace", dbName + "." + storageName);
		/*findOne*/
		// BSONObj Geo = _conn->findOne(dbName + "." + GeoMeteDataName, bdr.obj());
		// auto status = grid.catalogCache()->getDatabase(opCtx, dbName);
		auto status = Grid::get(opCtx)->catalogCache()->getDatabase(opCtx, dbName);
		uassertStatusOK(status.getStatus());
		;
		auto conf = Grid::get(opCtx)->shardRegistry();
		return conf->rtreeExists(opCtx, bdr.obj());
	}

	bool MongoIndexManagerIO::rtreeSetDataMBR(string dbName, string storageName, MBR &returnMBR, mongo::OID dataNodeKey, string columnName)
	{
		BSONObjBuilder bdr;
		bdr.append("_id", dataNodeKey);
		/*findOne*/
		BSONObj oneDoc;
		stdx::lock_guard<stdx::mutex> CONN_lock(_INDEXMANAGERIO_mu);
		{
			if (!isConnected())
			{
				connectMyself();
			}
			NamespaceString ns = NamespaceString(dbName + "." + storageName);
			oneDoc = _conn->findOne(ns, bdr.obj());
		}
		if (oneDoc.isEmpty())
		{
			return false;
		}

		BSONObj GeoObj = oneDoc[columnName].Obj();

		geojson_engine::GeoJSONPaser::VerifyGeoBSONType(GeoObj, returnMBR);
		return true;
	}

	bool MongoIndexManagerIO::rtreeSetDataMBR(mongo::BSONObj atomData, string columnName, MBR &returnMBR)
	{
		BSONObj GeoObj = atomData[columnName].Obj();
		if (atomData.hasField(columnName))
		{
			geojson_engine::GeoJSONPaser::VerifyGeoBSONType(GeoObj, returnMBR);
			return true;
		}
		return false;
	}

	void MongoIndexManagerIO::basicInitStorageTraverse(OperationContext *opCtx, string dbName, string storageName)
	{
		_currentStorage = storageName;
		/*query*/
		{
			stdx::lock_guard<stdx::mutex> CONN_lock(_INDEXMANAGERIO_mu);
			if (!isConnected())
			{
				connectMyself();
			}
			FindCommandRequest request(NamespaceStringOrUUID{NamespaceString(dbName + "." + _currentStorage)});
			request.setReadConcern(
				repl::ReadConcernArgs(repl::ReadConcernLevel::kMajorityReadConcern)
					.toBSONInner());

			_currentCursor = _conn->find(request);
		}
		BSONObjBuilder bdr;
		bdr.append("datanamespace  ", dbName + "." + storageName);
		/*findOne*/
		// BSONObj oneGeoMeteData = _conn->findOne(dbName + "." + GeoMeteDataName, bdr.obj());
		// auto status = grid.catalogCache()->getDatabase(opCtx, dbName);
		auto status = Grid::get(opCtx)->catalogCache()->getDatabase(opCtx, dbName);
		uassertStatusOK(status.getStatus());
		auto conf = Grid::get(opCtx)->shardRegistry();
		BSONObj oneGeoMetaData = conf->getGeometry(opCtx, bdr.obj());
		_columnName = oneGeoMetaData["column_name"].String();
	}

	/*
	  1: OK and Have Next;
	  -1: Not Ok ,error Data
	  0: Traverse Over
	*/
	int MongoIndexManagerIO::basicStorageTraverseNext(MBR &returnMBR, mongo::OID &returnKey)
	{
		/*more*/
		if (_currentCursor->more())
		{
			/*next*/
			BSONObj oneDoc = _currentCursor->next();
			returnKey = oneDoc["_id"].OID();

			BSONObj GeoJSONObj = oneDoc[_columnName].Obj();
			if (geojson_engine::GeoJSONPaser::VerifyGeoBSONType(GeoJSONObj, returnMBR))
			{
				return 1;
			}
			return -1;
		}
		else
		{
			return 0;
		}
		return 0;
	}

	DBClientBase *MongoIndexManagerIO::basicGetConnection()
	{
		return _conn;
	}

	bool MongoIndexManagerIO::geoVerifyIntersect(mongo::OID childOID, geos::geom::Geometry *searchGeometry, bool Conditions, string dbName, string storageName, string columnName)
	{
		if (Conditions)
		{
			return true;
		}
		else
		{

			BSONObj atomdata = basicFindNodeById(dbName, storageName, childOID);
			BSONObj geoObj = atomdata[columnName].Obj();
			if (geoObj.isEmpty())
				return false;
			geom::Geometry *pGeometry = NULL;
			int parseSt = _MGP.DataType2Geometry(geoObj, pGeometry);
			bool returnValue = false;
			if (parseSt == 1) // parseSuccess
			{
				if (pGeometry->intersects(searchGeometry))
				{
					returnValue = true;
				}
			}
			delete pGeometry;
			return returnValue;
		}
	}

	bool MongoIndexManagerIO::geoVerifyContains(mongo::OID childOID, geos::geom::Geometry *searchGeometry, bool Conditions, string dbName, string storageName, string columnName)
	{
		if (Conditions)
		{
			return true;
		}
		else
		{

			BSONObj atomdata = basicFindNodeById(dbName, storageName, childOID);
			BSONObj geoObj = atomdata[columnName].Obj();
			if (geoObj.isEmpty())
				return false;
			geom::Geometry *pGeometry = NULL;
			int parseSt = _MGP.DataType2Geometry(geoObj, pGeometry);
			bool returnValue = false;
			if (parseSt == 1) // parseSuccess
			{
				if (searchGeometry->contains(pGeometry))
				{
					returnValue = true;
				}
			}
			delete pGeometry;
			return returnValue;
		}
	}

	bool MongoIndexManagerIO::geoVerifyIntersect(mongo::OID childOID, double ctx, double cty, double rMin, double rMax, bool Contidions, string dbName, string storageName, string columnName, double &distance)
	{
		BSONObj atomdata = basicFindNodeById(dbName, storageName, childOID);
		BSONObj geoObj = atomdata[columnName].Obj();
		if (geoObj.isEmpty())
			return false;
		GeometryFactory::Ptr factory;
		CoordinateArraySequenceFactory csf;
		geom::Point *pPoint = factory.get()->createPoint(Coordinate(ctx, cty, 0));
		geom::Geometry *pGeometry = NULL;
		int parseSt = _MGP.DataType2Geometry(geoObj, pGeometry);
		bool returnValue = false;
		if (parseSt == 1) // parseSuccess
		{
			distance = pGeometry->distance(pPoint);
			if (distance <= rMax && distance >= rMin)
			{
				returnValue = true;
			}
		}
		delete pPoint;
		delete pGeometry;
		return returnValue;
	}

	bool MongoIndexManagerIO::parseGeometry(mongo::BSONObj Geometry2Parser, geos::geom::Geometry *&parsedGeometry)
	{
		try
		{
			int parseSt = _MGP.DataType2Geometry(Geometry2Parser, parsedGeometry);
			if (parseSt == 1) // parseSuccess
				return true;
			else
				return false;
		}
		catch (geos::util::GEOSException e)
		{
			cout << e.what() << endl;
			return false;
		}
	}

}
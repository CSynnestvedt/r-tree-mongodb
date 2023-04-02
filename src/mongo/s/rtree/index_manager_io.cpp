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

	
	
    MongoIndexManagerIO::MongoIndexManagerIO(DBClientBase * USER_CONN)
	{
		_conn = 0;
		//_conn = USER_CONN;
	}

	bool  MongoIndexManagerIO::connectMyself()
	{
		std::string errmsg;
		string url = "localhost:" + boost::lexical_cast<string>(serverGlobalParams.port);
		ConnectionString cs = ConnectionString::parse( url).getValue();
       // ConnectionString cs(ConnectionString::parse(connectionString));
        if (!cs.isValid()) {
           cout << "error parsing url: " << errmsg << endl;
           return false;
        }
		// std::string logMessage = "see what the connection string is: " + std::to_string(cs) + "\n"
		// LOGV2(40001, logMessage);
		_conn= cs.connect(errmsg).getValue().get();
        if (!_conn) {
            cout << "couldn't connect: " << errmsg << endl;
			return false;
        }
		 
		return true;
	}

	bool  MongoIndexManagerIO::IsConnected()
	{
		if (_conn!=0&&this->_conn->isStillConnected())
			return true;
		else
			return false;
	}

	mongo::OID MongoIndexManagerIO::Basic_Generate_Key()
	{
		return mongo::OID::gen();
	}

	mongo::BSONObj MongoIndexManagerIO::Basic_Fetch_AtomData(string DB_NAME, string STORAGE_NAME, mongo::OID Data2Fetch)
	{
		/*findOne*/
		BSONObjBuilder bdr;
		bdr.append("_id",Data2Fetch);
		/*
		 * please note that this _conn for indexmanagerIO
		 * is shared by many threads. just like RTreeIO _conn
		 * so lock it 1st
		 */
		 BSONObj returnBSONOBJ;
		{
			stdx::lock_guard<stdx::mutex> CONN_lock(_INDEXMANAGERIO_mu);
			if(!IsConnected())
			{
				connectMyself();
			}
			NamespaceString ns = NamespaceString(DB_NAME + "." + STORAGE_NAME);
		    returnBSONOBJ =_conn->findOne(ns, bdr.obj());
		}
		return returnBSONOBJ;
	}


	bool MongoIndexManagerIO::Basic_Store_One_Atom_Data(OperationContext* txn,string DB_NAME, string STORAGE_NAME, mongo::BSONObj AtomData, mongo::OID &AtomKey, BSONObjBuilder& result)
	{
		BSONObjBuilder bdr;
		AtomKey = AtomData["_id"].OID();
		//bdr.append("_id", AtomKey);
		bdr.appendElements(AtomData);
		/*insert*/
		bool ok;
		ok = RunWriteCommand(txn,DB_NAME, STORAGE_NAME, bdr.obj(), INSERT,result);
		//_conn->insert(DB_NAME + "." + STORAGE_NAME, AtomData);
		return 1;
	}

	bool MongoIndexManagerIO::Basic_Drop_Storage(OperationContext* txn,string DB_NAME, string STORAGE_NAME)
	{
		/*drop*/
		BSONObj empty;
		BSONObjBuilder bdr;
		BSONObjBuilder & bdrRef=bdr;
		return RunWriteCommand(txn,DB_NAME, STORAGE_NAME, empty, DROP,bdrRef);
	}

	int MongoIndexManagerIO::Basic_Delete_One_SpatialObj_Only(OperationContext* txn,string DB_NAME, string STORAGE_NAME, mongo::OID key2delete)
	{
		BSONObjBuilder bdr;
		bdr.append("_id",key2delete);
		/*remove*/
		//_conn->remove(_dbName + "." + STORAGE_NAME, bdr.obj());
		bool ok;
		
		BSONObjBuilder newBdr;
		BSONObjBuilder & newBdrRef=newBdr;
		ok = RunWriteCommand(txn,DB_NAME, STORAGE_NAME, bdr.obj(), REMOVE,newBdrRef);
		return 1;
	}

	int MongoIndexManagerIO::Basic_Get_Index_Type(OperationContext* txn,string DB_NAME,string STORAGE_NAME)
	{
		BSONObjBuilder bdr;
		bdr.append("datanamespace", DB_NAME+"."+STORAGE_NAME);

		/*findOne*/
		//auto status = grid.catalogCache()->getDatabase(txn, DB_NAME);
		auto status = Grid::get(txn)->catalogCache()->getDatabase(txn, DB_NAME);
        uassertStatusOK(status.getStatus());

		//DBConfigPtr conf = grid.getDBConfig(DB_NAME, false);
		BSONObj oneGeoMeta = Grid::get(txn)->shardRegistry()->getGeometry(txn, bdr.obj());

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

	bool MongoIndexManagerIO::Basic_Exist_Storage(string DB_NAME,string STORAGE_NAME)
	{
		/*EXIST*/
		stdx::lock_guard<stdx::mutex> CONN_lock(_INDEXMANAGERIO_mu);
		if(!IsConnected())
		{
				connectMyself();
		}
		if (_conn->exists(DB_NAME + "." + STORAGE_NAME))
		{
			return true;
		}
		else
		{
			return false;
		}
	}

	int MongoIndexManagerIO::Basic_StorageOneGeoMeteData(OperationContext* txn,string DB_NAME, string STORAGE_NAME, string COLUMN_NAME, int INDEX_TYPE, MBR m, int GTYPE, int SRID, int CRS_TYPE, double TOLERANCE)
	{
		BSONObjBuilder bdr;
		bdr.append("datanamespace", DB_NAME+"."+STORAGE_NAME);
		bdr.append("column_name", COLUMN_NAME);
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
		/*insert*/
        //auto status = grid.catalogCache()->getDatabase(txn, DB_NAME);
		auto status = Grid::get(txn)->catalogCache()->getDatabase(txn, DB_NAME);
        uassertStatusOK(status.getStatus());
        bool result = Grid::get(txn)->shardRegistry()->registerGeometry(txn, bdr.obj());
		// DBConfigPtr conf = grid.getDBConfig(DB_NAME, false);
	    // conf->registerGeometry(txn,bdr.obj());
		return result;
	}

	int MongoIndexManagerIO::Basic_DeleteOneGeoMeteData(OperationContext* txn,string DB_NAME,string STORAGE_NAME)
	{
		BSONObjBuilder querybdr;
		querybdr.append("datanamespace", DB_NAME+"."+STORAGE_NAME);
		//auto status = grid.catalogCache()->getDatabase(txn, DB_NAME);
		auto status = Grid::get(txn)->catalogCache()->getDatabase(txn, DB_NAME);
        uassertStatusOK(status.getStatus());
		Grid::get(txn)->shardRegistry()->deleteGeometry(txn, querybdr.obj());
        // shared_ptr<DBConfig> conf = status.getValue();
		//DBConfigPtr conf = grid.getDBConfig(DB_NAME, false);
		// conf->deleteGeometry(txn,querybdr.obj());
		return 1;
	}

	int MongoIndexManagerIO::Basic_ModifyIndexType(OperationContext* txn,string DB_NAME, string STORAGE_NAME, int Type2Modify)
	{
		BSONObjBuilder condition;
		condition.append("datanamespace", DB_NAME+"."+STORAGE_NAME);
		BSONObjBuilder setBuilder;
		BSONObjBuilder setConditionBuilder;
		setConditionBuilder.append("index_type", Type2Modify);
		setBuilder.append("$set", setConditionBuilder.obj());

		//BSONObjBuilder cmdObj;
		//cmdObj.append("query", condition.obj());
		//cmdObj.append("update", setBuilder.obj());
		//bool ok;
		//ok = RunWriteCommand(DB_NAME, GeoMeteDataName, cmdObj.obj(), UPDATE);
		//auto status = grid.catalogCache()->getDatabase(txn, DB_NAME);
		auto status = Grid::get(txn)->catalogCache()->getDatabase(txn, DB_NAME);
        uassertStatusOK(status.getStatus());
        auto conf = Grid::get(txn)->shardRegistry();
		conf->updateGeometry(txn,condition.obj(), setBuilder.obj());
		/*updata*/
		//_conn->update(_dbName + "." + GeoMeteDataName, Query(condition.obj()), setBuilder.obj());
		if (Type2Modify == 0)
		{
			BSONObjBuilder condition1;
			condition1.append("datanamespace", DB_NAME+"."+STORAGE_NAME);
			BSONObjBuilder setBuilder1;
			BSONObjBuilder setConditionBuilder1;
			mongo::OID nullOID;
			setConditionBuilder1.append("index_info", nullOID);
			setBuilder1.append("$set", setConditionBuilder1.obj());

			/*updata*/
			conf->updateGeometry(txn,condition.obj(), setBuilder1.obj());
		}

		return 1;
	}

	int MongoIndexManagerIO::Basic_ModifyIndexMeteDataKey(OperationContext* txn,string DB_NAME, string STORAGE_NAME, mongo::OID Key2Modify)
	{
		
		BSONObjBuilder condition;
		condition.append("NAMESPACE", DB_NAME+"."+STORAGE_NAME);
		BSONObjBuilder setBuilder;
		BSONObjBuilder setConditionBuilder;
		setConditionBuilder.append("index_info", Key2Modify);
		setBuilder.append("$set", setConditionBuilder.obj());
		//BSONObjBuilder cmdObj;
		//cmdObj.append("query", condition.obj());
		//cmdObj.append("update", setBuilder.obj());
		//bool ok;
		//ok = RunWriteCommand(DB_NAME, GeoMeteDataName, cmdObj.obj(), UPDATE);
		//auto status = grid.catalogCache()->getDatabase(txn, DB_NAME);
		auto status = Grid::get(txn)->catalogCache()->getDatabase(txn, DB_NAME);
        uassertStatusOK(status.getStatus());
		Grid::get(txn)->shardRegistry()->updateGeometry(txn, condition.obj(), setBuilder.obj());

        // shared_ptr<DBConfig> conf = status.getValue();
		// conf->updateGeometry(txn,condition.obj(), setBuilder.obj());
		/*updata*/
		//_conn->update(_dbName + "." + GeoMeteDataName, Query(condition.obj()), setBuilder.obj());

		return 1;
	}

	bool MongoIndexManagerIO::Basic_Exist_Geo_MeteData(OperationContext* txn,string DB_NAME, string STORAGE_NAME)
	{
		BSONObjBuilder bdr;
		bdr.append("NAMESPACE",DB_NAME+"."+STORAGE_NAME);
		//log() << "checkgeo:" << DB_NAME + "." + STORAGE_NAME << endl;
		/*findOne*/
		//auto status = grid.catalogCache()->getDatabase(txn, DB_NAME);
        // uassertStatusOK(status.getStatus());
        // shared_ptr<DBConfig> conf = status.getValue();
	    auto status = Grid::get(txn)->catalogCache()->getDatabase(txn, DB_NAME);
	    uassertStatusOK(status.getStatus());

		//shared_ptr<DBConfig> conf =
        //        uassertStatusOK(grid.catalogCache()->getDatabase(txn, DB_NAME));
		return Grid::get(txn)->shardRegistry()->checkGeoExist(txn, bdr.obj());
	}

	int MongoIndexManagerIO::RTree_StorageIndexMeteData(Transaction* t,string STORAGE_NAME, int MAX_NODE, int MAX_LEAF, mongo::OID RootKey)
	{
		BSONObjBuilder bdr;
		mongo::OID Index_INFO_OID = OID::gen();
		bdr.append("_id", Index_INFO_OID);
		bdr.append("max_node", MAX_NODE);
		bdr.append("max_leaf", MAX_LEAF);
		bdr.append("index_root", RootKey);
        string DB_NAME = t->getDBName();
		OperationContext* txn = t->getOperationContext();

		auto status = Grid::get(txn)->catalogCache()->getDatabase(txn, DB_NAME);
	    uassertStatusOK(status.getStatus());
        auto conf = Grid::get(txn)->shardRegistry();
		conf->insertIndexMetadata(txn,bdr.obj());
		t->InsertDone(0, "config.meta_rtree", rtree_index::INSERT, "insertIndexMetadata");
		Basic_ModifyIndexMeteDataKey(txn, DB_NAME,STORAGE_NAME, Index_INFO_OID);
		t->UpdateDone(1, "config.meta_rtree", rtree_index::UPDATE, "Basic_ModifyIndexMeteDataKey");
		Basic_ModifyIndexType(txn, DB_NAME, STORAGE_NAME, 1);
		t->UpdateDone(2, "config.meta_rtree", rtree_index::UPDATE, "Basic_ModifyIndexType");
		return 1;
	}

	bool MongoIndexManagerIO::RTree_GetParms(OperationContext* txn,string DB_NAME,string STORAGE_NAME, mongo::OID &theRoot, int &MAX_NODE, int & MAX_LEAF, string &COLUMN_NAME)
	{
		BSONObjBuilder bdr;
		bdr.append("datanamespace", DB_NAME+"."+STORAGE_NAME);
		
		/*findOne*/
		//auto status = grid.catalogCache()->getDatabase(txn, DB_NAME);
		auto status = Grid::get(txn)->catalogCache()->getDatabase(txn, DB_NAME);
	    uassertStatusOK(status.getStatus());
        auto conf = Grid::get(txn)->shardRegistry();
		BSONObj Geo = conf->getGeometry(txn,bdr.obj());
		//BSONObj Geo = _conn->findOne(DB_NAME+"."+GeoMeteDataName,bdr.obj());
		//log() << "Geoooooo:" << Geo << endl;
		COLUMN_NAME = Geo["column_name"].String();

		if (Geo["index_type"].Int() == 1)
		{
			mongo::OID index_info_oid = Geo["index_info"].OID();
			BSONObjBuilder indexquerybdr;
			indexquerybdr.append("_id", index_info_oid);

			/*findOne*/
			BSONObj index_info = conf->getIndexMetadata(txn,indexquerybdr.obj());

			theRoot = index_info["index_root"].OID();
			MAX_NODE = index_info["max_node"].Int();
			MAX_LEAF = index_info["max_leaf"].Int();
		}
		return true;
	}

	int MongoIndexManagerIO::RTree_ModifyRootKey(OperationContext* txn,string DB_NAME, string STORAGE_NAME, mongo::OID newRootKey)
	{
		BSONObjBuilder bdr;
		bdr.append("datanamespace", DB_NAME+"."+STORAGE_NAME);

		/*findOne*/
		//BSONObj Geo = _conn->findOne(DB_NAME + "." + GeoMeteDataName, bdr.obj());
		//auto status = grid.catalogCache()->getDatabase(txn, DB_NAME);
		auto status = Grid::get(txn)->catalogCache()->getDatabase(txn, DB_NAME);
	    uassertStatusOK(status.getStatus());
        auto conf = Grid::get(txn)->shardRegistry();
		BSONObj Geo = conf->getGeometry(txn,bdr.obj());

		BSONObjBuilder condition;
		condition.append("_id", Geo["index_info"].OID());
		BSONObjBuilder setBuilder;
		BSONObjBuilder setConditionBuilder;
		setConditionBuilder.append("index_root", newRootKey);
		setBuilder.append("$set", setConditionBuilder.obj());

	//	BSONObjBuilder cmdObj;
	//	cmdObj.append("query", condition.obj());
	//	cmdObj.append("update", setBuilder.obj());

		conf->updateIndexMetadata(txn,condition.obj(), setBuilder.obj());
		//bool ok;
		//ok = RunWriteCommand(DB_NAME, RTreeIndexMetaData, cmdObj.obj(), UPDATE);

		/*updata*/
		//_conn->update(_dbName + "." + RTreeIndexMetaData, Query(condition.obj()), setBuilder.obj());

		return 1;
	}

	bool MongoIndexManagerIO::RTree_DeleteIndex(OperationContext* txn,string DB_NAME, string STORAGE_NAME)
	{
		BSONObjBuilder bdr;
		bdr.append("datanamespace", DB_NAME+"."+STORAGE_NAME);

		/*findOne*/
		//BSONObj Geo = _conn->findOne(DB_NAME + "." + GeoMeteDataName, bdr.obj());
		//auto status = grid.catalogCache()->getDatabase(txn, DB_NAME);
		auto status = Grid::get(txn)->catalogCache()->getDatabase(txn, DB_NAME);
	    uassertStatusOK(status.getStatus());
        auto conf = Grid::get(txn)->shardRegistry();
		BSONObj Geo = conf->getGeometry(txn,bdr.obj());

		BSONObjBuilder condition;
		condition.append("_id", Geo["index_info"].OID());
		/*remove*/
		//_conn->remove(_dbName + "." + RTreeIndexMetaData, condition.obj());
		//ok = RunWriteCommand(DB_NAME, RTreeIndexMetaData, condition.obj(), REMOVE);
		conf->deleteIndexMetadata(txn,condition.obj());

		/*Exist*/
		{
		stdx::lock_guard<stdx::mutex> CONN_lock(_INDEXMANAGERIO_mu);
		if(!IsConnected())
		{
			connectMyself();
		}
		if (_conn->exists(DB_NAME + "." + "rtree_" + STORAGE_NAME))
		{
			/*Drop*/
			_conn->dropCollection(DB_NAME + "." + "rtree_" + STORAGE_NAME);
		}
		}

		Basic_DeleteOneGeoMeteData(txn,DB_NAME, STORAGE_NAME);

		return 1;
	}

	bool MongoIndexManagerIO::RTree_ExistIndex(OperationContext* txn,string DB_NAME,string STORAGE_NAME)
	{
		BSONObjBuilder bdr;
		bdr.append("datanamespace", DB_NAME+"."+STORAGE_NAME);
		/*findOne*/
		//BSONObj Geo = _conn->findOne(DB_NAME + "." + GeoMeteDataName, bdr.obj());
		//auto status = grid.catalogCache()->getDatabase(txn, DB_NAME);
		auto status = Grid::get(txn)->catalogCache()->getDatabase(txn, DB_NAME);
	    uassertStatusOK(status.getStatus());;
        auto conf = Grid::get(txn)->shardRegistry();
		return conf->rtreeExists(txn,bdr.obj());
	}

	bool MongoIndexManagerIO::RTree_GetDataMBR(string DB_NAME,string STORAGE_NAME, MBR &returnMBR, mongo::OID DataNodeKey, string COLUMN_NAME)
	{
		BSONObjBuilder bdr;
		bdr.append("_id",DataNodeKey);
		/*findOne*/
		BSONObj oneDoc;
		stdx::lock_guard<stdx::mutex> CONN_lock(_INDEXMANAGERIO_mu);
        {
			if(!IsConnected())
			{
				connectMyself();
			}
			NamespaceString ns = NamespaceString(DB_NAME+"."+STORAGE_NAME);
	    	oneDoc = _conn->findOne(ns, bdr.obj());
		}		
		if (oneDoc.isEmpty())
		{
			return false;
		}

		BSONObj GeoObj = oneDoc[COLUMN_NAME].Obj();

		geojson_engine::GeoJSONPaser::VerifyGeoBSONType(GeoObj, returnMBR);
		return true;
	}

	bool MongoIndexManagerIO::RTree_GetDataMBR(mongo::BSONObj AtomData, string COLUMN_NAME, MBR &returnMBR)
	{
		BSONObj GeoObj = AtomData[COLUMN_NAME].Obj();
		if (AtomData.hasField(COLUMN_NAME))
		{
			geojson_engine::GeoJSONPaser::VerifyGeoBSONType(GeoObj, returnMBR);
			return true;
		}
		return false;
	}

	void MongoIndexManagerIO::Basic_Init_Storage_Traverse(OperationContext* txn,string DB_NAME,string STORAGE_NAME)
	{
		_Current_Storage = STORAGE_NAME;
		/*query*/
		{
			stdx::lock_guard<stdx::mutex> CONN_lock(_INDEXMANAGERIO_mu);
			if(!IsConnected())
			{
				connectMyself();
			}
			FindCommandRequest request(NamespaceStringOrUUID{NamespaceString(DB_NAME+"."+_Current_Storage)});
			request.setReadConcern(
			repl::ReadConcernArgs(repl::ReadConcernLevel::kMajorityReadConcern)
				.toBSONInner());

			_Current_Cursor = _conn->find(request);
		}
		BSONObjBuilder bdr;
		bdr.append("NAMESPACE",DB_NAME+"."+STORAGE_NAME);
		/*findOne*/
		//BSONObj oneGeoMeteData = _conn->findOne(DB_NAME + "." + GeoMeteDataName, bdr.obj());
		//auto status = grid.catalogCache()->getDatabase(txn, DB_NAME);
		auto status = Grid::get(txn)->catalogCache()->getDatabase(txn, DB_NAME);
	    uassertStatusOK(status.getStatus());
        auto conf = Grid::get(txn)->shardRegistry();
		BSONObj oneGeoMetaData = conf->getGeometry(txn,bdr.obj());
		_COLUMN_NAME = oneGeoMetaData["column_name"].String();
	}

	/*
	  1: OK and Have Next;
	  -1: Not Ok ,error Data
	  0: Traverse Over
	*/
	int MongoIndexManagerIO::Basic_Storage_Traverse_Next(MBR &returnMBR, mongo::OID &returnKey)
	{
		/*more*/
		if (_Current_Cursor->more())
		{
			/*next*/
			BSONObj oneDoc = _Current_Cursor->next();
			returnKey = oneDoc["_id"].OID();
			
			BSONObj GeoJSONObj = oneDoc[_COLUMN_NAME].Obj();
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

	DBClientBase * MongoIndexManagerIO::Basic_Get_Connection()
	{
		return _conn;
	}

	bool MongoIndexManagerIO::Geo_Verify_Intersect(mongo::OID childOID, geos::geom::Geometry *searchGeometry, bool Conditions, string DB_NAME, string STORAGE_NAME, string COLUMN_NAME)
	{
		if (Conditions)
		{
			return true;
		}
		else
		{

			BSONObj atomdata = Basic_Fetch_AtomData(DB_NAME, STORAGE_NAME, childOID);
			BSONObj geoObj = atomdata[COLUMN_NAME].Obj();
			if (geoObj.isEmpty())return false;
			geom::Geometry *pGeometry = NULL;
			int parseSt = MGP.DataType2Geometry(geoObj, pGeometry);
			bool returnValue = false;
			if (parseSt == 1)//parseSuccess
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

	bool MongoIndexManagerIO::Geo_Verify_Contain(mongo::OID childOID, geos::geom::Geometry *searchGeometry, bool Conditions, string DB_NAME, string STORAGE_NAME, string COLUMN_NAME)
	{
		if (Conditions)
		{
			return true;
		}
		else
		{

			BSONObj atomdata = Basic_Fetch_AtomData(DB_NAME, STORAGE_NAME, childOID);
			BSONObj geoObj = atomdata[COLUMN_NAME].Obj();
			if (geoObj.isEmpty())return false;
			geom::Geometry *pGeometry = NULL;
			int parseSt = MGP.DataType2Geometry(geoObj, pGeometry);
			bool returnValue = false;
			if (parseSt == 1)//parseSuccess
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


	bool MongoIndexManagerIO::Geo_Verify_Intersect(mongo::OID childOID, double ctx, double cty,double rMin,double rMax, bool Contidions, string DB_NAME, string STORAGE_NAME, string COLUMN_NAME,double &distance)
	{
		BSONObj atomdata = Basic_Fetch_AtomData(DB_NAME, STORAGE_NAME, childOID);
		BSONObj geoObj = atomdata[COLUMN_NAME].Obj();
		if (geoObj.isEmpty())return false;
		GeometryFactory::Ptr factory;
		CoordinateArraySequenceFactory csf;
		geom::Point *pPoint = factory.get()->createPoint(Coordinate(ctx, cty, 0));
		geom::Geometry *pGeometry = NULL;
		int parseSt = MGP.DataType2Geometry(geoObj, pGeometry);
		bool returnValue = false;
		if (parseSt == 1)//parseSuccess
		{
			distance = pGeometry->distance(pPoint);
			if (distance <= rMax&&distance >= rMin)
			{
				returnValue = true;
			}
			
		}
		delete pPoint;
		delete pGeometry;
		return returnValue;
	}

	bool MongoIndexManagerIO::ParseGeometry(mongo::BSONObj Geometry2Parser, geos::geom::Geometry *&parsedGeometry)
	{
		try
		{
			int parseSt = MGP.DataType2Geometry(Geometry2Parser, parsedGeometry);
			if (parseSt == 1)//parseSuccess
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
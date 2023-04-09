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

#include <iostream>
#include "rtree_core.h"
#include "geojson_engine.h"
#include "mongo_geometry_parser.h"
// #include "GeoRef.h"
#include <boost/scoped_ptr.hpp>
#include "mongo/client/dbclient_connection.h"
// #include "mongo/s/catalog/type_database.h"
#include "mongo/s/rtree/transaction.h"

// header from s2 (geos ex)
#include "third_party/s2/geos/geom/Geometry.h"
#include "third_party/s2/geos/geom/GeometryFactory.h"
#include "third_party/s2/geos/geom/CoordinateArraySequenceFactory.h"
#include "third_party/s2/geos/geom/CoordinateSequence.h"
#include "third_party/s2/geos/geom/Polygon.h"
#include "third_party/s2/geos/geom/LineString.h"
#include "third_party/s2/geos/geom/Point.h"
#include "third_party/s2/geos/geom/GeometryCollection.h"
#include "third_party/s2/geos/geom/MultiLineString.h"
#include "third_party/s2/geos/geom/MultiPoint.h"
#include "third_party/s2/geos/geom/MultiPolygon.h"
#include "third_party/s2/geos/util/GEOSException.h"
#include "third_party/s2/geos/geom/LinearRing.h"

#include "mongo/stdx/mutex.h"

using namespace std;
using namespace rtree_index;
using namespace mongo;
using namespace geometry_parser;

namespace index_manager
{
	extern stdx::mutex _INDEXMANAGERIO_mu;
	/**
	 *Some operations in meta data are defined here
	 *By calling functions defined in DBConfig class
	 */

	class MongoIndexManagerIO
	{
	public:
		MongoIndexManagerIO(DBClientConnection *userConnection);
		mongo::OID Basic_Generate_Key();
		mongo::BSONObj basicFindNodeById(string dbName, string storageName, mongo::OID dataToFetch);
		bool basicInsertOneNode(OperationContext *opCtx, string dbName, string storageName, mongo::BSONObj atomData, mongo::OID &atomKey, BSONObjBuilder &result);
		bool basicDropStorage(OperationContext *opCtx, string dbName, string storageName);
		int basicDeleteNodeById(OperationContext *opCtx, string dbName, string storageName, mongo::OID keyTodelete);
		int basicGetIndexType(OperationContext *opCtx, string dbName, string storageName);
		bool basicStorageExists(string dbName, string storageName);
		int basicInsertGeoMetadata(OperationContext *opCtx, string dbName, string storageName, string columnName, int indexType, MBR m, int gType, int srid, int crsType, double tolerance);
		int basicDeleteGeoMetadata(OperationContext *opCtx, string dbName, string storageName);
		int basicModifyIndexType(OperationContext *opCtx, string dbName, string storageName, int typeToModify);
		bool basicGeoMetadataExists(OperationContext *opCtx, string dbName, string collectionName);
		int basicModifyIndexMetadataKey(OperationContext *opCtx, string dbName, string storageName, mongo::OID keyToModify);
		int rtreeInsertIndexMetaData(Transaction *t, string storageName, int maxNode, int maxLeaf, mongo::OID rootKey);
		int rteeModifyRootKey(OperationContext *opCtx, string dbName, string storageName, mongo::OID newRootKey);
		bool rtreeSetInputParamsIfExists(OperationContext *opCtx, string dbName, string storageName, mongo::OID &theRoot, int &maxNode, int &maxLeaf, string &columnName);
		bool rtreeDeleteIndex(OperationContext *opCtx, string dbName, string storageName);
		bool rtreeIndexExists(OperationContext *opCtx, string dbName, string storageName);
		bool rtreeSetDataMBR(string dbName, string storageName, MBR &returnMBR, mongo::OID dataNodeKey, string columnName);
		bool rtreeSetDataMBR(mongo::BSONObj atomData, string columnName, MBR &returnMBR);
		void basicInitStorageTraverse(OperationContext *opCtx, string dbName, string storageName);
		int basicStorageTraverseNext(MBR &returnMBR, mongo::OID &returnKey);
		DBClientBase *basicGetConnection();
		bool geoVerifyIntersect(mongo::OID childOID, geos::geom::Geometry *searchGeometry, bool Conditions, string dbName, string storageName, string columnName);
		bool geoVerifyContains(mongo::OID childOID, geos::geom::Geometry *searchGeometry, bool Conditions, string dbName, string storageName, string columnName);
		bool geoVerifyIntersect(mongo::OID childOID, double ctx, double cty, double rMin, double rMax, bool conditions, string dbName, string storageName, string columnName, double &distance);
		bool parseGeometry(mongo::BSONObj geometryToParser, geos::geom::Geometry *&parsedGeometry);
		bool connectMyself();
		bool isConnected();

	private:
		MongoGeometryParser _MGP;
		string _connectionString; // place here rather in IndexManagerBase to be IO detached
		DBClientConnection *_conn;
		string _currentStorage;
		std::unique_ptr<DBClientCursor> _currentCursor;
		string _columnName; // used to accelerate the process if necessary
	};

}
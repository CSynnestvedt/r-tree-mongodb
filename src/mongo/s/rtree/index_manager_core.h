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

#ifndef FileName_H_
#define FileName_H_

#include "index_manager_io.h"
#include "rtree_geonear_cursor.h"
#include "rtree_range_query_cursor.h"

//header from s2 (geos ex)
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



using namespace std;
using namespace rtree_index;


namespace index_manager
{
	struct KeywithDis
	{
		double distance;
		mongo::OID key;
	};

    bool nearcompare(KeywithDis o1, KeywithDis o2);
	
	/**
	 *The operations of cmd associated with rtree are defined here
	 *When the Command is called,it will call the class by a global object
	 */

	class IndexManagerBase
	{
	public:
		 IndexManagerBase();
		 IndexManagerBase(MongoIndexManagerIO *USER_INDEXMANAGER_IO, MongoIO *USER_RTREE_IO);
		 int RegisterGeometry(OperationContext* txn,string DB_NAME,string COLLECTION_NAME, string COLUMN_NAME, int SDO_GTYPE, int SDO_SRID, int CRS_TYPE, double SDO_TORRANCE);
		 int PrepareIndex(OperationContext* txn,string DB_NAME, string COLLECTION_NAME, string COLUMN_NAME, int INDEX_TYPE, int Max_Node, int Max_Leaf);
		 int DeleteGeoObjByKey(OperationContext* txn,string DB_NAME, string COLLECTION_NAME, mongo::OID key2delete);
		 int DeleteIntersectedGeoObj(OperationContext* txn,string DB_NAME, string COLLECTION_NAME,mongo::BSONObj InputGeometry);
		 int DeleteContainedGeoObj(OperationContext* txn,string DB_NAME, string COLLECTION_NAME,mongo::BSONObj InputGeometry);
		 int DropIndex(OperationContext* txn,string DB_NAME, string COLLECTIONNAME);
		 int DropCollection(OperationContext* txn,string DB_NAME, string COLLECTIONNAME);
		 int ValidateGeometry(OperationContext* txn,string DB_NAME, string COLLECTION_NAME);
		 int RepairIndex(string DB_NAME, string COLLECTION_NAME);
		 int InsertIndexedDoc(OperationContext* txn,string DB_NAME, string COLLECTION_NAME, mongo::BSONObj AtomData, BSONObjBuilder& result);
		 std::unique_ptr<RTreeRangeQueryCursor> GeoSearchWithin(OperationContext* txn,string DB_NAME, string COLLECTION_NAME,mongo::BSONObj InputGeometry);
		 bool GeoSearchWithinWithoutRefining(OperationContext* txn,string DB_NAME, string COLLECTION_NAME,mongo::BSONObj InputGeometry, vector<mongo::OID>& results);
         std::unique_ptr<RTreeRangeQueryCursor> GeoSearchIntersects(OperationContext* txn, string DB_NAME, string COLLECTION_NAME, mongo::BSONObj InputGeometry);
		 std::unique_ptr<RTreeGeoNearCursor> GeoSearchNear(OperationContext* txn, string DB_NAME,string COLLECTION_NAME,double ctx,double cty,double rMin,double rMax);
		 bool InitalizeManager(MongoIndexManagerIO *USER_INDEXMANAGER_IO,MongoIO *USER_RTREE_IO);
	private:
		 MongoIndexManagerIO *IO;
		 RTree _R;
		 MongoIO *_RIO;
	};

	

}
#endif
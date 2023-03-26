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



#include <iostream>
#include <vector>
#include "rtree_io.h"
#pragma  once
using namespace std;


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
#include "third_party/s2/geos/geom/Coordinate.h"
#include "third_party/s2/geos/geom/CoordinateSequence.h"
#include "third_party/s2/geos/geom/PrecisionModel.h"


namespace rtree_index
{
	class RTree
	{
	public:
		RTree(int Max_Node, int Max_Leaf, mongo::OID Root, MongoIO* UserIO);
		RTree();
		bool InsertRoot(OperationContext* txn,mongo::OID &Root);
		bool Insert(OperationContext* txn,mongo::OID &Root, Branch branch2insert, int Level);
		bool Search(geos::geom::Geometry * SearchGeometry , vector<mongo::OID> &Results,vector<bool> &lazyIntersect);
		// bool geoNear(double x, double y, double rMin,double rMax, vector<mongo::OID> &OIDList, vector<bool> &lazyIntersect);
		bool DeleteNode(OperationContext* txn,mongo::OID &RootKey, mongo::OID KeyNode2Delete, MBR mbrOfDeleteNode);
		void SetRoot(mongo::OID Root);
		bool ReConfigure(int Max_Node, int Max_Leaf, mongo::OID Root, string DB_NAME, string STORAGE_NAME);
		
		MongoIO *IO;

	private:

		
		int _Max_Node;
		int _Max_Leaf;
		mongo::OID _Root;

		
		Node NewNode(int);
		Node NewNode(int, mongo::OID);
		Branch createBranch(bool HasData, MBR m, mongo::OID childKey);
		MBR comBineMBR(MBR m1, MBR m2);
		double RTreeMBRSphericalVolume(MBR m);

		
		bool Insert2(OperationContext* txn,mongo::OID NodeOID, mongo::OID &newNode, Branch branch2insert, int Level);
		bool Search2(mongo::OID nodeKey, geos::geom::Geometry * SearchGeometry, int Level, vector<mongo::OID> &OIDList, vector<bool> &lazyIntersect);

	    
		bool InsertBranch(OperationContext* txn,mongo::OID terget, Branch branch2insert, mongo::OID &newNode);

		
		MBR createCover(mongo::OID keyofcreate);

		
		int SelectBestBranch(Node CurrentNode, MBR m);

		
		bool splitNode(OperationContext* txn,mongo::OID currentOID, Branch Branch2Insert, mongo::OID &newNode);

		int Intersect(MBR m1, MBR m2);

		bool DeleteNode2(OperationContext* txn,mongo::OID NodeKey, mongo::OID Key2Delete, MBR mbrOFDeletingNode, vector<mongo::OID> &L);

		
		geos::geom::GeometryFactory::Ptr factory;
		geos::geom::CoordinateArraySequenceFactory csf;
	};



	

}
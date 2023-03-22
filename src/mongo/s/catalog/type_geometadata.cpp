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

#include "mongo/s/catalog/type_geometadata.h"

#include "mongo/base/status_with.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/simple_bsonobj_comparator.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/db/server_options.h"
#include "mongo/logv2/log.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/str.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kSharding

namespace mongo {
    const NamespaceString GeoMetaData::ConfigNS("config.meta_geom");

    // Field names and types in the chunks collections.
    const BSONField<std::string> GeoMetaData::datanamespace("datanamespace");
    const BSONField<std::string> GeoMetaData::column_name("column_name");
    const BSONField<int> GeoMetaData::index_type("index_type");
    const BSONField<OID> GeoMetaData::index_info("index_info");
    const BSONField<int> GeoMetaData::gtype("gtype");
    const BSONField<int> GeoMetaData::srid("srid");
    const BSONField<int> GeoMetaData::crs_type("crs_type");
    const BSONField<double> GeoMetaData::tolerance("tolerance");


GeoMetaData::GeoMetaData() = default;

GeoMetaData::GeoMetaData(std::string datanamespace, std::string column_name,
int index_type, OID index_info, int gtype, int srid, int crs_type, double tolerance)
 : 
_datanamespace(std::move(datanamespace)),
_column_name(std::move(column_name)),
_index_type(std::move(index_type)),
_index_info(std::move(index_info)),
_gtype(std::move(gtype)),
_srid(std::move(srid)),
_crs_type(std::move(crs_type)),
_tolerance(std::move(tolerance))
{};


BSONObj GeoMetaData::toConfigBSON() const {
    BSONObjBuilder builder;
    if (_datanamespace)
        builder.append(datanamespace.name(), getDataNameSpace());
    if (_column_name)
        builder.append(column_name.name(), getColumnName());
    if (_index_type)
        builder.appendNumber(index_type.name(), getIndexType());
    if (_index_info)
        builder.append(index_info.name(), getIndexInfo());
    if (_gtype)
        builder.appendNumber(gtype.name(), getGType());
    if (_srid)
        builder.appendNumber(srid.name(), getSRID());
    if (_crs_type)
        builder.appendNumber(crs_type.name(), getCRSType());
    if (_tolerance)
        builder.appendNumber(tolerance.name(), getTolerance());
    return builder.obj();
}


    void GeoMetaData::setDataNameSpace(const std::string& datanamespace){
        _datanamespace = datanamespace;
    };

    void GeoMetaData::setColumnName(const std::string& column_name){
        _column_name = column_name;
    } ;

    void GeoMetaData::setIndexType(const int& index_type){
        _index_type = index_type;
    };

    void GeoMetaData::setIndexInfo(const OID& index_info){
        _index_info = index_info;
    };

    void GeoMetaData::setGType(const int& gtype){
        _gtype = gtype;
    };

    void GeoMetaData::setSRID(const int& srid){
        _srid = srid;
    };

    void GeoMetaData::setCRSType(const int& crs_type){
        _crs_type = crs_type;
    };

    void GeoMetaData::setTolerance(const double& tolerance){
        _tolerance = tolerance;
    };

    Status GeoMetaData::validate() const {
        return Status::OK();
    }

    std::string GeoMetaData::toString() const {
        return toConfigBSON().toString();
    }
}
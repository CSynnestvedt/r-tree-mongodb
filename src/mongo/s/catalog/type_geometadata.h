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

#pragma once

#include <boost/optional.hpp>
#include <string>

#include "mongo/bson/bsonobj.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/shard_id.h"
#include "mongo/s/catalog/type_chunk_base_gen.h"
#include "mongo/s/chunk_version.h"
#include "mongo/s/shard_key_pattern.h"
#include "mongo/stdx/type_traits.h"

namespace mongo {

class BSONObjBuilder;
class Status;
template <typename T>
class StatusWith;

class GeoMetaData {
public:
    // Name of the indexmeta collection in the config server.
    static const NamespaceString ConfigNS;

    // Might need for later, see commit 1ca58dc43dacd3bf4f6f01261d66585d85252703 in rtree repo
    // The shard chunks collections' common namespace prefix.
    // static const std::string ShardNSPrefix;

    // Field names and types in the chunks collections.
    static const BSONField<std::string> datanamespace;
    static const BSONField<std::string> column_name;
    static const BSONField<int> index_type;
    static const BSONField<OID> index_info;
    static const BSONField<int> gtype;
    static const BSONField<int> srid;
    static const BSONField<int> crs_type;
    static const BSONField<double> tolerance;

    GeoMetaData();
    GeoMetaData(std::string datanamespace, std::string column_name, int index_type, OID index_info, int gtype, int srid, int crs_type, double tolerance);

    /**
     * Returns OK if all the mandatory fields have been set. Otherwise returns NoSuchKey and
     * information about the first field that is missing.
     */
    Status validate() const;

    /**
     * Returns a std::string representation of the current internal state.
     */
    std::string toString() const;

        /**
     * Returns the BSON representation of the entry for the config server's config.chunks
     * collection.
     */
    BSONObj toConfigBSON() const;

        /**
     * Getters and setters.
     */
    const std::string& getDataNameSpace() const {
        return *_datanamespace;
    };   
    void setDataNameSpace(const std::string& id);

    const std::string& getColumnName() const {
        return *_column_name;
    };
    void setColumnName(const std::string& id);

    const int& getIndexType() const {
        return *_index_type;
    };
    void setIndexType(const int& id);

    const OID& getIndexInfo() const {
        return *_index_info;
    };
    void setIndexInfo(const OID& id);

    const int& getGType() const {
        return *_gtype;
    };
    void setGType(const int& id);

    const int& getSRID() const {
        return *_srid;
    };
    void setSRID(const int& id);

    const int& getCRSType() const {
        return *_crs_type;
    };
    void setCRSType(const int& id);

    const double& getTolerance() const {
        return *_tolerance;
    };
    void setTolerance(const double& id);

    private:
        // Convention: (M)andatory, (O)ptional, (S)pecial; (C)onfig, (S)hard.

    // (M)(C)(S)    auto-generated object id
    boost::optional<std::string> _datanamespace;
    // (M)(C)(S)    auto-generated object id
    boost::optional<std::string> _column_name;
    // (M)(C)(S)    auto-generated object id
    boost::optional<int> _index_type;
    // (M)(C)(S)    auto-generated object id
    boost::optional<OID> _index_info;
    // (M)(C)(S)    auto-generated object id
    boost::optional<int> _gtype;
    // (M)(C)(S)    auto-generated object id
    boost::optional<int> _srid;
    // (M)(C)(S)    auto-generated object id
    boost::optional<int> _crs_type;
    // (M)(C)(S)    auto-generated object id
    boost::optional<double> _tolerance;


};

}  // namespace mongo

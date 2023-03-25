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

#include "mongo/s/catalog/type_indexmetadata.h"

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
    const NamespaceString IndexMetaData::ConfigNS("config.meta_rtree");

    // Field names and types in the chunks collections.
    const BSONField<int> IndexMetaData::maxnode("maxnode");
    const BSONField<int> IndexMetaData::maxleaf("maxleaf");
    const BSONField<OID> IndexMetaData::root_key("root_key");


IndexMetaData::IndexMetaData() = default;

IndexMetaData::IndexMetaData(int maxnode, int maxleaf, OID root_key) 
 : 
_maxnode(std::move(maxnode)),
_maxleaf(std::move(maxleaf)),
_root_key(std::move(root_key))
{};


BSONObj IndexMetaData::toConfigBSON() const {
    BSONObjBuilder builder;
    if (_maxnode)
        builder.appendNumber(maxnode.name(), getMaxNode());
    if (_maxleaf)
        builder.appendNumber(maxleaf.name(), getMaxLeaf());
    if (_root_key)
        builder.append(root_key.name(), getRootKey());
    return builder.obj();
}


    void IndexMetaData::setMaxNode(const int& maxnode){
        _maxnode = maxnode;
    };

      void IndexMetaData::setMaxLeaf(const int& maxleaf){
        _maxleaf = maxleaf;
    };

    void IndexMetaData::setRootKey(const OID& root_key) {
        _root_key = root_key;
    }


    Status IndexMetaData::validate() const {
        return Status::OK();
    }

    std::string IndexMetaData::toString() const {
        return toConfigBSON().toString();
    }
}
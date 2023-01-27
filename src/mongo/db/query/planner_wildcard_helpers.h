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

#include "mongo/db/field_ref.h"
#include "mongo/db/index/multikey_paths.h"
#include "mongo/db/query/index_bounds_builder.h"
#include "mongo/db/query/index_entry.h"
#include "mongo/db/query/interval_evaluation_tree.h"
#include "mongo/db/query/query_solution.h"

namespace mongo {
namespace wildcard_planning {

using BoundsTightness = IndexBoundsBuilder::BoundsTightness;

/**
 * Specifies the maximum depth of nested array indices through which a query may traverse before a
 * $** index declines to answer it, due to the exponential complexity of the bounds required.
 */
static constexpr size_t kWildcardMaxArrayIndexTraversalDepth = 8u;

/**
 * Given a single wildcard index, and a set of fields which are being queried, create a 'mock'
 * IndexEntry for each of the query fields and add them into the provided vector.
 */
void expandWildcardIndexEntry(const IndexEntry& wildcardIndex,
                              const stdx::unordered_set<std::string>& fields,
                              std::vector<IndexEntry>* out);

/**
 * In certain circumstances, it is necessary to adjust the bounds and tightness generated by the
 * planner for $** indexes. For instance, if the query traverses through one or more arrays via
 * specific indices, then we must enforce INEXACT_FETCH to ensure correctness, regardless of the
 * predicate. Given an IndexEntry representing an expanded $** index, we apply any necessary
 * changes to the bounds, tightness, and interval evaluation tree here.
 */
BoundsTightness translateWildcardIndexBoundsAndTightness(
    const IndexEntry& index,
    BoundsTightness tightnessIn,
    OrderedIntervalList* oil,
    interval_evaluation_tree::Builder* ietBuilder);

/**
 * During planning, the expanded $** IndexEntry's keyPattern and bounds are in the single-field
 * format {'path': 1}. Once planning is complete, it is necessary to call this method in order to
 * prepare the IndexEntry and bounds for execution. This function performs the following actions:
 * - Converts the keyPattern to the {$_path: 1, "path": 1} format expected by the $** index.
 * - Adds a new entry '$_path' to the bounds vector, and computes the necessary intervals on it.
 * - Adds a new, empty entry to 'multikeyPaths' for '$_path'.
 * - Updates shouldDedup for index scan node.
 */
void finalizeWildcardIndexScanConfiguration(
    IndexScanNode* scan, std::vector<interval_evaluation_tree::Builder>* ietBuilders);

/**
 * Returns true if the given IndexScanNode is a $** scan whose bounds overlap the object type
 * bracket. Scans whose bounds include the object bracket have certain limitations for planning
 * purposes; for instance, they cannot provide covered results or be converted to DISTINCT_SCAN.
 */
bool isWildcardObjectSubpathScan(const IndexScanNode* node);

/**
 * Return true if the intervals on the 'value' field will include subobjects, and
 * thus require the bounds on $_path to include ["path.", "path/").
 */
bool requiresSubpathBounds(const OrderedIntervalList& intervals);

}  // namespace wildcard_planning
}  // namespace mongo

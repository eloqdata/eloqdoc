/**
 *    Copyright (C) 2025 EloqData Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the license:
 *    1. GNU Affero General Public License, version 3, as published by the Free
 *    Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */
#pragma once

#include "mongo/base/status.h"
#include "mongo/db/operation_context.h"

namespace mongo {

class IndexCatalog;
class NamespaceString;
struct InsertStatement;

namespace eloq_collection_helpers {

/**
 * Validates all unique index constraints before inserting into recordStore.
 * This prevents data inconsistency where recordStore has data but uniqueIndex doesn't.
 * Returns an error if any duplicate key is found in any unique index.
 * 
 * This function only performs validation for Eloq storage engine.
 * For other storage engines, it returns Status::OK() immediately.
 */
Status validateUniqueIndexConstraints(
    OperationContext* opCtx,
    const NamespaceString& ns,
    IndexCatalog* indexCatalog,
    std::vector<InsertStatement>::const_iterator begin,
    std::vector<InsertStatement>::const_iterator end);

}  // namespace eloq_collection_helpers
}  // namespace mongo


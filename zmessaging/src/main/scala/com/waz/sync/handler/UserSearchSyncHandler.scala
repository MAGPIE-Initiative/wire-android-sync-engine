/*
 * Wire
 * Copyright (C) 2016 Wire Swiss GmbH
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package com.waz.sync.handler

import com.waz.ZLog._
import com.waz.ZLog.ImplicitTag._
import com.waz.content.SearchQueryCacheStorage
import com.waz.model.SearchQuery
import com.waz.service.UserSearchService
import com.waz.sync.SyncResult
import com.waz.sync.client.UserSearchClient
import com.waz.threading.Threading

import scala.concurrent.Future
import scala.concurrent.Future.successful

class UserSearchSyncHandler(storage: SearchQueryCacheStorage, userSearch: UserSearchService, client: UserSearchClient) {
  import Threading.Implicits.Background

  def syncSearchQuery(query: SearchQuery): Future[SyncResult] = {
    debug(s"starting sync for: $query")
    client.getContacts(query).future flatMap {
      case Right(results) =>
        debug(s"searchSync, got: $results")
        userSearch.updateSearchResults(query, results).map(_ => SyncResult.Success)
      case Left(error) =>
        warn("graphSearch request failed")
        successful(SyncResult(error))
    }
  }
}

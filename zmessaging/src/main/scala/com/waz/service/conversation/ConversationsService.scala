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
package com.waz.service.conversation

import com.softwaremill.macwire._
import com.waz.ZLog.ImplicitTag._
import com.waz.ZLog._
import com.waz.api.ErrorType
import com.waz.api.IConversation.Access
import com.waz.api.impl.ErrorResponse
import com.waz.content._
import com.waz.model.ConversationData.{ConversationType, Link, getAccessAndRoleForGroupConv}
import com.waz.model._
import com.waz.service._
import com.waz.service.messages.{MessagesContentUpdater, MessagesService}
import com.waz.service.push.PushService
import com.waz.service.tracking.{GuestsAllowedToggled, TrackingService}
import com.waz.sync.client.ConversationsClient
import com.waz.sync.client.ConversationsClient.ConversationResponse
import com.waz.sync.{SyncRequestService, SyncServiceHandle}
import com.waz.threading.Threading
import com.waz.utils._
import com.waz.utils.events.EventContext
import com.waz.znet.ZNetClient.ErrorOr

import scala.collection.{breakOut, mutable}
import scala.concurrent.Future
import scala.concurrent.Future.successful
import scala.util.control.{NoStackTrace, NonFatal}

trait ConversationsService {
  def content: ConversationsContentUpdater
  def convStateEventProcessingStage: EventScheduler.Stage
  def processConversationEvent(ev: ConversationStateEvent, selfUserId: UserId, retryCount: Int = 0): Future[Any]
  def getSelfConversation: Future[Option[ConversationData]]
  def updateConversations(conversations: Seq[ConversationResponse]): Future[Seq[ConversationData]]
  def setConversationArchived(id: ConvId, archived: Boolean): Future[Option[ConversationData]]
  def forceNameUpdate(id: ConvId): Future[Option[(ConversationData, ConversationData)]]
  def onMemberAddFailed(conv: ConvId, users: Set[UserId], error: Option[ErrorType], resp: ErrorResponse): Future[Unit]
  def isGroupConversation(convId: ConvId): Future[Boolean]
  def isWithService(convId: ConvId): Future[Boolean]

  def setToTeamOnly(convId: ConvId, teamOnly: Boolean): ErrorOr[Unit]
  def createLink(convId: ConvId): ErrorOr[Link]
  def removeLink(convId: ConvId): ErrorOr[Unit]
}

class ConversationsServiceImpl(teamId:          Option[TeamId],
                               selfUserId:      UserId,
                               push:            PushService,
                               users:           UserService,
                               usersStorage:    UsersStorage,
                               membersStorage:  MembersStorage,
                               convsStorage:    ConversationStorage,
                               val content:     ConversationsContentUpdater,
                               sync:            SyncServiceHandle,
                               errors:          ErrorsService,
                               messages:        MessagesService,
                               msgContent:      MessagesContentUpdater,
                               userPrefs:       UserPreferences,
                               requests:        SyncRequestService,
                               eventScheduler:  => EventScheduler,
                               tracking:        TrackingService,
                               client:          ConversationsClient,
                               stats:           ConversationsListStateService,
                               syncReqService:  SyncRequestService) extends ConversationsService {

  private implicit val ev = EventContext.Global
  import Threading.Implicits.Background

  private val nameUpdater = wire[NameUpdater]
  nameUpdater.registerForUpdates()

  //On conversation changed, update the state of the access roles as part of migration, then check for a link if necessary
  stats.selectedConversationId {
    case Some(convId) => convsStorage.get(convId).flatMap {
      case Some(conv) if conv.accessRole.isEmpty =>
        for {
          syncId <- sync.syncConversations(Set(conv.id))
          _      <- syncReqService.scheduler.await(syncId)
          Some(updated) <- content.convById(conv.id)
        } yield if (updated.access.contains(Access.CODE)) sync.syncConvLink(conv.id)

      case _ => Future.successful({})
    }
    case None => //
  }

  val convStateEventProcessingStage = EventScheduler.Stage[ConversationStateEvent] { (_, events) =>
    RichFuture.processSequential(events)(processConversationEvent(_, selfUserId))
  }

  push.onHistoryLost { req =>
    verbose(s"onSlowSyncNeeded($req)")
    // TODO: this is just very basic implementation creating empty message
    // This should be updated to include information about possibly missed changes
    // this message will be shown rarely (when notifications stream skips data)
    convsStorage.list.flatMap(messages.addHistoryLostMessages(_, selfUserId))
  }

  errors.onErrorDismissed {
    case ErrorData(_, ErrorType.CANNOT_CREATE_GROUP_CONVERSATION_WITH_UNCONNECTED_USER, _, _, Some(convId), _, _, _, _) =>
      deleteConversation(convId)
    case ErrorData(_, ErrorType.CANNOT_ADD_UNCONNECTED_USER_TO_CONVERSATION, userIds, _, Some(convId), _, _, _, _) => Future.successful(())
    case ErrorData(_, ErrorType.CANNOT_ADD_USER_TO_FULL_CONVERSATION, userIds, _, Some(convId), _, _, _, _) => Future.successful(())
    case ErrorData(_, ErrorType.CANNOT_SEND_MESSAGE_TO_UNVERIFIED_CONVERSATION, _, _, Some(conv), _, _, _, _) =>
      convsStorage.setUnknownVerification(conv)
  }

  def processConversationEvent(ev: ConversationStateEvent, selfUserId: UserId, retryCount: Int = 0) = ev match {
    case CreateConversationEvent(rConvId, time, from, data) =>
      updateConversations(selfUserId, Seq(data)) flatMap { case (_, created) => Future.traverse(created) (created =>
        messages.addConversationStartMessage(created.id, from, (data.members.map(_.userId).toSet + selfUserId).filter(_ != from), created.name)
      )}

    case ConversationEvent(rConvId, _, _) =>
      content.convByRemoteId(rConvId) flatMap {
        case Some(conv) => processUpdateEvent(conv, ev)
        case None if retryCount > 3 =>
          tracking.exception(new Exception("No conversation data found for event") with NoStackTrace, "No conversation data found for event")
          successful(())
        case None =>
          ev match {
            case MemberJoinEvent(_, time, from, members, _) if from != selfUserId =>
              // this happens when we are added to group conversation
              for {
                conv <- convsStorage.insert(ConversationData(ConvId(), rConvId, None, from, ConversationType.Group, lastEventTime = time.instant))
                ms   <- membersStorage.add(conv.id, from +: members)
                _    <- messages.addMemberJoinMessage(conv.id, from, members.toSet)
                _    <- sync.syncConversations(Set(conv.id))
              } yield {}

            case _ =>
              warn(s"No conversation data found for event: $ev on try: $retryCount")
              content.processConvWithRemoteId(rConvId, retryAsync = true) { processUpdateEvent(_, ev) }
          }
      }
  }

  private def processUpdateEvent(conv: ConversationData, ev: ConversationEvent) = ev match {
    case RenameConversationEvent(_, _, _, name) => content.updateConversationName(conv.id, name)

    case MemberJoinEvent(_, _, _, userIds, _) =>
      def joined(updated: ConversationData) = !conv.isActive && updated.isActive && updated.convType == ConversationType.Group
      def ensureConvActive() = content.setConvActive(conv.id, active = true).map(_.map(_._2).filter(joined))

      for {
        syncId <- users.syncNotExistingOrExpired(userIds)
        _ <- syncId.fold(Future.successful(()))(sId => syncReqService.scheduler.await(sId).map(_ => ()))
        _ <- membersStorage.add(conv.id, userIds)
        _ <- if (userIds.contains(selfUserId)) ensureConvActive() else successful(None)
      } yield ()

    case MemberLeaveEvent(_, _, _, userIds) =>
      membersStorage.remove(conv.id, userIds) flatMap { _ =>
        if (userIds.contains(selfUserId)) content.setConvActive(conv.id, active = false)
        else successful(())
      }

    case MemberUpdateEvent(_, _, _, state) => content.updateConversationState(conv.id, state)

    case ConnectRequestEvent(_, _, from, _, recipient, _, _) =>
      debug(s"ConnectRequestEvent(from = $from, recipient = $recipient")
      membersStorage.add(conv.id, Set(from, recipient)).flatMap { added =>
        val userIdsAdded = added map (_.userId)
        usersStorage.listAll(userIdsAdded) map { localUsers =>
          users.syncIfNeeded(localUsers: _*)
          sync.syncUsersIfNotEmpty(userIdsAdded.filterNot(id => localUsers exists (_.id == id)).toSeq)
        }
      }

    case ConversationAccessEvent(_, _, _, access, accessRole) =>
      content.updateAccessMode(conv.id, access, Some(accessRole))

    case ConversationCodeUpdateEvent(_, _, _, l) =>
      convsStorage.update(conv.id, _.copy(link = Some(l)))

    case ConversationCodeDeleteEvent(_, _, _) =>
      convsStorage.update(conv.id, _.copy(link = None))

    case _ => successful(())
  }

  def getSelfConversation = {
    val selfConvId = ConvId(selfUserId.str)
    content.convById(selfConvId).flatMap {
      case Some(c) => successful(Some(c))
      case _ =>
        for {
          user  <- usersStorage.get(selfUserId)
          conv  =  ConversationData(ConvId(selfUserId.str), RConvId(selfUserId.str), None, selfUserId, ConversationType.Self, generatedName = user.map(_.name).getOrElse(""))
          saved <- convsStorage.getOrCreate(selfConvId, conv).map(Some(_))
        } yield saved
    }
  }

  def updateConversations(conversations: Seq[ConversationResponse]) = Future.traverse(conversations) { conv =>
    eventScheduler.post(conv.conversation.remoteId) {
      updateConversations(selfUserId, Seq(conv)) flatMap { case (all, created) =>
        messages.addDeviceStartMessages(created, selfUserId) map (_ => all)
      }
    }
  }.map { vs =>
    verbose(s"updated conversations: ${vs.flatten}")
    vs.foldLeft(Vector.empty[ConversationData])(_ ++ _)
  }

  private def updateConversations(selfUserId: UserId, convs: Seq[ConversationResponse]): Future[(Seq[ConversationData], Seq[ConversationData])] = {

    def updateConversationData() = {
      def createOneToOne(conv: ConversationData, members: Seq[ConversationMemberData]) =
        if (members.size > 1) conv.copy(id = oneToOneLocalId(members, selfUserId)) else conv.copy(hidden = true)

      def oneToOneLocalId(members: Seq[ConversationMemberData], selfUserId: UserId) = members.find(_.userId != selfUserId).fold(ConvId())(m => ConvId(m.userId.str))

      def findExistingId = convsStorage { (convById, remoteMap) =>
        def byRemoteId(id: RConvId) = returning(remoteMap.get(id).flatMap(convById.get)) { res => verbose(s"byRemoteId($id) - $res")}

        convs map { case ConversationResponse(conv, members) =>
          val newId = if (ConversationType.isOneToOne(conv.convType)) oneToOneLocalId(members, selfUserId) else conv.id

          val matching = byRemoteId(conv.remoteId).orElse {
            convById.get(newId) orElse {
              if (ConversationType.isOneToOne(conv.convType)) None
              else byRemoteId(ConversationsService.generateTempConversationId((members.map(_.userId) :+ selfUserId).distinct))
            }
          }

          (conv.copy(id = matching.fold(newId)(_.id)), members)
        }
      }

      var created = new mutable.HashSet[ConversationData]

      def updater(conv: ConversationData, members: Seq[ConversationMemberData]): (Option[ConversationData] => ConversationData) = {
        case Some(old) => old.updated(conv).getOrElse(old)
        case None if ConversationType.isOneToOne(conv.convType) => returning(createOneToOne(conv, members))(created += _)
        case None =>
          created += conv
          conv
      }

      for {
        withId <- findExistingId
        convs  <- convsStorage.updateOrCreateAll(withId.map { case (conv, members) => conv.id -> updater(conv, members) } (breakOut))
      } yield (convs, created.toSeq)
    }

    def updateMembers() = Future.sequence(convs map {
        case ConversationResponse(conv, members) =>
          content.convByRemoteId(conv.remoteId) flatMap {
            case Some(c) => membersStorage.set(c.id, selfUserId +: members.map(_.userId))
            case _ =>
              error(s"updateMembers() didn't find conv with given remote id for: $conv")
              successful(())
          }
      })

    def syncUsers() = users.syncNotExistingOrExpired(convs.flatMap { case ConversationResponse(_, members) => members.map(_.userId) })

    for {
      (convs, created) <- updateConversationData()
      _                <- updateMembers()
      _                <- syncUsers()
    } yield
      (convs.toSeq, created)
  }

  def setConversationArchived(id: ConvId, archived: Boolean) = content.updateConversationArchived(id, archived) flatMap {
    case Some((_, conv)) =>
      sync.postConversationState(id, ConversationState(archived = Some(conv.archived), archiveTime = Some(conv.archiveTime))) map { _ => Some(conv) }
    case None =>
      Future successful None
  }

  private def deleteConversation(convId: ConvId) = for {
    _ <- convsStorage.remove(convId)
    _ <- membersStorage.delete(convId)
    _ <- msgContent.deleteMessagesForConversation(convId: ConvId)
  } yield ()

  def forceNameUpdate(id: ConvId) = {
    warn(s"forceNameUpdate($id)")
    nameUpdater.forceNameUpdate(id)
  }

  def onMemberAddFailed(conv: ConvId, users: Set[UserId], error: Option[ErrorType], resp: ErrorResponse) = for {
    _ <- error.fold(Future.successful({}))(e => errors.addErrorWhenActive(ErrorData(e, resp, conv, users)).map(_ => {}))
    _ <- membersStorage.remove(conv, users)
    _ <- messages.removeLocalMemberJoinMessage(conv, users)
  } yield ()

  def isGroupConversation(convId: ConvId) =
    for {
      Some(conv) <- convsStorage.get(convId)
      res <-
        if (conv.convType != ConversationType.Group) Future.successful(false)
        else if (conv.name.isDefined || conv.team.isEmpty) Future.successful(true)
        else membersStorage.getActiveUsers(convId).map(ms => !(ms.contains(selfUserId) && ms.size == 2))
    } yield res

  def isWithService(convId: ConvId) =
    membersStorage.getActiveUsers(convId)
      .flatMap(usersStorage.getAll)
      .map(_.flatten.exists(_.isWireBot))

  def setToTeamOnly(convId: ConvId, teamOnly: Boolean) =
    teamId match {
      case None => Future.successful(Left(ErrorResponse.internalError("Private accounts can't be set to team-only or guest room access modes")))
      case Some(_) =>
        (for {
          true <- isGroupConversation(convId)
          _ = tracking.track(GuestsAllowedToggled(!teamOnly))
          (ac, ar) = getAccessAndRoleForGroupConv(teamOnly, teamId)
          Some((old, upd)) <- content.updateAccessMode(convId, ac, Some(ar))
          resp <-
            if (old.access != upd.access || old.accessRole != upd.accessRole) {
              client.postAccessUpdate(upd.remoteId, ac, ar)
            }.future.flatMap {
              case Right(_) => Future.successful(Right {})
              case Left(err) =>
                //set mode back on request failed
                content.updateAccessMode(convId, old.access, old.accessRole, old.link).map(_ => Left(err))
            }
            else Future.successful(Right {})
        } yield resp).recover {
          case NonFatal(e) =>
            warn("Unable to set team only mode on conversation", e)
            Left(ErrorResponse.internalError("Unable to set team only mode on conversation"))
        }
    }

  override def createLink(convId: ConvId) =
    (for {
      Some(conv) <- content.convById(convId) if conv.isGuestRoom || conv.isWirelessLegacy
      modeResp   <- if (conv.isWirelessLegacy) setToTeamOnly(convId, teamOnly = false) else Future.successful(Right({})) //upgrade legacy convs
      linkResp   <- modeResp match {
        case Right(_) => client.createLink(conv.remoteId).future
        case Left(err) => Future.successful(Left(err))
      }
      _ <- linkResp match {
        case Right(l) => convsStorage.update(convId, _.copy(link = Some(l)))
        case _ => Future.successful({})
      }
    } yield linkResp)
      .recover {
        case NonFatal(e) =>
          error("Failed to create link", e)
          Left(ErrorResponse.internalError("Unable to create link for conversation"))
      }

  override def removeLink(convId: ConvId) =
    (for {
      Some(conv) <- content.convById(convId)
      resp       <- client.removeLink(conv.remoteId).future
      _ <- resp match {
        case Right(_) => convsStorage.update(convId, _.copy(link = None))
        case _ => Future.successful({})
      }
    } yield resp)
      .recover {
        case NonFatal(e) =>
          error("Failed to remove link", e)
          Left(ErrorResponse.internalError("Unable to remove link for conversation"))
      }
}

object ConversationsService {
  import scala.concurrent.duration._

  val RetryBackoff = new ExponentialBackoff(500.millis, 3.seconds)

  /**
   * Generate temp ConversationID to identify conversations which don't have a RConvId yet
   */
  def generateTempConversationId(users: Seq[UserId]) =
    RConvId(users.map(_.toString).sorted.foldLeft("")(_ + _))
}

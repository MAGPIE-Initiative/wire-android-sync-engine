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
package com.waz.content

import com.waz.ZLog._
import com.waz.api.{Message, MessageFilter}
import com.waz.api.Message.Status
import com.waz.content.ConvMessagesIndex._
import com.waz.model.MessageData.{MessageDataDao, isUserContent}
import com.waz.model._
import com.waz.service.tracking.TrackingService
import com.waz.threading.{CancellableFuture, SerialDispatchQueue}
import com.waz.utils._
import com.waz.utils.events.{EventStream, RefreshingSignal, Signal, SourceSignal}
import org.threeten.bp.Instant

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._

class ConvMessagesIndex(conv: ConvId, messages: MessagesStorageImpl, selfUserId: UserId, users: UsersStorage,
                        convs: ConversationStorage, msgAndLikes: MessageAndLikesStorage, storage: ZmsDatabase,
                        tracking: TrackingService, filter: Option[MessageFilter] = None) { self =>

  private implicit val tag: LogTag = s"ConvMessagesIndex_$conv"

  import com.waz.utils.events.EventContext.Implicits.global
  private implicit val dispatcher = new SerialDispatchQueue(name = "ConvMessagesIndex")

  private val indexChanged = EventStream[Change]()
  private val lastLocalMessageByType = new mutable.HashMap[Message.Type, MessageData]
  private var firstMessage = Option.empty[MessageData]

  private object sources {
    val failedCount = Signal(0)
    val missedCall = Signal(Option.empty[MessageData])
    val incomingKnock = Signal(Option.empty[MessageData])
    val lastMessage = Signal(Option.empty[MessageData])
    val lastSentMessage = Signal(Option.empty[MessageData])
    val lastReadTime = returning(Signal[Instant]())(_.disableAutowiring())
    val lastMessageFromSelf = Signal(Option.empty[MessageData])
    val lastMessageFromOther = Signal(Option.empty[MessageData])
  }

  object signals {
    val indexChanged: EventStream[Change] = self.indexChanged

    val lastReadTime: Signal[Instant] = sources.lastReadTime
    val failedCount: Signal[Int] = sources.failedCount
    val lastMissedCall: Signal[Option[MessageId]] = Signal(sources.lastReadTime, sources.missedCall).map { case (time, msg) => msg.filter(_.time.isAfter(time)).map(_.id) }
    val incomingKnock: Signal[Option[MessageId]] = Signal(sources.lastReadTime, sources.incomingKnock).map { case (time, msg) => msg.filter(_.time.isAfter(time)).map(_.id) }
    val lastMessage: Signal[Option[MessageData]] = returning(sources.lastMessage)(_.disableAutowiring())
    val lastSentMessage: Signal[Option[MessageData]] = returning(sources.lastSentMessage)(_.disableAutowiring())
    val lastMessageFromSelf: Signal[Option[MessageData]] = returning(sources.lastMessageFromSelf)(_.disableAutowiring())
    val lastMessageFromOther: Signal[Option[MessageData]] = returning(sources.lastMessageFromOther)(_.disableAutowiring())

    lastMessageFromSelf { msg =>
      info(s"last message from self is now $msg")
    }

    val unreadCount = for {
      time <- sources.lastReadTime
      _ <- Signal.wrap(Instant.now, indexChanged.map(_.time)).throttle(500.millis)
      unread <- Signal.future(messages.countUnread(conv, time))
    } yield unread

    val messagesCursor: Signal[MessagesCursor] = new RefreshingSignal(loadCursor, indexChanged.filter(_.orderChanged))
  }

  import sources._

  private val init = convs.get(conv).flatMap { c =>
    c foreach updateLastRead

    storage.read { implicit db =>
      logTime(s"Initial load conversation entries for: $conv") {

        lastLocalMessageByType ++= MessageDataDao.listLocalMessages(conv).groupBy(m => m.msgType).map {
          case (tpe, msgs) => tpe -> msgs.maxBy(_.time)
        }
        missedCall ! MessageDataDao.lastMissedCall(conv)
        incomingKnock ! MessageDataDao.lastIncomingKnock(conv, selfUserId)
        failedCount ! MessageDataDao.countFailed(conv).toInt
        firstMessage = MessageDataDao.first(conv)
        lastMessage ! MessageDataDao.last(conv)
        lastSentMessage ! MessageDataDao.lastSent(conv)
        lastMessageFromSelf ! MessageDataDao.lastFromSelf(conv, selfUserId)
        lastMessageFromOther ! MessageDataDao.lastFromOther(conv, selfUserId)
      }
    }.map { _ =>
      Signal(signals.unreadCount, signals.failedCount, signals.lastMissedCall, signals.incomingKnock).throttle(500.millis) { case (unread, failed, missed, knock) =>
        verbose(s"update conversation state: unread: $unread, failed: $failed, missed: $missed, knock: $knock")
        convs.update(conv, _.copy(incomingKnockMessage = knock, missedCallMessage = missed, unreadCount = unread, failedCount = failed))
      }
    }
  }.recoverWithLog(reportHockey = true)
  def updateLastRead(c: ConversationData) = lastReadTime.mutateOrDefault(_ max c.lastRead, c.lastRead)

  private[waz] def loadCursor = CancellableFuture.lift(init) flatMap { _ =>
    verbose(s"loadCursor for $conv")
    storage { implicit db =>
      val (cursor, order) = filter match {
          //TODO: this ignores other filter types if the content query is on. they should all be considered...
        case Some(MessageFilter(_, Some(query), _)) => (MessageContentIndexDao.findContent(query, Some(conv)), MessagesCursor.Descending)
        case Some(MessageFilter(Some(types), _, limit)) => (MessageDataDao.msgIndexCursorFiltered(conv, types, limit), MessagesCursor.Descending)
        case _ => (MessageDataDao.msgIndexCursor(conv), MessagesCursor.Ascending)
      }
      val time = lastReadTime.currentValue.getOrElse(Instant.EPOCH)
      val readMessagesCount = MessageDataDao.countAtLeastAsOld(conv, time).toInt
      verbose(s"index of $time = $readMessagesCount")
      (cursor, order, time, math.max(0, readMessagesCount - 1))
    } ("ConvMessageIndex_loadCursor") map { case (cursor, order, time, lastReadIndex) =>
      new MessagesCursor(cursor, lastReadIndex, time, msgAndLikes, tracking)(order)
    }
  }

  def getLastMessage = init.map { _ => signals.lastMessage.currentValue.flatten }

  def getLastSentMessage = init.map { _ => signals.lastSentMessage.currentValue.flatten }

  def lastLocalMessage(tpe: Message.Type) = init.map { _ => lastLocalMessageByType.get(tpe).map(_.id) }

  def firstMessageId = init.map { _ => firstMessage.map(_.id) }

  private[content] def delete(msg: MessageData): Future[Unit] = init map { _ =>
    if (msg.state.isFailed) failedCount.mutate(c => math.max(c - 1, 0))

    if (msg.isLocal && lastLocalMessageByType.get(msg.msgType).exists(_.id == msg.id))
      lastLocalMessageByType.remove(msg.msgType)

    removeLast(_.id == msg.id)

    indexChanged ! Removed(msg)
  }

  private[content] def delete(upTo: Instant = Instant.MAX): Future[Unit] = init map { _ =>
    failedCount ! 0 // XXX: this might be wrong, hopefully not too often

    lastLocalMessageByType.filter { case (_, index) => !index.time.isAfter(upTo) } foreach { case (k, _) => lastLocalMessageByType.remove(k) }
    firstMessage = firstMessage.filter(_.time.isAfter(upTo))
    incomingKnock.mutate(_.filter(_.time.isAfter(upTo)))

    removeLast(!_.time.isAfter(upTo))

    indexChanged ! RemovedOlder(upTo)
  }

  private def removeLast(f: MessageData => Boolean) = {
    val lastRemoved = lastMessage.mutate(_ filterNot f)
    val lastSentRemoved = lastSentMessage.mutate(_ filterNot f)
    val lastFromSelfRemoved = lastMessageFromSelf.mutate(_ filterNot f)
    val lastFromOtherRemoved = lastMessageFromOther.mutate(_ filterNot f)

    if (lastRemoved || lastSentRemoved || lastFromSelfRemoved) {
      verbose("last message was removed, need to fetch it from db")
      storage.read { implicit db =>
        MessageDataDao.last(conv) foreach updateSignal(lastMessage)
        MessageDataDao.lastSent(conv) foreach updateSignal(lastSentMessage)
        MessageDataDao.lastFromSelf(conv, selfUserId) foreach updateSignal(lastMessageFromSelf)
        MessageDataDao.lastFromOther(conv, selfUserId) foreach updateSignal(lastMessageFromOther)
      }
    }
  }

  private[content] def add(msgs: Seq[MessageData]): Future[Unit] = init map { _ =>
    msgs foreach { msg =>
      verbose(s"add($msg), last: ${lastMessage.currentValue}")

      if (msg.isLocal && lastLocalMessageByType.get(msg.msgType).forall(_.time.isBefore(msg.time)))
        lastLocalMessageByType(msg.msgType) = msg

      if (!msg.isLocal) {
        if (msg.msgType == Message.Type.MISSED_CALL) missedCall.mutate(_.filter(_.time.isAfter(msg.time)).orElse(Some(msg)))
        if (msg.msgType == Message.Type.KNOCK && msg.userId != selfUserId) incomingKnock.mutate(_.filter(_.time.isAfter(msg.time)).orElse(Some(msg)))
      }
    }

    if (msgs.nonEmpty) {
      val firstIter = msgs.iterator.filter(m => MessagesStorage.FirstMessageTypes(m.msgType))
      if (firstIter.nonEmpty) {
        val first = firstIter.minBy(_.time)
        if (firstMessage.forall(_.time.isAfter(first.time)))
          firstMessage = Some(first)
      }

      val failed = msgs.count(_.state == Status.FAILED)
      if (failed > 0) failedCount.mutate(_ + failed)

      updateLast(msgs)

      indexChanged ! Added(msgs)
    }
  }

  private[content] def update(updates: Seq[(MessageData, MessageData)]): Future[Unit] = init map { _ =>
    updates foreach { case (msg, updated) =>
      verbose(s"update($msg, $updated)")
      assert(msg.id == updated.id, "trying to change message id")

      if (msg.isLocal && !updated.isLocal && lastLocalMessageByType.get(msg.msgType).exists(_.id == msg.id))
        lastLocalMessageByType.remove(msg.msgType)
      else if (updated.isLocal && !msg.isLocal)
        debug(s"non-local message was updated to local: $msg -> $updated")
    }

    if (updates.nonEmpty) {
      val failed = updates.foldLeft(0) { case (count, (p, u)) =>
        if (p.state.isFailed == u.state.isFailed) count
        else if (u.state.isFailed) count + 1 else count - 1
      }
      if (failed != 0) failedCount.mutate(_ + failed)

      updateLast(updates.view.map(_._2))

      indexChanged ! Updated(updates)
    }
  }

  private def updateLast(msgs: Iterable[MessageData]): Unit = {
    updateSignal(lastMessage)(msgs.maxBy(_.time))

    val sent = msgs.filter(m => m.state == Status.SENT || m.state == Status.DELIVERED)
    if (sent.nonEmpty) updateSignal(lastSentMessage)(sent.maxBy(_.time))

    val (fromSelf, fromOther) = msgs.filter(m => isUserContent(m.msgType)).partition(_.userId == selfUserId)
    if (fromSelf.nonEmpty) updateSignal(lastMessageFromSelf)(fromSelf.maxBy(_.time))
    if (fromOther.nonEmpty) updateSignal(lastMessageFromOther)(fromOther.maxBy(_.time))
  }

  private def updateSignal(signal: SourceSignal[Option[MessageData]])(last: MessageData): Unit =
    signal.mutate(_.filter(m => m.id != last.id && m.time.isAfter(last.time)).orElse(Some(last)))
}

object ConvMessagesIndex {

  sealed trait Change {
    val time: Instant = Instant.now
    val orderChanged = true
  }
  case class Added(msgs: Seq[MessageData]) extends Change
  case class Removed(msg: MessageData) extends Change
  case class RemovedOlder(clearTimestamp: Instant) extends Change
  case class Updated(updates: Seq[(MessageData, MessageData)]) extends Change {
    override val orderChanged = updates exists { case (prev, up) => prev.time != up.time || !prev.hasSameContentType(up) }
  }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import java.io.File
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import com.yammer.metrics.core.Gauge
import kafka.api._
import kafka.cluster.{Partition, Replica}
import kafka.controller.KafkaController
import kafka.log.{Log, LogAppendInfo, LogManager}
import kafka.metrics.KafkaMetricsGroup
import kafka.server.QuotaFactory.UnboundedQuota
import kafka.server.checkpoints.OffsetCheckpointFile
import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{KafkaStorageException, ControllerMovedException, CorruptRecordException, InvalidTimestampException, InvalidTopicException, NotEnoughReplicasException, NotLeaderForPartitionException, OffsetOutOfRangeException, PolicyViolationException, _}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.Errors.UNKNOWN_TOPIC_OR_PARTITION
import org.apache.kafka.common.protocol.Errors.KAFKA_STORAGE_ERROR
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.EpochEndOffset._
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.requests.FetchResponse.AbortedTransaction
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests.{DeleteRecordsRequest, DeleteRecordsResponse, LeaderAndIsrRequest, StopReplicaRequest, UpdateMetadataRequest, _}
import org.apache.kafka.common.utils.Time

import scala.collection.JavaConverters._
import scala.collection._

/*
 * Result metadata of a log append operation on the log
 */
case class LogAppendResult(info: LogAppendInfo, exception: Option[Throwable] = None) {
  def error: Errors = exception match {
    case None => Errors.NONE
    case Some(e) => Errors.forException(e)
  }
}

case class LogDeleteRecordsResult(requestedOffset: Long, lowWatermark: Long, exception: Option[Throwable] = None) {
  def error: Errors = exception match {
    case None => Errors.NONE
    case Some(e) => Errors.forException(e)
  }
}

/*
 * Result metadata of a log read operation on the log
 * @param info @FetchDataInfo returned by the @Log read
 * @param hw high watermark of the local replica
 * @param readSize amount of data that was read from the log i.e. size of the fetch
 * @param isReadFromLogEnd true if the request read up to the log end offset snapshot
 *                         when the read was initiated, false otherwise
 * @param error Exception if error encountered while reading from the log
 */
case class LogReadResult(info: FetchDataInfo,
                         highWatermark: Long,
                         leaderLogStartOffset: Long,
                         leaderLogEndOffset: Long,
                         followerLogStartOffset: Long,
                         fetchTimeMs: Long,
                         readSize: Int,
                         lastStableOffset: Option[Long],
                         exception: Option[Throwable] = None) {

  def error: Errors = exception match {
    case None => Errors.NONE
    case Some(e) => Errors.forException(e)
  }

  override def toString =
    s"Fetch Data: [$info], HW: [$highWatermark], leaderLogStartOffset: [$leaderLogStartOffset], leaderLogEndOffset: [$leaderLogEndOffset], " +
    s"followerLogStartOffset: [$followerLogStartOffset], fetchTimeMs: [$fetchTimeMs], readSize: [$readSize], error: [$error]"

}

case class FetchPartitionData(error: Errors = Errors.NONE,
                              highWatermark: Long,
                              logStartOffset: Long,
                              records: Records,
                              lastStableOffset: Option[Long],
                              abortedTransactions: Option[List[AbortedTransaction]])

object LogReadResult {
  val UnknownLogReadResult = LogReadResult(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
                                           highWatermark = -1L,
                                           leaderLogStartOffset = -1L,
                                           leaderLogEndOffset = -1L,
                                           followerLogStartOffset = -1L,
                                           fetchTimeMs = -1L,
                                           readSize = -1,
                                           lastStableOffset = None)
}

case class BecomeLeaderOrFollowerResult(responseMap: collection.Map[TopicPartition, Errors], error: Errors) {

  override def toString = {
    "update results: [%s], global error: [%d]".format(responseMap, error.code)
  }
}

object ReplicaManager {
  val HighWatermarkFilename = "replication-offset-checkpoint"
  val IsrChangePropagationBlackOut = 5000L
  val IsrChangePropagationInterval = 60000L
  val OfflinePartition = new Partition("", -1, null, null, isOffline = true)
}

class ReplicaManager(val config: KafkaConfig,
                     metrics: Metrics,
                     time: Time,
                     val zkUtils: ZkUtils,
                     scheduler: Scheduler,
                     val logManager: LogManager,
                     val isShuttingDown: AtomicBoolean,
                     quotaManager: ReplicationQuotaManager,
                     val brokerTopicStats: BrokerTopicStats,
                     val metadataCache: MetadataCache,
                     logDirFailureChannel: LogDirFailureChannel,
                     val delayedProducePurgatory: DelayedOperationPurgatory[DelayedProduce],
                     val delayedFetchPurgatory: DelayedOperationPurgatory[DelayedFetch],
                     val delayedDeleteRecordsPurgatory: DelayedOperationPurgatory[DelayedDeleteRecords],
                     threadNamePrefix: Option[String]) extends Logging with KafkaMetricsGroup {

  def this(config: KafkaConfig,
           metrics: Metrics,
           time: Time,
           zkUtils: ZkUtils,
           scheduler: Scheduler,
           logManager: LogManager,
           isShuttingDown: AtomicBoolean,
           quotaManager: ReplicationQuotaManager,
           brokerTopicStats: BrokerTopicStats,
           metadataCache: MetadataCache,
           logDirFailureChannel: LogDirFailureChannel,
           threadNamePrefix: Option[String] = None) {
    this(config, metrics, time, zkUtils, scheduler, logManager, isShuttingDown,
      quotaManager, brokerTopicStats, metadataCache, logDirFailureChannel,
      DelayedOperationPurgatory[DelayedProduce](
        purgatoryName = "Produce", brokerId = config.brokerId,
        purgeInterval = config.producerPurgatoryPurgeIntervalRequests),
      DelayedOperationPurgatory[DelayedFetch](
        purgatoryName = "Fetch", brokerId = config.brokerId,
        purgeInterval = config.fetchPurgatoryPurgeIntervalRequests),
      DelayedOperationPurgatory[DelayedDeleteRecords](
        purgatoryName = "DeleteRecords", brokerId = config.brokerId,
        purgeInterval = config.deleteRecordsPurgatoryPurgeIntervalRequests),
      threadNamePrefix)
  }

  /* epoch of the controller that last changed the leader */
  @volatile var controllerEpoch: Int = KafkaController.InitialControllerEpoch - 1
  private val localBrokerId = config.brokerId
  private val allPartitions = new Pool[TopicPartition, Partition](valueFactory = Some(tp =>
    new Partition(tp.topic, tp.partition, time, this)))
  private val replicaStateChangeLock = new Object
  val replicaFetcherManager = createReplicaFetcherManager(metrics, time, threadNamePrefix, quotaManager)
  private val highWatermarkCheckPointThreadStarted = new AtomicBoolean(false)
  @volatile var highWatermarkCheckpoints = logManager.liveLogDirs.map(dir =>
    (dir.getAbsolutePath, new OffsetCheckpointFile(new File(dir, ReplicaManager.HighWatermarkFilename), logDirFailureChannel))).toMap

  private var hwThreadInitialized = false
  this.logIdent = "[Replica Manager on Broker " + localBrokerId + "]: "
  val stateChangeLogger = KafkaController.stateChangeLogger
  private val isrChangeSet: mutable.Set[TopicPartition] = new mutable.HashSet[TopicPartition]()
  private val lastIsrChangeMs = new AtomicLong(System.currentTimeMillis())
  private val lastIsrPropagationMs = new AtomicLong(System.currentTimeMillis())
  private var logDirFailureHandler: LogDirFailureHandler = null

  private class LogDirFailureHandler(name: String, haltBrokerOnDirFailure: Boolean) extends ShutdownableThread(name) {
    override def doWork() {
      val newOfflineLogDir = logDirFailureChannel.takeNextOfflineLogDir()
      if (haltBrokerOnDirFailure) {
        fatal(s"Halting broker because dir $newOfflineLogDir is offline")
        Exit.halt(1)
      }
      handleLogDirFailure(newOfflineLogDir)
    }
  }

  val leaderCount = newGauge(
    "LeaderCount",
    new Gauge[Int] {
      def value = getLeaderPartitions.size
    }
  )
  val partitionCount = newGauge(
    "PartitionCount",
    new Gauge[Int] {
      def value = allPartitions.size
    }
  )
  val offlineReplicaCount = newGauge(
    "OfflineReplicaCount",
    new Gauge[Int] {
      def value = allPartitions.values.count(_ eq ReplicaManager.OfflinePartition)
    }
  )
  val underReplicatedPartitions = newGauge(
    "UnderReplicatedPartitions",
    new Gauge[Int] {
      def value = underReplicatedPartitionCount
    }
  )

  val underMinIsrPartitionCount = newGauge(
    "UnderMinIsrPartitionCount",
    new Gauge[Int] {
      def value = getLeaderPartitions.count(_.isUnderMinIsr)
    }
  )

  val isrExpandRate = newMeter("IsrExpandsPerSec", "expands", TimeUnit.SECONDS)
  val isrShrinkRate = newMeter("IsrShrinksPerSec", "shrinks", TimeUnit.SECONDS)
  val failedIsrUpdatesRate = newMeter("FailedIsrUpdatesPerSec", "failedUpdates", TimeUnit.SECONDS)

  def underReplicatedPartitionCount: Int =
    getLeaderPartitions.count(_.isUnderReplicated)

  def startHighWaterMarksCheckPointThread() = {
    if(highWatermarkCheckPointThreadStarted.compareAndSet(false, true))
      scheduler.schedule("highwatermark-checkpoint", checkpointHighWatermarks _, period = config.replicaHighWatermarkCheckpointIntervalMs, unit = TimeUnit.MILLISECONDS)
  }

  def recordIsrChange(topicPartition: TopicPartition) {
    isrChangeSet synchronized {
      isrChangeSet += topicPartition
      lastIsrChangeMs.set(System.currentTimeMillis())
    }
  }
  /**
   * This function periodically runs to see if ISR needs to be propagated. It propagates ISR when:
   * 1. There is ISR change not propagated yet.
   * 2. There is no ISR Change in the last five seconds, or it has been more than 60 seconds since the last ISR propagation.
   * This allows an occasional ISR change to be propagated within a few seconds, and avoids overwhelming controller and
   * other brokers when large amount of ISR change occurs.
   */
  def maybePropagateIsrChanges() {
    val now = System.currentTimeMillis()
    isrChangeSet synchronized {
      if (isrChangeSet.nonEmpty &&
        (lastIsrChangeMs.get() + ReplicaManager.IsrChangePropagationBlackOut < now ||
          lastIsrPropagationMs.get() + ReplicaManager.IsrChangePropagationInterval < now)) {
        ReplicationUtils.propagateIsrChanges(zkUtils, isrChangeSet)
        isrChangeSet.clear()
        lastIsrPropagationMs.set(now)
      }
    }
  }

  def getLog(topicPartition: TopicPartition): Option[Log] = logManager.getLog(topicPartition)

  /**
   * Try to complete some delayed produce requests with the request key;
   * this can be triggered when:
   *
   * 1. The partition HW has changed (for acks = -1)
   * 2. A follower replica's fetch operation is received (for acks > 1)
   */
  def tryCompleteDelayedProduce(key: DelayedOperationKey) {
    val completed = delayedProducePurgatory.checkAndComplete(key)
    debug("Request key %s unblocked %d producer requests.".format(key.keyLabel, completed))
  }

  /**
   * Try to complete some delayed fetch requests with the request key;
   * this can be triggered when:
   *
   * 1. The partition HW has changed (for regular fetch)
   * 2. A new message set is appended to the local log (for follower fetch)
   */
  def tryCompleteDelayedFetch(key: DelayedOperationKey) {
    val completed = delayedFetchPurgatory.checkAndComplete(key)
    debug("Request key %s unblocked %d fetch requests.".format(key.keyLabel, completed))
  }

  /**
   * Try to complete some delayed DeleteRecordsRequest with the request key;
   * this needs to be triggered when the partition low watermark has changed
   */
  def tryCompleteDelayedDeleteRecords(key: DelayedOperationKey) {
    val completed = delayedDeleteRecordsPurgatory.checkAndComplete(key)
    debug("Request key %s unblocked %d DeleteRecordsRequest.".format(key.keyLabel, completed))
  }

  def startup() {
    // start ISR expiration thread
    // A follower can lag behind leader for up to config.replicaLagTimeMaxMs x 1.5 before it is removed from ISR
    scheduler.schedule("isr-expiration", maybeShrinkIsr _, period = config.replicaLagTimeMaxMs / 2, unit = TimeUnit.MILLISECONDS)
    scheduler.schedule("isr-change-propagation", maybePropagateIsrChanges _, period = 2500L, unit = TimeUnit.MILLISECONDS)
    val haltBrokerOnFailure = config.interBrokerProtocolVersion < KAFKA_1_0_IV0
    logDirFailureHandler = new LogDirFailureHandler("LogDirFailureHandler", haltBrokerOnFailure)
    logDirFailureHandler.start()
  }

  def stopReplica(topicPartition: TopicPartition, deletePartition: Boolean): Errors  = {
    stateChangeLogger.trace(s"Broker $localBrokerId handling stop replica (delete=$deletePartition) for partition $topicPartition")
    val error = Errors.NONE
    getPartition(topicPartition) match {
      case Some(partition) =>
        if (deletePartition) {
          if (partition eq ReplicaManager.OfflinePartition)
            throw new KafkaStorageException(s"Partition $topicPartition is on an offline disk")
          val removedPartition = allPartitions.remove(topicPartition)
          if (removedPartition != null) {
            val topicHasPartitions = allPartitions.values.exists(partition => topicPartition.topic == partition.topic)
            if (!topicHasPartitions)
              brokerTopicStats.removeMetrics(topicPartition.topic)
            // this will delete the local log. This call may throw exception if the log is on offline directory
            removedPartition.delete()
          } else if (logManager.getLog(topicPartition).isDefined) {
            // Delete log and corresponding folders in case replica manager doesn't hold them anymore.
            // This could happen when topic is being deleted while broker is down and recovers.
            logManager.asyncDelete(topicPartition)
          }
        }
      case None =>
        // Delete log and corresponding folders in case replica manager doesn't hold them anymore.
        // This could happen when topic is being deleted while broker is down and recovers.
        if (deletePartition && logManager.getLog(topicPartition).isDefined)
          logManager.asyncDelete(topicPartition)
        stateChangeLogger.trace(s"Broker $localBrokerId ignoring stop replica (delete=$deletePartition) for partition $topicPartition as replica doesn't exist on broker")
    }
    stateChangeLogger.trace(s"Broker $localBrokerId finished handling stop replica (delete=$deletePartition) for partition $topicPartition")
    error
  }

  def stopReplicas(stopReplicaRequest: StopReplicaRequest): (mutable.Map[TopicPartition, Errors], Errors) = {
    replicaStateChangeLock synchronized {
      val responseMap = new collection.mutable.HashMap[TopicPartition, Errors]
      if(stopReplicaRequest.controllerEpoch() < controllerEpoch) {
        stateChangeLogger.warn("Broker %d received stop replica request from an old controller epoch %d. Latest known controller epoch is %d"
          .format(localBrokerId, stopReplicaRequest.controllerEpoch, controllerEpoch))
        (responseMap, Errors.STALE_CONTROLLER_EPOCH)
      } else {
        val partitions = stopReplicaRequest.partitions.asScala
        controllerEpoch = stopReplicaRequest.controllerEpoch
        // First stop fetchers for all partitions, then stop the corresponding replicas
        replicaFetcherManager.removeFetcherForPartitions(partitions)
        for (topicPartition <- partitions){
          try {
            val error = stopReplica(topicPartition, stopReplicaRequest.deletePartitions)
            responseMap.put(topicPartition, error)
          } catch {
            case e: KafkaStorageException =>
              stateChangeLogger.error(s"Broker $localBrokerId ignoring stop replica (delete=${stopReplicaRequest.deletePartitions}) for partition $topicPartition due to storage exception", e)
              responseMap.put(topicPartition, Errors.KAFKA_STORAGE_ERROR)
          }
        }
        (responseMap, Errors.NONE)
      }
    }
  }

  def getOrCreatePartition(topicPartition: TopicPartition): Partition =
    allPartitions.getAndMaybePut(topicPartition)

  def getPartition(topicPartition: TopicPartition): Option[Partition] =
    Option(allPartitions.get(topicPartition))

  def getReplicaOrException(topicPartition: TopicPartition): Replica = {
    getPartition(topicPartition) match {
      case Some(partition) =>
        if (partition eq ReplicaManager.OfflinePartition)
          throw new KafkaStorageException(s"Replica $localBrokerId is in an offline log directory for partition $topicPartition")
        else
          partition.getReplica(localBrokerId).getOrElse(
            throw new ReplicaNotAvailableException(s"Replica $localBrokerId is not available for partition $topicPartition"))
      case None =>
        throw new ReplicaNotAvailableException(s"Replica $localBrokerId is not available for partition $topicPartition")
    }
  }

  def getLeaderReplicaIfLocal(topicPartition: TopicPartition): Replica =  {
    val partitionOpt = getPartition(topicPartition)
    partitionOpt match {
      case None =>
        throw new UnknownTopicOrPartitionException(s"Partition $topicPartition doesn't exist on $localBrokerId")
      case Some(partition) =>
        if (partition eq ReplicaManager.OfflinePartition)
          throw new KafkaStorageException(s"Partition $topicPartition is in an offline log directory on broker $localBrokerId")
        else partition.leaderReplicaIfLocal match {
          case Some(leaderReplica) => leaderReplica
          case None =>
            throw new NotLeaderForPartitionException(s"Leader not local for partition $topicPartition on broker $localBrokerId")
        }
    }
  }

  def getReplica(topicPartition: TopicPartition, replicaId: Int): Option[Replica] =
    getPartition(topicPartition).filter(_ ne ReplicaManager.OfflinePartition).flatMap(_.getReplica(replicaId))

  def getReplica(tp: TopicPartition): Option[Replica] = getReplica(tp, localBrokerId)

  def getLogDir(topicPartition: TopicPartition): Option[String] = {
    getReplica(topicPartition).flatMap(_.log) match {
      case Some(log) => Some(log.dir.getParent)
      case None => None
    }
  }

  /**
   * Append messages to leader replicas of the partition, and wait for them to be replicated to other replicas;
   * the callback function will be triggered either when timeout or the required acks are satisfied;
   * if the callback function itself is already synchronized on some object then pass this object to avoid deadlock.
   */
  def appendRecords(timeout: Long,
                    requiredAcks: Short,
                    internalTopicsAllowed: Boolean,
                    isFromClient: Boolean,
                    entriesPerPartition: Map[TopicPartition, MemoryRecords],
                    responseCallback: Map[TopicPartition, PartitionResponse] => Unit,
                    delayedProduceLock: Option[Object] = None) {
    if (isValidRequiredAcks(requiredAcks)) {
      val sTime = time.milliseconds
      val localProduceResults = appendToLocalLog(internalTopicsAllowed = internalTopicsAllowed,
        isFromClient = isFromClient, entriesPerPartition, requiredAcks)
      debug("Produce to local log in %d ms".format(time.milliseconds - sTime))

      val produceStatus = localProduceResults.map { case (topicPartition, result) =>
        topicPartition ->
                ProducePartitionStatus(
                  result.info.lastOffset + 1, // required offset
                  new PartitionResponse(result.error, result.info.firstOffset, result.info.logAppendTime)) // response status
      }

      if (delayedProduceRequestRequired(requiredAcks, entriesPerPartition, localProduceResults)) {
        // create delayed produce operation
        val produceMetadata = ProduceMetadata(requiredAcks, produceStatus)
        val delayedProduce = new DelayedProduce(timeout, produceMetadata, this, responseCallback, delayedProduceLock)

        // create a list of (topic, partition) pairs to use as keys for this delayed produce operation
        val producerRequestKeys = entriesPerPartition.keys.map(new TopicPartitionOperationKey(_)).toSeq

        // try to complete the request immediately, otherwise put it into the purgatory
        // this is because while the delayed produce operation is being created, new
        // requests may arrive and hence make this operation completable.
        delayedProducePurgatory.tryCompleteElseWatch(delayedProduce, producerRequestKeys)

      } else {
        // we can respond immediately
        val produceResponseStatus = produceStatus.mapValues(status => status.responseStatus)
        responseCallback(produceResponseStatus)
      }
    } else {
      // If required.acks is outside accepted range, something is wrong with the client
      // Just return an error and don't handle the request at all
      val responseStatus = entriesPerPartition.map { case (topicPartition, _) =>
        topicPartition -> new PartitionResponse(Errors.INVALID_REQUIRED_ACKS,
          LogAppendInfo.UnknownLogAppendInfo.firstOffset, RecordBatch.NO_TIMESTAMP)
      }
      responseCallback(responseStatus)
    }
  }

  /**
   * Delete records on leader replicas of the partition, and wait for delete records operation be propagated to other replicas;
   * the callback function will be triggered either when timeout or logStartOffset of all live replicas have reached the specified offset
   */
  private def deleteRecordsOnLocalLog(offsetPerPartition: Map[TopicPartition, Long]): Map[TopicPartition, LogDeleteRecordsResult] = {
    trace("Delete records on local logs to offsets [%s]".format(offsetPerPartition))
    offsetPerPartition.map { case (topicPartition, requestedOffset) =>
      // reject delete records operation on internal topics
      if (Topic.isInternal(topicPartition.topic)) {
        (topicPartition, LogDeleteRecordsResult(-1L, -1L, Some(new InvalidTopicException(s"Cannot delete records of internal topic ${topicPartition.topic}"))))
      } else {
        try {
          val partition = getPartition(topicPartition) match {
            case Some(p) =>
              if (p eq ReplicaManager.OfflinePartition)
                throw new KafkaStorageException("Partition %s is in an offline log directory on broker %d".format(topicPartition, localBrokerId))
              p
            case None =>
              throw new UnknownTopicOrPartitionException("Partition %s doesn't exist on %d".format(topicPartition, localBrokerId))
          }
          val convertedOffset =
            if (requestedOffset == DeleteRecordsRequest.HIGH_WATERMARK) {
              partition.leaderReplicaIfLocal match {
                case Some(leaderReplica) =>
                  leaderReplica.highWatermark.messageOffset
                case None =>
                  throw new NotLeaderForPartitionException("Leader not local for partition %s on broker %d"
                    .format(topicPartition, localBrokerId))
              }
            } else
              requestedOffset
          if (convertedOffset < 0)
            throw new OffsetOutOfRangeException(s"The offset $convertedOffset for partition $topicPartition is not valid")

          val lowWatermark = partition.deleteRecordsOnLeader(convertedOffset)
          (topicPartition, LogDeleteRecordsResult(convertedOffset, lowWatermark))
        } catch {
          // NOTE: Failed produce requests metric is not incremented for known exceptions
          // it is supposed to indicate un-expected failures of a broker in handling a produce request
          case e@ (_: UnknownTopicOrPartitionException |
                   _: NotLeaderForPartitionException |
                   _: OffsetOutOfRangeException |
                   _: PolicyViolationException |
                   _: KafkaStorageException |
                   _: NotEnoughReplicasException) =>
            (topicPartition, LogDeleteRecordsResult(-1L, -1L, Some(e)))
          case t: Throwable =>
            error("Error processing delete records operation on partition %s".format(topicPartition), t)
            (topicPartition, LogDeleteRecordsResult(-1L, -1L, Some(t)))
        }
      }
    }
  }

  // If there exists a topic partition that meets the following requirement,
  // we need to put a delayed DeleteRecordsRequest and wait for the delete records operation to complete
  //
  // 1. the delete records operation on this partition is successful
  // 2. low watermark of this partition is smaller than the specified offset
  private def delayedDeleteRecordsRequired(localDeleteRecordsResults: Map[TopicPartition, LogDeleteRecordsResult]): Boolean = {
    localDeleteRecordsResults.exists{ case (tp, deleteRecordsResult) =>
      deleteRecordsResult.exception.isEmpty && deleteRecordsResult.lowWatermark < deleteRecordsResult.requestedOffset
    }
  }

  def deleteRecords(timeout: Long,
                    offsetPerPartition: Map[TopicPartition, Long],
                    responseCallback: Map[TopicPartition, DeleteRecordsResponse.PartitionResponse] => Unit) {
    val timeBeforeLocalDeleteRecords = time.milliseconds
    val localDeleteRecordsResults = deleteRecordsOnLocalLog(offsetPerPartition)
    debug("Delete records on local log in %d ms".format(time.milliseconds - timeBeforeLocalDeleteRecords))

    val deleteRecordsStatus = localDeleteRecordsResults.map { case (topicPartition, result) =>
      topicPartition ->
        DeleteRecordsPartitionStatus(
          result.requestedOffset, // requested offset
          new DeleteRecordsResponse.PartitionResponse(result.lowWatermark, result.error)) // response status
    }

    if (delayedDeleteRecordsRequired(localDeleteRecordsResults)) {
      // create delayed delete records operation
      val delayedDeleteRecords = new DelayedDeleteRecords(timeout, deleteRecordsStatus, this, responseCallback)

      // create a list of (topic, partition) pairs to use as keys for this delayed delete records operation
      val deleteRecordsRequestKeys = offsetPerPartition.keys.map(new TopicPartitionOperationKey(_)).toSeq

      // try to complete the request immediately, otherwise put it into the purgatory
      // this is because while the delayed delete records operation is being created, new
      // requests may arrive and hence make this operation completable.
      delayedDeleteRecordsPurgatory.tryCompleteElseWatch(delayedDeleteRecords, deleteRecordsRequestKeys)
    } else {
      // we can respond immediately
      val deleteRecordsResponseStatus = deleteRecordsStatus.mapValues(status => status.responseStatus)
      responseCallback(deleteRecordsResponseStatus)
    }
  }

  // If all the following conditions are true, we need to put a delayed produce request and wait for replication to complete
  //
  // 1. required acks = -1
  // 2. there is data to append
  // 3. at least one partition append was successful (fewer errors than partitions)
  private def delayedProduceRequestRequired(requiredAcks: Short,
                                            entriesPerPartition: Map[TopicPartition, MemoryRecords],
                                            localProduceResults: Map[TopicPartition, LogAppendResult]): Boolean = {
    requiredAcks == -1 &&
    entriesPerPartition.nonEmpty &&
    localProduceResults.values.count(_.exception.isDefined) < entriesPerPartition.size
  }

  private def isValidRequiredAcks(requiredAcks: Short): Boolean = {
    requiredAcks == -1 || requiredAcks == 1 || requiredAcks == 0
  }

  /**
   * Append the messages to the local replica logs
   */
  private def appendToLocalLog(internalTopicsAllowed: Boolean,
                               isFromClient: Boolean,
                               entriesPerPartition: Map[TopicPartition, MemoryRecords],
                               requiredAcks: Short): Map[TopicPartition, LogAppendResult] = {
    trace("Append [%s] to local log ".format(entriesPerPartition))
    entriesPerPartition.map { case (topicPartition, records) =>
      brokerTopicStats.topicStats(topicPartition.topic).totalProduceRequestRate.mark()
      brokerTopicStats.allTopicsStats.totalProduceRequestRate.mark()

      // reject appending to internal topics if it is not allowed
      if (Topic.isInternal(topicPartition.topic) && !internalTopicsAllowed) {
        (topicPartition, LogAppendResult(
          LogAppendInfo.UnknownLogAppendInfo,
          Some(new InvalidTopicException(s"Cannot append to internal topic ${topicPartition.topic}"))))
      } else {
        try {
          val partitionOpt = getPartition(topicPartition)
          val info = partitionOpt match {
            case Some(partition) =>
              if (partition eq ReplicaManager.OfflinePartition)
                throw new KafkaStorageException(s"Partition $topicPartition is in an offline log directory on broker $localBrokerId")
              partition.appendRecordsToLeader(records, isFromClient, requiredAcks)

            case None => throw new UnknownTopicOrPartitionException("Partition %s doesn't exist on %d"
              .format(topicPartition, localBrokerId))
          }

          val numAppendedMessages =
            if (info.firstOffset == -1L || info.lastOffset == -1L)
              0
            else
              info.lastOffset - info.firstOffset + 1

          // update stats for successfully appended bytes and messages as bytesInRate and messageInRate
          brokerTopicStats.topicStats(topicPartition.topic).bytesInRate.mark(records.sizeInBytes)
          brokerTopicStats.allTopicsStats.bytesInRate.mark(records.sizeInBytes)
          brokerTopicStats.topicStats(topicPartition.topic).messagesInRate.mark(numAppendedMessages)
          brokerTopicStats.allTopicsStats.messagesInRate.mark(numAppendedMessages)

          trace("%d bytes written to log %s-%d beginning at offset %d and ending at offset %d"
            .format(records.sizeInBytes, topicPartition.topic, topicPartition.partition, info.firstOffset, info.lastOffset))
          (topicPartition, LogAppendResult(info))
        } catch {
          // NOTE: Failed produce requests metric is not incremented for known exceptions
          // it is supposed to indicate un-expected failures of a broker in handling a produce request
          case e@ (_: UnknownTopicOrPartitionException |
                   _: NotLeaderForPartitionException |
                   _: RecordTooLargeException |
                   _: RecordBatchTooLargeException |
                   _: CorruptRecordException |
                   _: KafkaStorageException |
                   _: InvalidTimestampException) =>
            (topicPartition, LogAppendResult(LogAppendInfo.UnknownLogAppendInfo, Some(e)))
          case t: Throwable =>
            brokerTopicStats.topicStats(topicPartition.topic).failedProduceRequestRate.mark()
            brokerTopicStats.allTopicsStats.failedProduceRequestRate.mark()
            error("Error processing append operation on partition %s".format(topicPartition), t)
            (topicPartition, LogAppendResult(LogAppendInfo.UnknownLogAppendInfo, Some(t)))
        }
      }
    }
  }

  /**
   * Fetch messages from the leader replica, and wait until enough data can be fetched and return;
   * the callback function will be triggered either when timeout or required fetch info is satisfied
   */
  def fetchMessages(timeout: Long,
                    replicaId: Int,
                    fetchMinBytes: Int,
                    fetchMaxBytes: Int,
                    hardMaxBytesLimit: Boolean,
                    fetchInfos: Seq[(TopicPartition, PartitionData)],
                    quota: ReplicaQuota = UnboundedQuota,
                    responseCallback: Seq[(TopicPartition, FetchPartitionData)] => Unit,
                    isolationLevel: IsolationLevel) {
    val isFromFollower = replicaId >= 0
    val fetchOnlyFromLeader: Boolean = replicaId != Request.DebuggingConsumerId
    val fetchOnlyCommitted: Boolean = ! Request.isValidBrokerId(replicaId)

    // read from local logs
    val logReadResults = readFromLocalLog(
      replicaId = replicaId,
      fetchOnlyFromLeader = fetchOnlyFromLeader,
      readOnlyCommitted = fetchOnlyCommitted,
      fetchMaxBytes = fetchMaxBytes,
      hardMaxBytesLimit = hardMaxBytesLimit,
      readPartitionInfo = fetchInfos,
      quota = quota,
      isolationLevel = isolationLevel)

    // if the fetch comes from the follower,
    // update its corresponding log end offset
    if(Request.isValidBrokerId(replicaId))
      updateFollowerLogReadResults(replicaId, logReadResults)

    // check if this fetch request can be satisfied right away
    val logReadResultValues = logReadResults.map { case (_, v) => v }
    val bytesReadable = logReadResultValues.map(_.info.records.sizeInBytes).sum
    val errorReadingData = logReadResultValues.foldLeft(false) ((errorIncurred, readResult) =>
      errorIncurred || (readResult.error != Errors.NONE))

    // respond immediately if 1) fetch request does not want to wait
    //                        2) fetch request does not require any data
    //                        3) has enough data to respond
    //                        4) some error happens while reading data
    if (timeout <= 0 || fetchInfos.isEmpty || bytesReadable >= fetchMinBytes || errorReadingData) {
      val fetchPartitionData = logReadResults.map { case (tp, result) =>
        tp -> FetchPartitionData(result.error, result.highWatermark, result.leaderLogStartOffset, result.info.records,
          result.lastStableOffset, result.info.abortedTransactions)
      }
      responseCallback(fetchPartitionData)
    } else {
      // construct the fetch results from the read results
      val fetchPartitionStatus = logReadResults.map { case (topicPartition, result) =>
        val fetchInfo = fetchInfos.collectFirst {
          case (tp, v) if tp == topicPartition => v
        }.getOrElse(sys.error(s"Partition $topicPartition not found in fetchInfos"))
        (topicPartition, FetchPartitionStatus(result.info.fetchOffsetMetadata, fetchInfo))
      }
      val fetchMetadata = FetchMetadata(fetchMinBytes, fetchMaxBytes, hardMaxBytesLimit, fetchOnlyFromLeader,
        fetchOnlyCommitted, isFromFollower, replicaId, fetchPartitionStatus)
      val delayedFetch = new DelayedFetch(timeout, fetchMetadata, this, quota, isolationLevel, responseCallback)

      // create a list of (topic, partition) pairs to use as keys for this delayed fetch operation
      val delayedFetchKeys = fetchPartitionStatus.map { case (tp, _) => new TopicPartitionOperationKey(tp) }

      // try to complete the request immediately, otherwise put it into the purgatory;
      // this is because while the delayed fetch operation is being created, new requests
      // may arrive and hence make this operation completable.
      delayedFetchPurgatory.tryCompleteElseWatch(delayedFetch, delayedFetchKeys)
    }
  }

  /**
   * Read from multiple topic partitions at the given offset up to maxSize bytes
   */
  def readFromLocalLog(replicaId: Int,
                       fetchOnlyFromLeader: Boolean,
                       readOnlyCommitted: Boolean,
                       fetchMaxBytes: Int,
                       hardMaxBytesLimit: Boolean,
                       readPartitionInfo: Seq[(TopicPartition, PartitionData)],
                       quota: ReplicaQuota,
                       isolationLevel: IsolationLevel): Seq[(TopicPartition, LogReadResult)] = {

    def read(tp: TopicPartition, fetchInfo: PartitionData, limitBytes: Int, minOneMessage: Boolean): LogReadResult = {
      val offset = fetchInfo.fetchOffset
      val partitionFetchSize = fetchInfo.maxBytes
      val followerLogStartOffset = fetchInfo.logStartOffset

      brokerTopicStats.topicStats(tp.topic).totalFetchRequestRate.mark()
      brokerTopicStats.allTopicsStats.totalFetchRequestRate.mark()

      try {
        trace(s"Fetching log segment for partition $tp, offset $offset, partition fetch size $partitionFetchSize, " +
          s"remaining response limit $limitBytes" +
          (if (minOneMessage) s", ignoring response/partition size limits" else ""))

        // decide whether to only fetch from leader
        val localReplica = if (fetchOnlyFromLeader)
          getLeaderReplicaIfLocal(tp)
        else
          getReplicaOrException(tp)

        val initialHighWatermark = localReplica.highWatermark.messageOffset
        val lastStableOffset = if (isolationLevel == IsolationLevel.READ_COMMITTED)
          Some(localReplica.lastStableOffset.messageOffset)
        else
          None

        // decide whether to only fetch committed data (i.e. messages below high watermark)
        val maxOffsetOpt = if (readOnlyCommitted)
          Some(lastStableOffset.getOrElse(initialHighWatermark))
        else
          None

        /* Read the LogOffsetMetadata prior to performing the read from the log.
         * We use the LogOffsetMetadata to determine if a particular replica is in-sync or not.
         * Using the log end offset after performing the read can lead to a race condition
         * where data gets appended to the log immediately after the replica has consumed from it
         * This can cause a replica to always be out of sync.
         */
        val initialLogEndOffset = localReplica.logEndOffset.messageOffset
        val initialLogStartOffset = localReplica.logStartOffset
        val fetchTimeMs = time.milliseconds
        val logReadInfo = localReplica.log match {
          case Some(log) =>
            val adjustedFetchSize = math.min(partitionFetchSize, limitBytes)

            // Try the read first, this tells us whether we need all of adjustedFetchSize for this partition
            val fetch = log.read(offset, adjustedFetchSize, maxOffsetOpt, minOneMessage, isolationLevel)

            // If the partition is being throttled, simply return an empty set.
            if (shouldLeaderThrottle(quota, tp, replicaId))
              FetchDataInfo(fetch.fetchOffsetMetadata, MemoryRecords.EMPTY)
            // For FetchRequest version 3, we replace incomplete message sets with an empty one as consumers can make
            // progress in such cases and don't need to report a `RecordTooLargeException`
            else if (!hardMaxBytesLimit && fetch.firstEntryIncomplete)
              FetchDataInfo(fetch.fetchOffsetMetadata, MemoryRecords.EMPTY)
            else fetch

          case None =>
            error(s"Leader for partition $tp does not have a local log")
            FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY)
        }

        LogReadResult(info = logReadInfo,
                      highWatermark = initialHighWatermark,
                      leaderLogStartOffset = initialLogStartOffset,
                      leaderLogEndOffset = initialLogEndOffset,
                      followerLogStartOffset = followerLogStartOffset,
                      fetchTimeMs = fetchTimeMs,
                      readSize = partitionFetchSize,
                      lastStableOffset = lastStableOffset,
                      exception = None)
      } catch {
        // NOTE: Failed fetch requests metric is not incremented for known exceptions since it
        // is supposed to indicate un-expected failure of a broker in handling a fetch request
        case e@ (_: UnknownTopicOrPartitionException |
                 _: NotLeaderForPartitionException |
                 _: ReplicaNotAvailableException |
                 _: KafkaStorageException |
                 _: OffsetOutOfRangeException) =>
          LogReadResult(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
                        highWatermark = -1L,
                        leaderLogStartOffset = -1L,
                        leaderLogEndOffset = -1L,
                        followerLogStartOffset = -1L,
                        fetchTimeMs = -1L,
                        readSize = partitionFetchSize,
                        lastStableOffset = None,
                        exception = Some(e))
        case e: Throwable =>
          brokerTopicStats.topicStats(tp.topic).failedFetchRequestRate.mark()
          brokerTopicStats.allTopicsStats.failedFetchRequestRate.mark()
          error(s"Error processing fetch operation on partition $tp, offset $offset", e)
          LogReadResult(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
                        highWatermark = -1L,
                        leaderLogStartOffset = -1L,
                        leaderLogEndOffset = -1L,
                        followerLogStartOffset = -1L,
                        fetchTimeMs = -1L,
                        readSize = partitionFetchSize,
                        lastStableOffset = None,
                        exception = Some(e))
      }
    }

    var limitBytes = fetchMaxBytes
    val result = new mutable.ArrayBuffer[(TopicPartition, LogReadResult)]
    var minOneMessage = !hardMaxBytesLimit
    readPartitionInfo.foreach { case (tp, fetchInfo) =>
      val readResult = read(tp, fetchInfo, limitBytes, minOneMessage)
      val messageSetSize = readResult.info.records.sizeInBytes
      // Once we read from a non-empty partition, we stop ignoring request and partition level size limits
      if (messageSetSize > 0)
        minOneMessage = false
      limitBytes = math.max(0, limitBytes - messageSetSize)
      result += (tp -> readResult)
    }
    result
  }

  /**
   *  To avoid ISR thrashing, we only throttle a replica on the leader if it's in the throttled replica list,
   *  the quota is exceeded and the replica is not in sync.
   */
  def shouldLeaderThrottle(quota: ReplicaQuota, topicPartition: TopicPartition, replicaId: Int): Boolean = {
    val isReplicaInSync = getPartition(topicPartition).filter(_ ne ReplicaManager.OfflinePartition).flatMap { partition =>
      partition.getReplica(replicaId).map(partition.inSyncReplicas.contains)
    }.getOrElse(false)
    quota.isThrottled(topicPartition) && quota.isQuotaExceeded && !isReplicaInSync
  }

  def getMagic(topicPartition: TopicPartition): Option[Byte] =
    getReplica(topicPartition).flatMap(_.log.map(_.config.messageFormatVersion.messageFormatVersion))

  def maybeUpdateMetadataCache(correlationId: Int, updateMetadataRequest: UpdateMetadataRequest) : Seq[TopicPartition] =  {
    replicaStateChangeLock synchronized {
      if(updateMetadataRequest.controllerEpoch < controllerEpoch) {
        val stateControllerEpochErrorMessage = ("Broker %d received update metadata request with correlation id %d from an " +
          "old controller %d with epoch %d. Latest known controller epoch is %d").format(localBrokerId,
          correlationId, updateMetadataRequest.controllerId, updateMetadataRequest.controllerEpoch,
          controllerEpoch)
        stateChangeLogger.warn(stateControllerEpochErrorMessage)
        throw new ControllerMovedException(stateControllerEpochErrorMessage)
      } else {
        val deletedPartitions = metadataCache.updateCache(correlationId, updateMetadataRequest)
        controllerEpoch = updateMetadataRequest.controllerEpoch
        deletedPartitions
      }
    }
  }

  def becomeLeaderOrFollower(correlationId: Int,
                             leaderAndISRRequest: LeaderAndIsrRequest,
                             onLeadershipChange: (Iterable[Partition], Iterable[Partition]) => Unit): BecomeLeaderOrFollowerResult = {
    leaderAndISRRequest.partitionStates.asScala.foreach { case (topicPartition, stateInfo) =>
      stateChangeLogger.trace("Broker %d received LeaderAndIsr request %s correlation id %d from controller %d epoch %d for partition [%s,%d]"
                                .format(localBrokerId, stateInfo, correlationId,
                                        leaderAndISRRequest.controllerId, leaderAndISRRequest.controllerEpoch, topicPartition.topic, topicPartition.partition))
    }
    replicaStateChangeLock synchronized {
      val responseMap = new mutable.HashMap[TopicPartition, Errors]
      if (leaderAndISRRequest.controllerEpoch < controllerEpoch) {
        stateChangeLogger.warn(("Broker %d ignoring LeaderAndIsr request from controller %d with correlation id %d since " +
          "its controller epoch %d is old. Latest known controller epoch is %d").format(localBrokerId, leaderAndISRRequest.controllerId,
          correlationId, leaderAndISRRequest.controllerEpoch, controllerEpoch))
        BecomeLeaderOrFollowerResult(responseMap, Errors.STALE_CONTROLLER_EPOCH)
      } else {
        val controllerId = leaderAndISRRequest.controllerId
        controllerEpoch = leaderAndISRRequest.controllerEpoch

        // First check partition's leader epoch
        val partitionState = new mutable.HashMap[Partition, LeaderAndIsrRequest.PartitionState]()
        leaderAndISRRequest.partitionStates.asScala.foreach { case (topicPartition, stateInfo) =>
          val partition = getOrCreatePartition(topicPartition)
          val partitionLeaderEpoch = partition.getLeaderEpoch
          if (partition eq ReplicaManager.OfflinePartition) {
            stateChangeLogger.warn(("Broker %d ignoring LeaderAndIsr request from controller %d with correlation id %d " +
              "epoch %d for partition [%s,%d] as the local replica for the partition is in an offline log directory")
              .format(localBrokerId, controllerId, correlationId, leaderAndISRRequest.controllerEpoch, topicPartition.topic, topicPartition.partition))
            responseMap.put(topicPartition, Errors.KAFKA_STORAGE_ERROR)
          } else if (partitionLeaderEpoch < stateInfo.basePartitionState.leaderEpoch) {
            // If the leader epoch is valid record the epoch of the controller that made the leadership decision.
            // This is useful while updating the isr to maintain the decision maker controller's epoch in the zookeeper path
            if(stateInfo.basePartitionState.replicas.contains(localBrokerId))
              partitionState.put(partition, stateInfo)
            else {
              stateChangeLogger.warn(("Broker %d ignoring LeaderAndIsr request from controller %d with correlation id %d " +
                "epoch %d for partition [%s,%d] as itself is not in assigned replica list %s")
                .format(localBrokerId, controllerId, correlationId, leaderAndISRRequest.controllerEpoch,
                  topicPartition.topic, topicPartition.partition, stateInfo.basePartitionState.replicas.asScala.mkString(",")))
              responseMap.put(topicPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION)
            }
          } else {
            // Otherwise record the error code in response
            stateChangeLogger.warn(("Broker %d ignoring LeaderAndIsr request from controller %d with correlation id %d " +
              "epoch %d for partition [%s,%d] since its associated leader epoch %d is not higher than the current leader epoch %d")
              .format(localBrokerId, controllerId, correlationId, leaderAndISRRequest.controllerEpoch,
                topicPartition.topic, topicPartition.partition, stateInfo.basePartitionState.leaderEpoch, partitionLeaderEpoch))
            responseMap.put(topicPartition, Errors.STALE_CONTROLLER_EPOCH)
          }
        }

        val partitionsTobeLeader = partitionState.filter { case (_, stateInfo) =>
          stateInfo.basePartitionState.leader == localBrokerId
        }
        val partitionsToBeFollower = partitionState -- partitionsTobeLeader.keys

        val partitionsBecomeLeader = if (partitionsTobeLeader.nonEmpty)
          makeLeaders(controllerId, controllerEpoch, partitionsTobeLeader, correlationId, responseMap)
        else
          Set.empty[Partition]
        val partitionsBecomeFollower = if (partitionsToBeFollower.nonEmpty)
          makeFollowers(controllerId, controllerEpoch, partitionsToBeFollower, correlationId, responseMap)
        else
          Set.empty[Partition]

        leaderAndISRRequest.partitionStates.asScala.keys.foreach( topicPartition =>
          /*
           * If there is offline log directory, a Partition object may have been created by getOrCreatePartition()
           * before getOrCreateReplica() failed to create local replica due to KafkaStorageException.
           * In this case ReplicaManager.allPartitions will map this topic-partition to an empty Partition object.
           * we need to map this topic-partition to OfflinePartition instead.
           */
          if (getReplica(topicPartition).isEmpty && (allPartitions.get(topicPartition) ne ReplicaManager.OfflinePartition))
            allPartitions.put(topicPartition, ReplicaManager.OfflinePartition)
        )

        // we initialize highwatermark thread after the first leaderisrrequest. This ensures that all the partitions
        // have been completely populated before starting the checkpointing there by avoiding weird race conditions
        if (!hwThreadInitialized) {
          startHighWaterMarksCheckPointThread()
          hwThreadInitialized = true
        }
        replicaFetcherManager.shutdownIdleFetcherThreads()
        onLeadershipChange(partitionsBecomeLeader, partitionsBecomeFollower)
        BecomeLeaderOrFollowerResult(responseMap, Errors.NONE)
      }
    }
  }

  /*
   * Make the current broker to become leader for a given set of partitions by:
   *
   * 1. Stop fetchers for these partitions
   * 2. Update the partition metadata in cache
   * 3. Add these partitions to the leader partitions set
   *
   * If an unexpected error is thrown in this function, it will be propagated to KafkaApis where
   * the error message will be set on each partition since we do not know which partition caused it. Otherwise,
   * return the set of partitions that are made leader due to this method
   *
   *  TODO: the above may need to be fixed later
   */
  private def makeLeaders(controllerId: Int,
                          epoch: Int,
                          partitionState: Map[Partition, LeaderAndIsrRequest.PartitionState],
                          correlationId: Int,
                          responseMap: mutable.Map[TopicPartition, Errors]): Set[Partition] = {
    partitionState.keys.foreach { partition =>
      stateChangeLogger.trace(("Broker %d handling LeaderAndIsr request correlationId %d from controller %d epoch %d " +
        "starting the become-leader transition for partition %s")
        .format(localBrokerId, correlationId, controllerId, epoch, partition.topicPartition))
    }

    for (partition <- partitionState.keys)
      responseMap.put(partition.topicPartition, Errors.NONE)

    val partitionsToMakeLeaders: mutable.Set[Partition] = mutable.Set()

    try {
      // First stop fetchers for all the partitions
      replicaFetcherManager.removeFetcherForPartitions(partitionState.keySet.map(_.topicPartition))
      // Update the partition information to be the leader
      partitionState.foreach{ case (partition, partitionStateInfo) =>
        try {
          if (partition.makeLeader(controllerId, partitionStateInfo, correlationId)) {
            partitionsToMakeLeaders += partition
            stateChangeLogger.trace(("Broker %d stopped fetchers as part of become-leader request from controller " +
              "%d epoch %d with correlation id %d for partition %s")
              .format(localBrokerId, controllerId, epoch, correlationId, partition.topicPartition))
          } else
            stateChangeLogger.info(("Broker %d skipped the become-leader state change after marking its partition as leader with correlation id %d from " +
              "controller %d epoch %d for partition %s since it is already the leader for the partition.")
              .format(localBrokerId, correlationId, controllerId, epoch, partition.topicPartition))
        } catch {
          case e: KafkaStorageException =>
            stateChangeLogger.error(("Broker %d skipped the become-leader state change with correlation id %d from " +
              "controller %d epoch %d for partition %s since the replica for the partition is offline due to disk error %s.")
              .format(localBrokerId, correlationId, controllerId, epoch, partition.topicPartition, e))
            val dirOpt = getLogDir(new TopicPartition(partition.topic, partition.partitionId))
            error(s"Error while making broker the leader for partition $partition in dir $dirOpt", e)
            responseMap.put(new TopicPartition(partition.topic, partition.partitionId), Errors.KAFKA_STORAGE_ERROR)
        }
      }

    } catch {
      case e: Throwable =>
        partitionState.keys.foreach { partition =>
          val errorMsg = ("Error on broker %d while processing LeaderAndIsr request correlationId %d received from controller %d" +
            " epoch %d for partition %s").format(localBrokerId, correlationId, controllerId, epoch, partition.topicPartition)
          stateChangeLogger.error(errorMsg, e)
        }
        // Re-throw the exception for it to be caught in KafkaApis
        throw e
    }

    partitionState.keys.foreach { partition =>
      stateChangeLogger.trace(("Broker %d completed LeaderAndIsr request correlationId %d from controller %d epoch %d " +
        "for the become-leader transition for partition %s")
        .format(localBrokerId, correlationId, controllerId, epoch, partition.topicPartition))
    }

    partitionsToMakeLeaders
  }

  /*
   * Make the current broker to become follower for a given set of partitions by:
   *
   * 1. Remove these partitions from the leader partitions set.
   * 2. Mark the replicas as followers so that no more data can be added from the producer clients.
   * 3. Stop fetchers for these partitions so that no more data can be added by the replica fetcher threads.
   * 4. Truncate the log and checkpoint offsets for these partitions.
   * 5. Clear the produce and fetch requests in the purgatory
   * 6. If the broker is not shutting down, add the fetcher to the new leaders.
   *
   * The ordering of doing these steps make sure that the replicas in transition will not
   * take any more messages before checkpointing offsets so that all messages before the checkpoint
   * are guaranteed to be flushed to disks
   *
   * If an unexpected error is thrown in this function, it will be propagated to KafkaApis where
   * the error message will be set on each partition since we do not know which partition caused it. Otherwise,
   * return the set of partitions that are made follower due to this method
   */
  private def makeFollowers(controllerId: Int,
                            epoch: Int,
                            partitionState: Map[Partition, LeaderAndIsrRequest.PartitionState],
                            correlationId: Int,
                            responseMap: mutable.Map[TopicPartition, Errors]) : Set[Partition] = {
    partitionState.keys.foreach { partition =>
      stateChangeLogger.trace(("Broker %d handling LeaderAndIsr request correlationId %d from controller %d epoch %d " +
        "starting the become-follower transition for partition %s")
        .format(localBrokerId, correlationId, controllerId, epoch, partition.topicPartition))
    }

    for (partition <- partitionState.keys)
      responseMap.put(partition.topicPartition, Errors.NONE)

    val partitionsToMakeFollower: mutable.Set[Partition] = mutable.Set()

    try {

      // TODO: Delete leaders from LeaderAndIsrRequest
      partitionState.foreach{ case (partition, partitionStateInfo) =>
        try {
          val newLeaderBrokerId = partitionStateInfo.basePartitionState.leader
          metadataCache.getAliveBrokers.find(_.id == newLeaderBrokerId) match {
            // Only change partition state when the leader is available
            case Some(_) =>
              if (partition.makeFollower(controllerId, partitionStateInfo, correlationId))
                partitionsToMakeFollower += partition
              else
                stateChangeLogger.info(("Broker %d skipped the become-follower state change after marking its partition as follower with correlation id %d from " +
                  "controller %d epoch %d for partition %s since the new leader %d is the same as the old leader")
                  .format(localBrokerId, correlationId, controllerId, partitionStateInfo.basePartitionState.controllerEpoch,
                    partition.topicPartition, newLeaderBrokerId))
            case None =>
              // The leader broker should always be present in the metadata cache.
              // If not, we should record the error message and abort the transition process for this partition
              stateChangeLogger.error(("Broker %d received LeaderAndIsrRequest with correlation id %d from controller" +
                " %d epoch %d for partition %s but cannot become follower since the new leader %d is unavailable.")
                .format(localBrokerId, correlationId, controllerId, partitionStateInfo.basePartitionState.controllerEpoch,
                  partition.topicPartition, newLeaderBrokerId))
              // Create the local replica even if the leader is unavailable. This is required to ensure that we include
              // the partition's high watermark in the checkpoint file (see KAFKA-1647)
              partition.getOrCreateReplica(isNew = partitionStateInfo.isNew)
          }
        } catch {
          case e: KafkaStorageException =>
            stateChangeLogger.error(("Broker %d skipped the become-follower state change with correlation id %d from " +
              "controller %d epoch %d for partition [%s,%d] since the replica for the partition is offline due to disk error %s")
              .format(localBrokerId, correlationId, controllerId, partitionStateInfo.basePartitionState.controllerEpoch, partition.topic, partition.partitionId, e))
            val dirOpt = getLogDir(new TopicPartition(partition.topic, partition.partitionId))
            error(s"Error while making broker the follower for partition $partition in dir $dirOpt", e)
            responseMap.put(new TopicPartition(partition.topic, partition.partitionId), Errors.KAFKA_STORAGE_ERROR)
        }
      }

      replicaFetcherManager.removeFetcherForPartitions(partitionsToMakeFollower.map(_.topicPartition))
      partitionsToMakeFollower.foreach { partition =>
        stateChangeLogger.trace(("Broker %d stopped fetchers as part of become-follower request from controller " +
          "%d epoch %d with correlation id %d for partition %s")
          .format(localBrokerId, controllerId, epoch, correlationId, partition.topicPartition))
      }

      partitionsToMakeFollower.foreach { partition =>
        val topicPartitionOperationKey = new TopicPartitionOperationKey(partition.topicPartition)
        tryCompleteDelayedProduce(topicPartitionOperationKey)
        tryCompleteDelayedFetch(topicPartitionOperationKey)
      }

      partitionsToMakeFollower.foreach { partition =>
        stateChangeLogger.trace(("Broker %d truncated logs and checkpointed recovery boundaries for partition %s as part of " +
          "become-follower request with correlation id %d from controller %d epoch %d").format(localBrokerId,
          partition.topicPartition, correlationId, controllerId, epoch))
      }

      if (isShuttingDown.get()) {
        partitionsToMakeFollower.foreach { partition =>
          stateChangeLogger.trace(("Broker %d skipped the adding-fetcher step of the become-follower state change with correlation id %d from " +
            "controller %d epoch %d for partition %s since it is shutting down").format(localBrokerId, correlationId,
            controllerId, epoch, partition.topicPartition))
        }
      }
      else {
        // we do not need to check if the leader exists again since this has been done at the beginning of this process
        val partitionsToMakeFollowerWithLeaderAndOffset = partitionsToMakeFollower.map(partition =>
          partition.topicPartition -> BrokerAndInitialOffset(
            metadataCache.getAliveBrokers.find(_.id == partition.leaderReplicaIdOpt.get).get.getBrokerEndPoint(config.interBrokerListenerName),
            partition.getReplica().get.logEndOffset.messageOffset)).toMap
        replicaFetcherManager.addFetcherForPartitions(partitionsToMakeFollowerWithLeaderAndOffset)

        partitionsToMakeFollower.foreach { partition =>
          stateChangeLogger.trace(("Broker %d started fetcher to new leader as part of become-follower request from controller " +
            "%d epoch %d with correlation id %d for partition %s")
            .format(localBrokerId, controllerId, epoch, correlationId, partition.topicPartition))
        }
      }
    } catch {
      case e: Throwable =>
        val errorMsg = ("Error on broker %d while processing LeaderAndIsr request with correlationId %d received from controller %d " +
          "epoch %d").format(localBrokerId, correlationId, controllerId, epoch)
        stateChangeLogger.error(errorMsg, e)
        // Re-throw the exception for it to be caught in KafkaApis
        throw e
    }

    partitionState.keys.foreach { partition =>
      stateChangeLogger.trace(("Broker %d completed LeaderAndIsr request correlationId %d from controller %d epoch %d " +
        "for the become-follower transition for partition %s")
        .format(localBrokerId, correlationId, controllerId, epoch, partition.topicPartition))
    }

    partitionsToMakeFollower
  }

  private def maybeShrinkIsr(): Unit = {
    trace("Evaluating ISR list of partitions to see which replicas can be removed from the ISR")
    allPartitions.values.filter(_ ne ReplicaManager.OfflinePartition).foreach(partition => partition.maybeShrinkIsr(config.replicaLagTimeMaxMs))
  }

  private def updateFollowerLogReadResults(replicaId: Int, readResults: Seq[(TopicPartition, LogReadResult)]) {
    debug("Recording follower broker %d log read results: %s ".format(replicaId, readResults))
    readResults.foreach { case (topicPartition, readResult) =>
      getPartition(topicPartition) match {
        case Some(partition) =>
          if (partition ne ReplicaManager.OfflinePartition)
            partition.updateReplicaLogReadResult(replicaId, readResult)

          // for producer requests with ack > 1, we need to check
          // if they can be unblocked after some follower's log end offsets have moved
          tryCompleteDelayedProduce(new TopicPartitionOperationKey(topicPartition))
        case None =>
          warn("While recording the replica LEO, the partition %s hasn't been created.".format(topicPartition))
      }
    }
  }

  private def getLeaderPartitions: List[Partition] =
    allPartitions.values.filter(partition => (partition ne ReplicaManager.OfflinePartition) && partition.leaderReplicaIfLocal.isDefined).toList

  def getLogEndOffset(topicPartition: TopicPartition): Option[Long] = {
    getPartition(topicPartition) match {
      case Some(partition) =>
        if (partition eq ReplicaManager.OfflinePartition)
          None
        else
          partition.leaderReplicaIfLocal.map(_.logEndOffset.messageOffset)
      case None => None
    }
  }

  // Flushes the highwatermark value for all partitions to the highwatermark file
  def checkpointHighWatermarks() {
    val replicas = allPartitions.values.filter(_ ne ReplicaManager.OfflinePartition).flatMap(_.getReplica(localBrokerId))
    val replicasByDir = replicas.filter(_.log.isDefined).groupBy(_.log.get.dir.getParent)
    for ((dir, reps) <- replicasByDir) {
      val hwms = reps.map(r => r.topicPartition -> r.highWatermark.messageOffset).toMap
      try {
        highWatermarkCheckpoints.get(dir).foreach(_.write(hwms))
      } catch {
        case e: KafkaStorageException =>
          error(s"Error while writing to highwatermark file in directory $dir", e)
      }
    }
  }

  def handleLogDirFailure(dir: String) {
    if (!logManager.isLogDirOnline(dir))
      return

    info(s"Stopping serving replicas in dir $dir")
    replicaStateChangeLock synchronized {
      val newOfflinePartitions = allPartitions.values.filter { partition =>
        if (partition eq ReplicaManager.OfflinePartition)
          false
        else partition.getReplica(config.brokerId) match {
          case Some(replica) =>
            replica.log.isDefined && replica.log.get.dir.getParent == dir
          case None => false
        }
      }.map(_.topicPartition)

      info(s"Partitions ${newOfflinePartitions.mkString(",")} are offline due to failure on log directory $dir")

      newOfflinePartitions.foreach { topicPartition =>
        val partition = allPartitions.put(topicPartition, ReplicaManager.OfflinePartition)
        partition.removePartitionMetrics()
      }

      newOfflinePartitions.map(_.topic).toSet.foreach { topic: String =>
        val topicHasPartitions = allPartitions.values.exists(partition => topic == partition.topic)
        if (!topicHasPartitions)
          brokerTopicStats.removeMetrics(topic)
      }

      replicaFetcherManager.removeFetcherForPartitions(newOfflinePartitions.toSet)
      highWatermarkCheckpoints = highWatermarkCheckpoints.filterKeys(_ != dir)
      info("Broker %d stopped fetcher for partitions %s because they are in the failed log dir %s"
        .format(localBrokerId, newOfflinePartitions.mkString(", "), dir))
    }
    logManager.handleLogDirFailure(dir)
    LogDirUtils.propagateLogDirEvent(zkUtils, localBrokerId)
    info(s"Stopped serving replicas in dir $dir")
  }

  def removeMetrics() {
    removeMetric("LeaderCount")
    removeMetric("PartitionCount")
    removeMetric("OfflineReplicaCount")
    removeMetric("UnderReplicatedPartitions")
    removeMetric("UnderMinIsrPartitionCount")
  }

  // High watermark do not need to be checkpointed only when under unit tests
  def shutdown(checkpointHW: Boolean = true) {
    info("Shutting down")
    removeMetrics()
    if (logDirFailureHandler != null)
      logDirFailureHandler.shutdown()
    replicaFetcherManager.shutdown()
    delayedFetchPurgatory.shutdown()
    delayedProducePurgatory.shutdown()
    delayedDeleteRecordsPurgatory.shutdown()
    if (checkpointHW)
      checkpointHighWatermarks()
    info("Shut down completely")
  }

  protected def createReplicaFetcherManager(metrics: Metrics, time: Time, threadNamePrefix: Option[String], quotaManager: ReplicationQuotaManager) = {
    new ReplicaFetcherManager(config, this, metrics, time, threadNamePrefix, quotaManager)
  }

  def lastOffsetForLeaderEpoch(requestedEpochInfo: Map[TopicPartition, Integer]): Map[TopicPartition, EpochEndOffset] = {
    requestedEpochInfo.map { case (tp, leaderEpoch) =>
      val epochEndOffset = getPartition(tp) match {
        case Some(partition) =>
          if (partition eq ReplicaManager.OfflinePartition)
            new EpochEndOffset(KAFKA_STORAGE_ERROR, UNDEFINED_EPOCH_OFFSET)
          else
            partition.lastOffsetForLeaderEpoch(leaderEpoch)
        case None =>
          new EpochEndOffset(UNKNOWN_TOPIC_OR_PARTITION, UNDEFINED_EPOCH_OFFSET)
      }
      tp -> epochEndOffset
    }
  }
}


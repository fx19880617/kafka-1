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

package kafka.admin

import kafka.common._
import kafka.cluster.{BrokerEndPoint, Broker}

import kafka.log.LogConfig
import kafka.utils._
import kafka.api.{TopicMetadata, PartitionMetadata}

import java.util.Random
import java.util.Properties
import org.apache.kafka.common.protocol.SecurityProtocol

import scala.Predef._
import scala.collection._
import mutable.ListBuffer
import scala.collection.mutable
import collection.Map
import collection.Set

import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNodeExistsException

object AdminUtils extends Logging {
  val rand = new Random

  val AdminClientId = "__admin_client"

  val TopicConfigChangeZnodePrefix = "config_change_"

  /**
   * There are 2 goals of replica assignment:
   * 1. Spread the replicas evenly among brokers.
   * 2. For partitions assigned to a particular broker, their other replicas are spread over the other brokers.
   *
   * To achieve this goal, we:
   * 1. Assign the first replica of each partition by round-robin, starting from a random position in the broker list.
   * 2. Assign the remaining replicas of each partition with an increasing shift.
   *
   * Here is an example of assigning
   * broker-0  broker-1  broker-2  broker-3  broker-4
   * p0        p1        p2        p3        p4       (1st replica)
   * p5        p6        p7        p8        p9       (1st replica)
   * p4        p0        p1        p2        p3       (2nd replica)
   * p8        p9        p5        p6        p7       (2nd replica)
   * p3        p4        p0        p1        p2       (3nd replica)
   * p7        p8        p9        p5        p6       (3nd replica)
   */
  def assignReplicasToBrokers(brokerList: Seq[Int],
                              nPartitions: Int,
                              replicationFactor: Int,
                              fixedStartIndex: Int = -1,
                              startPartitionId: Int = -1)
  : Map[Int, Seq[Int]] = {
    if (nPartitions <= 0)
      throw new AdminOperationException("number of partitions must be larger than 0")
    if (replicationFactor <= 0)
      throw new AdminOperationException("replication factor must be larger than 0")
    if (replicationFactor > brokerList.size)
      throw new AdminOperationException("replication factor: " + replicationFactor +
        " larger than available brokers: " + brokerList.size)
    val ret = new mutable.HashMap[Int, List[Int]]()
    val startIndex = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(brokerList.size)
    var currentPartitionId = if (startPartitionId >= 0) startPartitionId else 0

    var nextReplicaShift = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(brokerList.size)
    for (i <- 0 until nPartitions) {
      if (currentPartitionId > 0 && (currentPartitionId % brokerList.size == 0))
        nextReplicaShift += 1
      val firstReplicaIndex = (currentPartitionId + startIndex) % brokerList.size
      var replicaList = List(brokerList(firstReplicaIndex))
      for (j <- 0 until replicationFactor - 1)
        replicaList ::= brokerList(replicaIndex(firstReplicaIndex, nextReplicaShift, j, brokerList.size))
      ret.put(currentPartitionId, replicaList.reverse)
      currentPartitionId = currentPartitionId + 1
    }
    ret.toMap
  }

  /**
   * The pre-assumption here is, we are doing cluster expansion. And all the old brokers have
   * to be in the new brokers list.
   * 
   * There is only one goal to do partitions rebalancing: minimize number of partitions movement.
   * To achieve this goal, we:
   * 1. Get the old brokers to partitions mapping
   * 2. Use priority queue to move partitions from brokers with more partitions to brokers with less partitions
   * 3. Then end condition is number of minimal and maximal partitions diff is smaller than 2.
   *
   */
  def rebalanceReplicasToBrokers(brokerList: Seq[Int],
                                nPartitions: Int,
                                replicationFactor: Int,
                                currentAssignment: Map[Int,Seq[Int]],
                                debug: Boolean): Map[Int, Seq[Int]] = {
    if (nPartitions <= 0)
      throw new AdminOperationException("number of partitions must be larger than 0")
    if (replicationFactor <= 0)
      throw new AdminOperationException("replication factor must be larger than 0")
    if (replicationFactor > brokerList.size)
      throw new AdminOperationException("replication factor: " + replicationFactor +
        " larger than available brokers: " + brokerList.size)
    val currentBrokerToPartitionMapping = mutable.Map.empty[Int, ListBuffer[Int]]
    brokerList.foreach { brokerId =>
      currentBrokerToPartitionMapping(brokerId) = new ListBuffer[Int]
    }
    currentAssignment.foreach(assignment =>
      assignment._2.foreach { brokerId =>
        currentBrokerToPartitionMapping(brokerId) += assignment._1
      }
    )
    if (debug) {
      brokerList.foreach { brokerId =>
        println("Old BrokerToPartitionMapping: broker %s => partitions: [%s]".format(brokerId ,currentBrokerToPartitionMapping(brokerId))) 
      }
    }
    def balancedDescOrdering(assignment: (Int,ListBuffer[Int])) = assignment._2.size
    def balancedAscOrdering(assignment: (Int,ListBuffer[Int])) = assignment._2.size * -1
    // BrokerId -> Seq(Partitions)
    val assignmentToMoveOut = new collection.mutable.PriorityQueue[(Int, ListBuffer[Int])]()(Ordering.by(balancedDescOrdering))
    val assignmentToMoveIn = new collection.mutable.PriorityQueue[(Int, ListBuffer[Int])]()(Ordering.by(balancedAscOrdering))
    currentBrokerToPartitionMapping.foreach(mapping => {
        assignmentToMoveOut.enqueue(mapping)
        assignmentToMoveIn.enqueue(mapping)
      }
    )
    var totalMovedPartitions = 0;
    while (assignmentToMoveOut.head._2.size - assignmentToMoveIn.head._2.size > 1) {
      var headToMoveOut = assignmentToMoveOut.dequeue()
      var headToMoveIn = assignmentToMoveIn.dequeue()
      var parititonToMove = headToMoveOut._2(new Random(System.nanoTime()).nextInt(headToMoveOut._2.size))
      while (headToMoveIn._2.contains(parititonToMove)) {
        parititonToMove = headToMoveOut._2(new Random(System.nanoTime()).nextInt(headToMoveOut._2.size))
      }
      if (debug) {
        println("Move partition %s from broker: %s to broker: %s".format(parititonToMove,headToMoveOut._1,headToMoveIn._1))
      }
      headToMoveOut._2 -= parititonToMove
      headToMoveIn._2 += parititonToMove
      assignmentToMoveOut.enqueue(headToMoveOut)
      assignmentToMoveIn.enqueue(headToMoveIn)
      totalMovedPartitions += 1;
    }
    if (debug) {
      println("Total moved Partitions: %s".format(totalMovedPartitions))
    }
    // partitionId to ListBuffer[brokerId] mapping
    val newPartitionToBrokersMap = mutable.Map.empty[Int, ListBuffer[Int]]
    for (currentPartitionId <- 0 until nPartitions) {
      newPartitionToBrokersMap.put(currentPartitionId, new ListBuffer[Int])
    }
    brokerList.foreach { brokerId =>
      println("New BrokerToPartitionMapping: broker %s => partitions: [%s]".format(brokerId ,currentBrokerToPartitionMapping(brokerId)))
    }
    currentBrokerToPartitionMapping.foreach(mapping =>
      mapping._2.foreach { partitionId =>
        newPartitionToBrokersMap(partitionId) += mapping._1
      }
    )
    val ret = new mutable.HashMap[Int, List[Int]]()
    for (currentPartitionId <- 0 until nPartitions) {
      ret.put(currentPartitionId, newPartitionToBrokersMap(currentPartitionId).toList)
    }
    ret.toMap
  }


 /**
  * Add partitions to existing topic with optional replica assignment
  *
  * @param zkClient Zookeeper client
  * @param topic Topic for adding partitions to
  * @param numPartitions Number of partitions to be set
  * @param replicaAssignmentStr Manual replica assignment
  * @param checkBrokerAvailable Ignore checking if assigned replica broker is available. Only used for testing
  * @param config Pre-existing properties that should be preserved
  */
  def addPartitions(zkClient: ZkClient,
                    brokerList: Seq[Int],
                    topic: String,
                    numPartitions: Int = 1,
                    replicaAssignmentStr: String = "",
                    checkBrokerAvailable: Boolean = true,
                    config: Properties = new Properties) {
    val existingPartitionsReplicaList = ZkUtils.getReplicaAssignmentForTopics(zkClient, List(topic))
    if (existingPartitionsReplicaList.size == 0)
      throw new AdminOperationException("The topic %s does not exist".format(topic))

    val existingReplicaList = existingPartitionsReplicaList.head._2
    val partitionsToAdd = numPartitions - existingPartitionsReplicaList.size
    if (partitionsToAdd <= 0)
      throw new AdminOperationException("The number of partitions for a topic can only be increased")

    val newPartitionReplicaList = if (replicaAssignmentStr == null || replicaAssignmentStr == "")
      AdminUtils.assignReplicasToBrokers(brokerList, partitionsToAdd, existingReplicaList.size, existingReplicaList.head, existingPartitionsReplicaList.size)
    else
      getManualReplicaAssignment(replicaAssignmentStr, brokerList.toSet, existingPartitionsReplicaList.size, checkBrokerAvailable)

    // check if manual assignment has the right replication factor
    val unmatchedRepFactorList = newPartitionReplicaList.values.filter(p => (p.size != existingReplicaList.size))
    if (unmatchedRepFactorList.size != 0)
      throw new AdminOperationException("The replication factor in manual replication assignment " +
        " is not equal to the existing replication factor for the topic " + existingReplicaList.size)

    info("Add partition list for %s is %s".format(topic, newPartitionReplicaList))
    val partitionReplicaList = existingPartitionsReplicaList.map(p => p._1.partition -> p._2)
    // add the new list
    partitionReplicaList ++= newPartitionReplicaList
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, partitionReplicaList, config, true)
  }

  def getManualReplicaAssignment(replicaAssignmentList: String, availableBrokerList: Set[Int], startPartitionId: Int, checkBrokerAvailable: Boolean = true): Map[Int, List[Int]] = {
    var partitionList = replicaAssignmentList.split(",")
    val ret = new mutable.HashMap[Int, List[Int]]()
    var partitionId = startPartitionId
    partitionList = partitionList.takeRight(partitionList.size - partitionId)
    for (i <- 0 until partitionList.size) {
      val brokerList = partitionList(i).split(":").map(s => s.trim().toInt)
      if (brokerList.size <= 0)
        throw new AdminOperationException("replication factor must be larger than 0")
      if (brokerList.size != brokerList.toSet.size)
        throw new AdminOperationException("duplicate brokers in replica assignment: " + brokerList)
      if (checkBrokerAvailable && !brokerList.toSet.subsetOf(availableBrokerList))
        throw new AdminOperationException("some specified brokers not available. specified brokers: " + brokerList.toString +
          "available broker:" + availableBrokerList.toString)
      ret.put(partitionId, brokerList.toList)
      if (ret(partitionId).size != ret(startPartitionId).size)
        throw new AdminOperationException("partition " + i + " has different replication factor: " + brokerList)
      partitionId = partitionId + 1
    }
    ret.toMap
  }
  
  def deleteTopic(zkClient: ZkClient, topic: String) {
    ZkUtils.createPersistentPath(zkClient, ZkUtils.getDeleteTopicPath(topic))
  }
  
  def isConsumerGroupActive(zkClient: ZkClient, group: String) = {
    ZkUtils.getConsumersInGroup(zkClient, group).nonEmpty
  }

  /**
   * Delete the whole directory of the given consumer group if the group is inactive.
   *
   * @param zkClient Zookeeper client
   * @param group Consumer group
   * @return whether or not we deleted the consumer group information
   */
  def deleteConsumerGroupInZK(zkClient: ZkClient, group: String) = {
    if (!isConsumerGroupActive(zkClient, group)) {
      val dir = new ZKGroupDirs(group)
      ZkUtils.deletePathRecursive(zkClient, dir.consumerGroupDir)
      true
    }
    else false
  }

  /**
   * Delete the given consumer group's information for the given topic in Zookeeper if the group is inactive.
   * If the consumer group consumes no other topics, delete the whole consumer group directory.
   *
   * @param zkClient Zookeeper client
   * @param group Consumer group
   * @param topic Topic of the consumer group information we wish to delete
   * @return whether or not we deleted the consumer group information for the given topic
   */
  def deleteConsumerGroupInfoForTopicInZK(zkClient: ZkClient, group: String, topic: String) = {
    val topics = ZkUtils.getTopicsByConsumerGroup(zkClient, group)
    if (topics == Seq(topic)) {
      deleteConsumerGroupInZK(zkClient, group)
    }
    else if (!isConsumerGroupActive(zkClient, group)) {
      val dir = new ZKGroupTopicDirs(group, topic)
      ZkUtils.deletePathRecursive(zkClient, dir.consumerOwnerDir)
      ZkUtils.deletePathRecursive(zkClient, dir.consumerOffsetDir)
      true
    }
    else false
  }

  /**
   * Delete every inactive consumer group's information about the given topic in Zookeeper.
   *
   * @param zkClient Zookeeper client
   * @param topic Topic of the consumer group information we wish to delete
   */
  def deleteAllConsumerGroupInfoForTopicInZK(zkClient: ZkClient, topic: String) {
    val groups = ZkUtils.getAllConsumerGroupsForTopic(zkClient, topic)
    groups.foreach(group => deleteConsumerGroupInfoForTopicInZK(zkClient, group, topic))
  }

  def topicExists(zkClient: ZkClient, topic: String): Boolean = 
    zkClient.exists(ZkUtils.getTopicPath(topic))

  def createTopic(zkClient: ZkClient,
                  brokerList: Seq[Int],
                  topic: String,
                  partitions: Int, 
                  replicationFactor: Int, 
                  topicConfig: Properties = new Properties) {
    val replicaAssignment = AdminUtils.assignReplicasToBrokers(brokerList, partitions, replicationFactor)
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, replicaAssignment, topicConfig)
  }
                  
  def createOrUpdateTopicPartitionAssignmentPathInZK(zkClient: ZkClient,
                                                     topic: String,
                                                     partitionReplicaAssignment: Map[Int, Seq[Int]],
                                                     config: Properties = new Properties,
                                                     update: Boolean = false) {
    // validate arguments
    Topic.validate(topic)
    LogConfig.validate(config)
    require(partitionReplicaAssignment.values.map(_.size).toSet.size == 1, "All partitions should have the same number of replicas.")

    val topicPath = ZkUtils.getTopicPath(topic)
    if(!update && zkClient.exists(topicPath))
      throw new TopicExistsException("Topic \"%s\" already exists.".format(topic))
    partitionReplicaAssignment.values.foreach(reps => require(reps.size == reps.toSet.size, "Duplicate replica assignment found: "  + partitionReplicaAssignment))
    
    // write out the config if there is any, this isn't transactional with the partition assignments
    writeTopicConfig(zkClient, topic, config)
    
    // create the partition assignment
    writeTopicPartitionAssignment(zkClient, topic, partitionReplicaAssignment, update)
  }
  
  private def writeTopicPartitionAssignment(zkClient: ZkClient, topic: String, replicaAssignment: Map[Int, Seq[Int]], update: Boolean) {
    try {
      val zkPath = ZkUtils.getTopicPath(topic)
      val jsonPartitionData = ZkUtils.replicaAssignmentZkData(replicaAssignment.map(e => (e._1.toString -> e._2)))

      if (!update) {
        info("Topic creation " + jsonPartitionData.toString)
        ZkUtils.createPersistentPath(zkClient, zkPath, jsonPartitionData)
      } else {
        info("Topic update " + jsonPartitionData.toString)
        ZkUtils.updatePersistentPath(zkClient, zkPath, jsonPartitionData)
      }
      debug("Updated path %s with %s for replica assignment".format(zkPath, jsonPartitionData))
    } catch {
      case e: ZkNodeExistsException => throw new TopicExistsException("topic %s already exists".format(topic))
      case e2: Throwable => throw new AdminOperationException(e2.toString)
    }
  }
  
  /**
   * Update the config for an existing topic and create a change notification so the change will propagate to other brokers
   * @param zkClient: The ZkClient handle used to write the new config to zookeeper
   * @param topic: The topic for which configs are being changed
   * @param configs: The final set of configs that will be applied to the topic. If any new configs need to be added or
   *                 existing configs need to be deleted, it should be done prior to invoking this API
   *
   */
  def changeTopicConfig(zkClient: ZkClient, topic: String, configs: Properties) {
    if(!topicExists(zkClient, topic))
      throw new AdminOperationException("Topic \"%s\" does not exist.".format(topic))

    // remove the topic overrides
    LogConfig.validate(configs)

    // write the new config--may not exist if there were previously no overrides
    writeTopicConfig(zkClient, topic, configs)
    
    // create the change notification
    zkClient.createPersistentSequential(ZkUtils.TopicConfigChangesPath + "/" + TopicConfigChangeZnodePrefix, Json.encode(topic))
  }
  
  /**
   * Write out the topic config to zk, if there is any
   */
  private def writeTopicConfig(zkClient: ZkClient, topic: String, config: Properties) {
    val configMap: mutable.Map[String, String] = {
      import JavaConversions._
      config
    }
    val map = Map("version" -> 1, "config" -> configMap)
    ZkUtils.updatePersistentPath(zkClient, ZkUtils.getTopicConfigPath(topic), Json.encode(map))
  }
  
  /**
   * Read the topic config (if any) from zk
   */
  def fetchTopicConfig(zkClient: ZkClient, topic: String): Properties = {
    val str: String = zkClient.readData(ZkUtils.getTopicConfigPath(topic), true)
    val props = new Properties()
    if(str != null) {
      Json.parseFull(str) match {
        case None => // there are no config overrides
        case Some(mapAnon: Map[_, _]) =>
          val map = mapAnon collect { case (k: String, v: Any) => k -> v }
          require(map("version") == 1)
          map.get("config") match {
            case Some(config: Map[_, _]) =>
              for(configTup <- config)
                configTup match {
                  case (k: String, v: String) =>
                    props.setProperty(k, v)
                  case _ => throw new IllegalArgumentException("Invalid topic config: " + str)
                }
            case _ => throw new IllegalArgumentException("Invalid topic config: " + str)
          }

        case o => throw new IllegalArgumentException("Unexpected value in config: "  + str)
      }
    }
    props
  }

  def fetchAllTopicConfigs(zkClient: ZkClient): Map[String, Properties] =
    ZkUtils.getAllTopics(zkClient).map(topic => (topic, fetchTopicConfig(zkClient, topic))).toMap

  def fetchTopicMetadataFromZk(topic: String, zkClient: ZkClient): TopicMetadata =
    fetchTopicMetadataFromZk(topic, zkClient, new mutable.HashMap[Int, Broker])

  def fetchTopicMetadataFromZk(topics: Set[String], zkClient: ZkClient): Set[TopicMetadata] = {
    val cachedBrokerInfo = new mutable.HashMap[Int, Broker]()
    topics.map(topic => fetchTopicMetadataFromZk(topic, zkClient, cachedBrokerInfo))
  }



  private def fetchTopicMetadataFromZk(topic: String, zkClient: ZkClient, cachedBrokerInfo: mutable.HashMap[Int, Broker], protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT): TopicMetadata = {
    if(ZkUtils.pathExists(zkClient, ZkUtils.getTopicPath(topic))) {
      val topicPartitionAssignment = ZkUtils.getPartitionAssignmentForTopics(zkClient, List(topic)).get(topic).get
      val sortedPartitions = topicPartitionAssignment.toList.sortWith((m1, m2) => m1._1 < m2._1)
      val partitionMetadata = sortedPartitions.map { partitionMap =>
        val partition = partitionMap._1
        val replicas = partitionMap._2
        val inSyncReplicas = ZkUtils.getInSyncReplicasForPartition(zkClient, topic, partition)
        val leader = ZkUtils.getLeaderForPartition(zkClient, topic, partition)
        debug("replicas = " + replicas + ", in sync replicas = " + inSyncReplicas + ", leader = " + leader)

        var leaderInfo: Option[BrokerEndPoint] = None
        var replicaInfo: Seq[BrokerEndPoint] = Nil
        var isrInfo: Seq[BrokerEndPoint] = Nil
        try {
          leaderInfo = leader match {
            case Some(l) =>
              try {
                Some(getBrokerInfoFromCache(zkClient, cachedBrokerInfo, List(l)).head.getBrokerEndPoint(protocol))
              } catch {
                case e: Throwable => throw new LeaderNotAvailableException("Leader not available for partition [%s,%d]".format(topic, partition), e)
              }
            case None => throw new LeaderNotAvailableException("No leader exists for partition " + partition)
          }
          try {
            replicaInfo = getBrokerInfoFromCache(zkClient, cachedBrokerInfo, replicas).map(_.getBrokerEndPoint(protocol))
            isrInfo = getBrokerInfoFromCache(zkClient, cachedBrokerInfo, inSyncReplicas).map(_.getBrokerEndPoint(protocol))
          } catch {
            case e: Throwable => throw new ReplicaNotAvailableException(e)
          }
          if(replicaInfo.size < replicas.size)
            throw new ReplicaNotAvailableException("Replica information not available for following brokers: " +
              replicas.filterNot(replicaInfo.map(_.id).contains(_)).mkString(","))
          if(isrInfo.size < inSyncReplicas.size)
            throw new ReplicaNotAvailableException("In Sync Replica information not available for following brokers: " +
              inSyncReplicas.filterNot(isrInfo.map(_.id).contains(_)).mkString(","))
          new PartitionMetadata(partition, leaderInfo, replicaInfo, isrInfo, ErrorMapping.NoError)
        } catch {
          case e: Throwable =>
            debug("Error while fetching metadata for partition [%s,%d]".format(topic, partition), e)
            new PartitionMetadata(partition, leaderInfo, replicaInfo, isrInfo,
              ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]))
        }
      }
      new TopicMetadata(topic, partitionMetadata)
    } else {
      // topic doesn't exist, send appropriate error code
      new TopicMetadata(topic, Seq.empty[PartitionMetadata], ErrorMapping.UnknownTopicOrPartitionCode)
    }
  }

  private def getBrokerInfoFromCache(zkClient: ZkClient,
                                     cachedBrokerInfo: scala.collection.mutable.Map[Int, Broker],
                                     brokerIds: Seq[Int]): Seq[Broker] = {
    var failedBrokerIds: ListBuffer[Int] = new ListBuffer()
    val brokerMetadata = brokerIds.map { id =>
      val optionalBrokerInfo = cachedBrokerInfo.get(id)
      optionalBrokerInfo match {
        case Some(brokerInfo) => Some(brokerInfo) // return broker info from the cache
        case None => // fetch it from zookeeper
          ZkUtils.getBrokerInfo(zkClient, id) match {
            case Some(brokerInfo) =>
              cachedBrokerInfo += (id -> brokerInfo)
              Some(brokerInfo)
            case None =>
              failedBrokerIds += id
              None
          }
      }
    }
    brokerMetadata.filter(_.isDefined).map(_.get)
  }

  private def replicaIndex(firstReplicaIndex: Int, secondReplicaShift: Int, replicaIndex: Int, nBrokers: Int): Int = {
    val shift = 1 + (secondReplicaShift + replicaIndex) % (nBrokers - 1)
    (firstReplicaIndex + shift) % nBrokers
  }
}

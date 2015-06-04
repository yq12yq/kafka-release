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

package kafka.security

import java.util
import java.util.Properties

import kafka.admin.AdminUtils
import kafka.api.FetchRequestBuilder
import kafka.common.{ErrorMapping, FailedToSendMessageException}
import kafka.consumer.SimpleConsumer
import kafka.message.Message
import kafka.producer.KeyedMessage
import kafka.serializer.StringEncoder
import kafka.server.{KafkaConfig, KafkaRequestHandler, KafkaServer}
import kafka.utils._
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.log4j.{Level, Logger}
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.Test
import org.scalatest.TestFailedException
import org.scalatest.junit.JUnit3Suite

class SecureProducerConsumerTest extends JUnit3Suite with SecureTestHarness with Logging{
  private val brokerId1 = 0
  private var server1: KafkaServer = null
  private var consumer1: SimpleConsumer = null
  private val requestHandlerLogger = Logger.getLogger(classOf[KafkaRequestHandler])
  private var servers = List.empty[KafkaServer]

  // Creation of consumers is deferred until they are actually needed. This allows us to kill brokers that use random
  // ports and then get a consumer instance that will be pointed at the correct port
  def getConsumer1() = {
    if (consumer1 == null)
      consumer1 = new SimpleConsumer("localhost", server1.boundPort(SecurityProtocol.PLAINTEXTSASL), 1000000, 64*1024, "", protocol = SecurityProtocol.PLAINTEXTSASL)
    consumer1
  }

  override def setUp() {
    super.setUp()
    // set up 2 brokers with 4 partitions each
    val props1 = TestUtils.createBrokerConfig(brokerId1, zkConnect, false, enableKerberos = true)
    props1.put("num.partitions", "1")
    props1.put(KafkaConfig.DefaultReplicationFactorProp, "1")
    val config1 = KafkaConfig.fromProps(props1)

    server1 = TestUtils.createServer(config1)
    servers = List(server1)

    // temporarily set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.FATAL)
  }

  override def tearDown() {
    // restore set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.ERROR)

    if (consumer1 != null)
      consumer1.close()

    server1.shutdown
    CoreUtils.rm(server1.config.logDirs)
    super.tearDown()
  }

  def testProduceAndConsume() {
    val props1 = new util.Properties()
    props1.put("request.required.acks", "-1")

    val topic = "new-topic"
    // create topic with 1 partition and await leadership
    TestUtils.createTopic(zkClient, topic, numPartitions = 1, replicationFactor = 1, servers = servers)

    val producer1 = TestUtils.createProducer[String, String](
      brokerList = TestUtils.getBrokerListStrFromServers(Seq(server1)),
      encoder = classOf[StringEncoder].getName,
      keyEncoder = classOf[StringEncoder].getName,
      partitioner = classOf[StaticPartitioner].getName,
      producerProps = props1,
      enableKerberos = true)

    // Available partition ids should be 0.
    producer1.send(new KeyedMessage[String, String](topic, "test", "test1"))
    producer1.send(new KeyedMessage[String, String](topic, "test", "test2"))
    // get the leader
    val leaderOpt = ZkUtils.getLeaderForPartition(zkClient, topic, 0)
    assertTrue("Leader for topic new-topic partition 0 should exist", leaderOpt.isDefined)
    val leader = leaderOpt.get

    val response1 = getConsumer1().fetch(new FetchRequestBuilder().addFetch(topic, 0, 0, 10000).build())
    val messageSet = response1.messageSet("new-topic", 0).iterator.toBuffer

    assertEquals("Should have fetched 2 messages", 2, messageSet.size)
    assertEquals(new Message(bytes = "test1".getBytes, key = "test".getBytes), messageSet(0).message)
    assertEquals(new Message(bytes = "test2".getBytes, key = "test".getBytes), messageSet(1).message)
    producer1.close()
  }
}

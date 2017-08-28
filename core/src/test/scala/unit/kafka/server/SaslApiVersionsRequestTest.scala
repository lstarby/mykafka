/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package kafka.server

import java.io.IOException
import java.net.Socket
import java.util.Collections

import org.apache.kafka.common.protocol.{ApiKeys, Errors, SecurityProtocol}
import org.apache.kafka.common.requests.{ApiVersionsRequest, ApiVersionsResponse}
import org.apache.kafka.common.requests.SaslHandshakeRequest
import org.apache.kafka.common.requests.SaslHandshakeResponse
import org.junit.{After, Before, Test}
import org.junit.Assert._
import kafka.api.{KafkaSasl, SaslSetup}
import kafka.utils.JaasTestUtils

class SaslApiVersionsRequestTest extends BaseRequestTest with SaslSetup {
  override protected def securityProtocol = SecurityProtocol.SASL_PLAINTEXT
  private val kafkaClientSaslMechanism = "PLAIN"
  private val kafkaServerSaslMechanisms = List("PLAIN")
  protected override val serverSaslProperties = Some(kafkaServerSaslProperties(kafkaServerSaslMechanisms, kafkaClientSaslMechanism))
  protected override val clientSaslProperties = Some(kafkaClientSaslProperties(kafkaClientSaslMechanism))
  override def numBrokers = 1

  @Before
  override def setUp(): Unit = {
    startSasl(jaasSections(kafkaServerSaslMechanisms, Some(kafkaClientSaslMechanism), KafkaSasl, JaasTestUtils.KafkaServerContextName))
    super.setUp()
  }

  @After
  override def tearDown(): Unit = {
    super.tearDown()
    closeSasl()
  }

  @Test
  def testApiVersionsRequestBeforeSaslHandshakeRequest() {
    val plaintextSocket = connect(protocol = securityProtocol)
    try {
      val apiVersionsResponse = sendApiVersionsRequest(plaintextSocket, new ApiVersionsRequest.Builder().build(0))
      ApiVersionsRequestTest.validateApiVersionsResponse(apiVersionsResponse)
      sendSaslHandshakeRequestValidateResponse(plaintextSocket)
    } finally {
      plaintextSocket.close()
    }
  }

  @Test
  def testApiVersionsRequestAfterSaslHandshakeRequest() {
    val plaintextSocket = connect(protocol = securityProtocol)
    try {
      sendSaslHandshakeRequestValidateResponse(plaintextSocket)
      try {
        sendApiVersionsRequest(plaintextSocket, new ApiVersionsRequest.Builder().build(0))
        fail("Versions Request during Sasl handshake did not fail")
      } catch {
        case _: IOException => // expected exception
      }
    } finally {
      plaintextSocket.close()
    }
  }

  @Test
  def testApiVersionsRequestWithUnsupportedVersion() {
    val plaintextSocket = connect(protocol = securityProtocol)
    try {
      val apiVersionsRequest = new ApiVersionsRequest(0)
      val apiVersionsResponse = sendApiVersionsRequest(plaintextSocket, apiVersionsRequest, Some(Short.MaxValue))
      assertEquals(Errors.UNSUPPORTED_VERSION, apiVersionsResponse.error)
      val apiVersionsResponse2 = sendApiVersionsRequest(plaintextSocket, new ApiVersionsRequest.Builder().build(0))
      ApiVersionsRequestTest.validateApiVersionsResponse(apiVersionsResponse2)
      sendSaslHandshakeRequestValidateResponse(plaintextSocket)
    } finally {
      plaintextSocket.close()
    }
  }

  private def sendApiVersionsRequest(socket: Socket, request: ApiVersionsRequest,
                                     apiVersion: Option[Short] = None): ApiVersionsResponse = {
    val response = send(request, ApiKeys.API_VERSIONS, socket, apiVersion)
    ApiVersionsResponse.parse(response, request.version)
  }

  private def sendSaslHandshakeRequestValidateResponse(socket: Socket) {
    val request = new SaslHandshakeRequest("PLAIN")
    val response = send(request, ApiKeys.SASL_HANDSHAKE, socket)
    val handshakeResponse = SaslHandshakeResponse.parse(response, request.version)
    assertEquals(Errors.NONE, handshakeResponse.error)
    assertEquals(Collections.singletonList("PLAIN"), handshakeResponse.enabledMechanisms)
  }
}

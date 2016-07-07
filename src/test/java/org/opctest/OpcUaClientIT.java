package org.opctest;
/*
 * Copyright (c) 2016 Kevin Herron
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 * The Eclipse Public License is available at
 * 	http://www.eclipse.org/legal/epl-v10.html
 * and the Eclipse Distribution License is available at
 * 	http://www.eclipse.org/org/documents/edl-v10.html.
 */


import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.config.OpcUaClientConfig;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaMonitoredItem;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaSubscription;
import org.eclipse.milo.opcua.stack.client.UaTcpStackClient;
import org.eclipse.milo.opcua.stack.core.AttributeId;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.QualifiedName;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MonitoringMode;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoredItemCreateRequest;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoringParameters;
import org.eclipse.milo.opcua.stack.core.types.structured.ReadValueId;
import org.eclipse.milo.opcua.stack.server.tcp.SocketServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static com.google.common.collect.Lists.newArrayList;
import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.uint;
import static org.testng.Assert.assertEquals;

public class OpcUaClientIT {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private OpcUaClient client;


    @BeforeTest
    public void startClient() throws Exception {
        logger.info("startClient()");


        EndpointDescription[] endpoints = UaTcpStackClient.getEndpoints("opc.tcp://opcua.demo-this.com:51210/UA/SampleServer").get();

        EndpointDescription endpoint = Arrays.stream(endpoints)
                .filter(e -> e.getSecurityPolicyUri().equals(SecurityPolicy.None.getSecurityPolicyUri()))
                .findFirst().orElseThrow(() -> new Exception("no desired endpoints returned"));

        OpcUaClientConfig clientConfig = OpcUaClientConfig.builder()
                .setApplicationName(LocalizedText.english("digitalpetri opc-ua client"))
                .setApplicationUri("urn:digitalpetri:opcua:client")
                .setEndpoint(endpoint)
                .setRequestTimeout(uint(60000))
                .build();

        client = new OpcUaClient(clientConfig);
    }

    @AfterTest
    public void stopClient() {
        logger.info("stopClient()");

        try {
            client.disconnect().get();
        } catch (InterruptedException | ExecutionException e) {
            logger.warn("Error disconnecting client.", e);
        }
        SocketServer.shutdownAll();
    }

    @Test
    public void testSubscribe() throws Exception {
        logger.info("testSubscribe()");
        final HashSet<String> receivedMonitoredItemIds = new HashSet<>();
        final CompletableFuture<Integer> allMonitoredItemsNodeIdsReceived = new CompletableFuture<Integer>();
        BiConsumer<UaMonitoredItem, DataValue> consumer = (UaMonitoredItem u, DataValue d) -> {
//           //subscription.createMonitoredItems(TimestampsToReturn.Both, newArrayList(request1, request2, request3))
            //this BiConsumer should receive a different NodeId for each value
            //currently it receives 3 different values(corresponding to request1..3)
            // but the same NodeId for the UaMonitoredItem
            receivedMonitoredItemIds.add(u.getReadValueId().getNodeId().getIdentifier().toString());
            System.out.println(u.getReadValueId().getNodeId().getIdentifier());
            System.out.println(d.getValue().toString());
            if (receivedMonitoredItemIds.size() == 3)
                allMonitoredItemsNodeIdsReceived.complete(3);

        };

        // create a subscription and a monitored item
        UaSubscription subscription = client.getSubscriptionManager().createSubscription(1000.0).get();

        ReadValueId readValueId1 = new ReadValueId(
                new NodeId(2, 10854),//DoubleValue
                AttributeId.Value.uid(), null, QualifiedName.NULL_VALUE);
        ReadValueId readValueId2 = new ReadValueId(
                new NodeId(2, 10846),//ByteValue
                AttributeId.Value.uid(), null, QualifiedName.NULL_VALUE);
        ReadValueId readValueId3 = new ReadValueId(
                new NodeId(2, 10849),//IntValue
                AttributeId.Value.uid(), null, QualifiedName.NULL_VALUE);


        MonitoringParameters parameters = new MonitoringParameters(
                uint(1),    // client handle
                1000.0,     // sampling interval
                null,       // no (default) filter
                uint(10),   // queue size
                true);      // discard oldest

        MonitoredItemCreateRequest request1 = new MonitoredItemCreateRequest(
                readValueId1, MonitoringMode.Reporting, parameters);

        MonitoredItemCreateRequest request2 = new MonitoredItemCreateRequest(
                readValueId2, MonitoringMode.Reporting, parameters);

        MonitoredItemCreateRequest request3 = new MonitoredItemCreateRequest(
                readValueId3, MonitoringMode.Reporting, parameters);

        List<UaMonitoredItem> items = subscription
                .createMonitoredItems(TimestampsToReturn.Both, newArrayList(request1, request2, request3)).get();


        UaMonitoredItem item1 = items.get(0);
        UaMonitoredItem item2 = items.get(1);
        UaMonitoredItem item3 = items.get(2);


        CompletableFuture<DataValue> f = new CompletableFuture<>();
        item1.setValueConsumer(consumer);
        item2.setValueConsumer(consumer);
        item3.setValueConsumer(consumer);


        assertEquals(allMonitoredItemsNodeIdsReceived.get(20, TimeUnit.SECONDS), (Integer)3, "should receive 3 different nodeIds");
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.dataproxy.sink.mq.pulsar;

import org.apache.inlong.common.enums.DataProxyErrCode;
import org.apache.inlong.common.monitor.LogCounter;
import org.apache.inlong.dataproxy.config.ConfigManager;
import org.apache.inlong.dataproxy.config.pojo.CacheClusterConfig;
import org.apache.inlong.dataproxy.config.pojo.IdTopicConfig;
import org.apache.inlong.dataproxy.consts.StatConstants;
import org.apache.inlong.dataproxy.sink.common.EventHandler;
import org.apache.inlong.dataproxy.sink.mq.BatchPackProfile;
import org.apache.inlong.dataproxy.sink.mq.MessageQueueHandler;
import org.apache.inlong.dataproxy.sink.mq.MessageQueueZoneSinkContext;
import org.apache.inlong.dataproxy.sink.mq.PackProfile;
import org.apache.inlong.dataproxy.sink.mq.SimplePackProfile;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Context;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SizeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.apache.inlong.dataproxy.consts.ConfigConstants.KEY_STATS_INTERVAL_SECONDS;

/**
 * PulsarHandler
 */
public class PulsarHandler implements MessageQueueHandler {

    public static final Logger LOG = LoggerFactory.getLogger(PulsarHandler.class);
    // log print count
    private static final LogCounter logCounter = new LogCounter(10, 100000, 30 * 1000);

    public static final String KEY_TENANT = "tenant";
    public static final String KEY_NAMESPACE = "namespace";

    public static final String KEY_SERVICE_URL = "serviceUrl";
    public static final String KEY_AUTHENTICATION = "authentication";

    public static final String KEY_ENABLEBATCHING = "enableBatching";
    public static final String KEY_BATCHINGMAXBYTES = "batchingMaxBytes";
    public static final String KEY_BATCHINGMAXMESSAGES = "batchingMaxMessages";
    public static final String KEY_BATCHINGMAXPUBLISHDELAY = "batchingMaxPublishDelay";
    public static final String KEY_MAXPENDINGMESSAGES = "maxPendingMessages";
    public static final String KEY_MAXPENDINGMESSAGESACROSSPARTITIONS = "maxPendingMessagesAcrossPartitions";
    public static final String KEY_SENDTIMEOUT = "sendTimeout";
    public static final String KEY_COMPRESSIONTYPE = "compressionType";
    public static final String KEY_BLOCKIFQUEUEFULL = "blockIfQueueFull";
    public static final String KEY_ROUNDROBINROUTERBATCHINGPARTITIONSWITCHFREQUENCY = "roundRobinRouter"
            + "BatchingPartitionSwitchFrequency";

    public static final String KEY_IOTHREADS = "ioThreads";
    public static final String KEY_MEMORYLIMIT = "memoryLimit";
    public static final String KEY_CONNECTIONSPERBROKER = "connectionsPerBroker";

    private CacheClusterConfig config;
    private String clusterName;
    private MessageQueueZoneSinkContext sinkContext;

    private String tenant;
    private String namespace;
    private ThreadLocal<EventHandler> handlerLocal = new ThreadLocal<>();

    /**
     * pulsar client
     */
    private PulsarClient client;
    private ProducerBuilder<byte[]> baseBuilder;

    private ConcurrentHashMap<String, Producer<byte[]>> producerMap = new ConcurrentHashMap<>();

    /**
     * init
     * @param config
     * @param sinkContext
     */
    public void init(CacheClusterConfig config, MessageQueueZoneSinkContext sinkContext) {
        this.config = config;
        this.clusterName = config.getClusterName();
        this.sinkContext = sinkContext;
        this.tenant = config.getParams().get(KEY_TENANT);
        this.namespace = config.getParams().get(KEY_NAMESPACE);
    }

    /**
     * start
     */
    @Override
    public void start() {
        // create pulsar client
        try {
            String serviceUrl = config.getParams().get(KEY_SERVICE_URL);
            String authentication = config.getParams().get(KEY_AUTHENTICATION);
            if (StringUtils.isEmpty(authentication)) {
                authentication = config.getToken();
            }
            Context context = sinkContext.getProducerContext();
            ClientBuilder builder = PulsarClient.builder();
            if (StringUtils.isNotEmpty(authentication)) {
                builder.authentication(AuthenticationFactory.token(authentication));
            }
            this.client = builder
                    .serviceUrl(serviceUrl)
                    .ioThreads(context.getInteger(KEY_IOTHREADS, 1))
                    .memoryLimit(context.getLong(KEY_MEMORYLIMIT, 1073741824L), SizeUnit.BYTES)
                    .connectionsPerBroker(context.getInteger(KEY_CONNECTIONSPERBROKER, 10))
                    .statsInterval(NumberUtils.toLong(config.getParams().get(KEY_STATS_INTERVAL_SECONDS), -1),
                            TimeUnit.SECONDS)
                    .build();
            this.baseBuilder = client.newProducer();
            // Map<String, Object> builderConf = new HashMap<>();
            // builderConf.putAll(context.getParameters());
            this.baseBuilder
                    .sendTimeout(context.getInteger(KEY_SENDTIMEOUT, 0), TimeUnit.MILLISECONDS)
                    .maxPendingMessages(context.getInteger(KEY_MAXPENDINGMESSAGES, 500))
                    .maxPendingMessagesAcrossPartitions(
                            context.getInteger(KEY_MAXPENDINGMESSAGESACROSSPARTITIONS, 60000));
            this.baseBuilder
                    .batchingMaxMessages(context.getInteger(KEY_BATCHINGMAXMESSAGES, 500))
                    .batchingMaxPublishDelay(context.getInteger(KEY_BATCHINGMAXPUBLISHDELAY, 100),
                            TimeUnit.MILLISECONDS)
                    .batchingMaxBytes(context.getInteger(KEY_BATCHINGMAXBYTES, 131072));
            this.baseBuilder
                    .accessMode(ProducerAccessMode.Shared)
                    .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
                    .blockIfQueueFull(context.getBoolean(KEY_BLOCKIFQUEUEFULL, true));
            this.baseBuilder
                    .roundRobinRouterBatchingPartitionSwitchFrequency(
                            context.getInteger(KEY_ROUNDROBINROUTERBATCHINGPARTITIONSWITCHFREQUENCY, 60))
                    .enableBatching(context.getBoolean(KEY_ENABLEBATCHING, true))
                    .compressionType(this.getPulsarCompressionType());
        } catch (Throwable e) {
            LOG.error(e.getMessage(), e);
        }
        LOG.info("pulsar handler started");
    }

    /**
     * stop
     */
    @Override
    public void stop() {
        for (Entry<String, Producer<byte[]>> entry : this.producerMap.entrySet()) {
            try {
                entry.getValue().close();
            } catch (PulsarClientException e) {
                LOG.error(e.getMessage(), e);
            }
        }
        try {
            this.client.close();
        } catch (PulsarClientException e) {
            LOG.error(e.getMessage(), e);
        }
        LOG.info("pulsar handler stopped");
    }

    @Override
    public void publishTopic(Set<String> topicSet) {
        //
    }

    /**
     * send
     * @param profile
     * @return
     */
    @Override
    public boolean send(PackProfile profile) {
        try {
            // idConfig
            IdTopicConfig idConfig = ConfigManager.getInstance().getIdTopicConfig(
                    profile.getInlongGroupId(), profile.getInlongStreamId());
            if (idConfig == null) {
                sinkContext.fileMetricEventInc(StatConstants.EVENT_SINK_NOUID);
                sinkContext.addSendResultMetric(profile, clusterName, profile.getUid(), false, 0);
                sinkContext.getDispatchQueue().release(profile.getSize());
                profile.fail(DataProxyErrCode.GROUPID_OR_STREAMID_NOT_CONFIGURE, "");
                return false;
            }
            // topic
            String producerTopic = idConfig.getPulsarTopicName(tenant, namespace);
            if (producerTopic == null) {
                sinkContext.fileMetricEventInc(StatConstants.EVENT_SINK_NOTOPIC);
                sinkContext.addSendResultMetric(profile, clusterName, profile.getUid(), false, 0);
                sinkContext.getDispatchQueue().release(profile.getSize());
                profile.fail(DataProxyErrCode.TOPIC_IS_BLANK, "");
                return false;
            }
            // get producer
            Producer<byte[]> producer = this.producerMap.get(producerTopic);
            if (producer == null) {
                try {
                    LOG.info("try to new a object for topic " + producerTopic);
                    SecureRandom secureRandom = new SecureRandom(
                            (producerTopic + System.currentTimeMillis()).getBytes());
                    String producerName = producerTopic + "-" + secureRandom.nextLong();
                    producer = baseBuilder.clone().topic(producerTopic)
                            .producerName(producerName)
                            .create();
                    LOG.info("create new producer success:{}", producer.getProducerName());
                    Producer<byte[]> oldProducer = this.producerMap.putIfAbsent(producerTopic, producer);
                    if (oldProducer != null) {
                        producer.close();
                        LOG.info("close producer success:{}", producer.getProducerName());
                        producer = oldProducer;
                    }
                } catch (Throwable ex) {
                    LOG.error("create new producer failed", ex);
                }
            }
            // create producer failed
            if (producer == null) {
                sinkContext.fileMetricEventInc(StatConstants.EVENT_SINK_NOPRODUCER);
                sinkContext.processSendFail(profile, clusterName, producerTopic, 0,
                        DataProxyErrCode.PRODUCER_IS_NULL, "");
                return false;
            }
            // send
            if (profile instanceof SimplePackProfile) {
                this.sendSimplePackProfile((SimplePackProfile) profile, idConfig, producer, producerTopic);
            } else {
                this.sendBatchPackProfile((BatchPackProfile) profile, idConfig, producer, producerTopic);
            }
            return true;
        } catch (Exception ex) {
            sinkContext.fileMetricEventInc(StatConstants.EVENT_SINK_SENDEXCEPT);
            sinkContext.processSendFail(profile, clusterName, profile.getUid(), 0,
                    DataProxyErrCode.SEND_REQUEST_TO_MQ_FAILURE, ex.getMessage());
            LOG.error(ex.getMessage(), ex);
            return false;
        }
    }

    /**
     * getPulsarCompressionType
     * 
     * @return CompressionType
     */
    private CompressionType getPulsarCompressionType() {
        Context context = sinkContext.getProducerContext();
        String type = context.getString(KEY_COMPRESSIONTYPE, CompressionType.SNAPPY.name());
        switch (type) {
            case "LZ4":
                return CompressionType.LZ4;
            case "ZLIB":
                return CompressionType.ZLIB;
            case "ZSTD":
                return CompressionType.ZSTD;
            case "SNAPPY":
                return CompressionType.SNAPPY;
            case "NONE":
            default:
                return CompressionType.NONE;
        }
    }

    /**
     * send BatchPackProfile
     */
    private void sendBatchPackProfile(BatchPackProfile batchProfile, IdTopicConfig idConfig, Producer<byte[]> producer,
            String producerTopic) throws Exception {
        EventHandler handler = handlerLocal.get();
        if (handler == null) {
            handler = this.sinkContext.createEventHandler();
            handlerLocal.set(handler);
        }
        // headers
        Map<String, String> headers = handler.parseHeader(idConfig, batchProfile, sinkContext.getNodeId(),
                sinkContext.getCompressType());
        // compress
        byte[] bodyBytes = handler.parseBody(idConfig, batchProfile, sinkContext.getCompressType());
        // metric
        sinkContext.addSendMetric(batchProfile, clusterName, producerTopic, bodyBytes.length);
        // sendAsync
        long sendTime = System.currentTimeMillis();
        CompletableFuture<MessageId> future = producer.newMessage().properties(headers)
                .value(bodyBytes).sendAsync();
        // callback
        future.whenCompleteAsync((msgId, ex) -> {
            if (ex != null) {
                sinkContext.fileMetricEventInc(StatConstants.EVENT_SINK_RECEIVEEXCEPT);
                sinkContext.processSendFail(batchProfile, clusterName, producerTopic, sendTime,
                        DataProxyErrCode.MQ_RETURN_ERROR, ex.getMessage());
                LOG.error("Send ProfileV1 to Pulsar failure", ex);
            } else {
                sinkContext.fileMetricEventInc(StatConstants.EVENT_SINK_SUCCESS);
                sinkContext.addSendResultMetric(batchProfile, clusterName, producerTopic, true, sendTime);
                sinkContext.getDispatchQueue().release(batchProfile.getSize());
                batchProfile.ack();
            }
        });
    }

    /**
     * send SimplePackProfile
     */
    private void sendSimplePackProfile(SimplePackProfile simpleProfile, IdTopicConfig idConfig,
            Producer<byte[]> producer,
            String producerTopic) throws Exception {
        // headers
        Map<String, String> headers = simpleProfile.getProperties();
        // body
        byte[] bodyBytes = simpleProfile.getEvent().getBody();
        // metric
        sinkContext.addSendMetric(simpleProfile, clusterName, producerTopic, bodyBytes.length);
        // sendAsync
        long sendTime = System.currentTimeMillis();
        CompletableFuture<MessageId> future = producer.newMessage().properties(headers)
                .value(bodyBytes).sendAsync();
        // callback
        future.whenCompleteAsync((msgId, ex) -> {
            if (ex != null) {
                sinkContext.fileMetricEventInc(StatConstants.EVENT_SINK_RECEIVEEXCEPT);
                sinkContext.processSendFail(simpleProfile, clusterName, producerTopic, sendTime,
                        DataProxyErrCode.MQ_RETURN_ERROR, ex.getMessage());
                LOG.error("Send SimpleProfileV0 to Pulsar failure", ex);
            } else {
                sinkContext.fileMetricEventInc(StatConstants.EVENT_SINK_SUCCESS);
                sinkContext.addSendResultMetric(simpleProfile, clusterName, producerTopic, true, sendTime);
                sinkContext.getDispatchQueue().release(simpleProfile.getSize());
                simpleProfile.ack();
            }
        });
    }
}

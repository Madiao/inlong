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

package org.apache.inlong.manager.pojo.sort.node.provider;

import org.apache.inlong.common.enums.DataTypeEnum;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.pojo.sink.starrocks.StarRocksSink;
import org.apache.inlong.manager.pojo.sort.node.base.LoadNodeProvider;
import org.apache.inlong.manager.pojo.stream.StreamField;
import org.apache.inlong.manager.pojo.stream.StreamNode;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.node.LoadNode;
import org.apache.inlong.sort.protocol.node.format.CanalJsonFormat;
import org.apache.inlong.sort.protocol.node.format.DebeziumJsonFormat;
import org.apache.inlong.sort.protocol.node.format.Format;
import org.apache.inlong.sort.protocol.node.load.StarRocksLoadNode;
import org.apache.inlong.sort.protocol.transformation.FieldRelation;

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * The Provider for creating StarRocks load nodes.
 */
public class StarRocksProvider implements LoadNodeProvider {

    @Override
    public Boolean accept(String sinkType) {
        return SinkType.STARROCKS.equals(sinkType);
    }

    @Override
    public LoadNode createLoadNode(StreamNode nodeInfo, Map<String, StreamField> constantFieldMap) {
        StarRocksSink starRocksSink = (StarRocksSink) nodeInfo;
        Map<String, String> properties = parseProperties(starRocksSink.getProperties());
        List<FieldInfo> fieldInfos = parseSinkFieldInfos(starRocksSink.getSinkFieldList(), starRocksSink.getSinkName());
        List<FieldRelation> fieldRelations = parseSinkFields(starRocksSink.getSinkFieldList(), constantFieldMap);

        Format format = null;
        if (Boolean.TRUE.equals(starRocksSink.getSinkMultipleEnable())
                && StringUtils.isNotBlank(starRocksSink.getSinkMultipleFormat())) {
            DataTypeEnum dataType = DataTypeEnum.forType(starRocksSink.getSinkMultipleFormat());
            switch (dataType) {
                case CANAL:
                    format = new CanalJsonFormat();
                    break;
                case DEBEZIUM_JSON:
                    format = new DebeziumJsonFormat();
                    break;
                default:
                    throw new IllegalArgumentException(
                            String.format("Unsupported dataType=%s for StarRocks", dataType));
            }
        }
        return new StarRocksLoadNode(
                starRocksSink.getSinkName(),
                starRocksSink.getSinkName(),
                fieldInfos,
                fieldRelations,
                null,
                null,
                null,
                properties,
                starRocksSink.getJdbcUrl(),
                starRocksSink.getLoadUrl(),
                starRocksSink.getUsername(),
                starRocksSink.getPassword(),
                starRocksSink.getDatabaseName(),
                starRocksSink.getTableName(),
                starRocksSink.getPrimaryKey(),
                starRocksSink.getSinkMultipleEnable(),
                format,
                starRocksSink.getDatabasePattern(),
                starRocksSink.getTablePattern());
    }
}
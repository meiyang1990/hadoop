// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.timelineservice.storage.reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.HBaseTimelineStorageUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityRowKeyPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityTableRW;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

/**
 * 时间线实体类型读取器，根据读取上下文列出所有可用的实体类型。
 * 目前仅支持列出单个YARN应用下的所有实体类型。
 */
public final class EntityTypeReader extends AbstractTimelineStorageReader {

  private static final Logger LOG =
      LoggerFactory.getLogger(EntityTypeReader.class);
  private static final EntityTableRW ENTITY_TABLE = new EntityTableRW();

  /**
   * 构造函数，初始化实体类型读取器。
   * @param context 时间线读取上下文
   */
  public EntityTypeReader(TimelineReaderContext context) {
    super(context);
  }

  /**
   * 从HBase存储中读取给定上下文下的所有时间线实体类型。
   *
   * @param hbaseConf HBase配置
   * @param conn HBase连接
   * @return 实体类型集合，每个返回对象仅设置了type字段
   * @throws IOException 读取过程中发生异常时抛出
   */
  public Set<String> readEntityTypes(Configuration hbaseConf,
      Connection conn) throws IOException {

    // 验证必填参数
    validateParams();
    // 补充参数信息
    augmentParams(hbaseConf, conn);

    // 使用TreeSet存储排序后的去重实体类型
    Set<String> types = new TreeSet<>();
    TimelineReaderContext context = getContext();
    // 构造当前应用对应的实体行键前缀
    EntityRowKeyPrefix prefix = new EntityRowKeyPrefix(context.getClusterId(),
        context.getUserId(), context.getFlowName(), context.getFlowRunId(),
        context.getAppId());
    // 初始起始行键为前缀本身
    byte[] currRowKey = prefix.getRowKeyPrefix();
    // 计算范围查询的结束行键：前缀最后一位加1，实现前缀范围查询
    byte[] nextRowKey = prefix.getRowKeyPrefix();
    nextRowKey[nextRowKey.length - 1]++;

    // 构造过滤条件列表，仅获取实体类型所需信息，减少数据传输
    FilterList typeFilterList = new FilterList();
    // 仅返回每个行键的第一个单元格，减少扫描量
    typeFilterList.addFilter(new FirstKeyOnlyFilter());
    // 仅返回键信息，不需要值，进一步减少数据传输
    typeFilterList.addFilter(new KeyOnlyFilter());
    // 每页只获取1条记录，因为同一类型的所有实体都在同一个前缀下
    typeFilterList.addFilter(new PageFilter(1));
    LOG.debug("FilterList created for scan is - {}", typeFilterList);

    int counter = 0;
    // 循环分页扫描，每次找出下一个实体类型
    while (true) {
      // 使用try-with-resources自动关闭ResultScanner
      try (ResultScanner results =
          getResult(hbaseConf, conn, typeFilterList, currRowKey, nextRowKey)) {
        // 解析当前结果获取实体类型
        TimelineEntity entity = parseEntityForType(results.next());
        // 没有更多结果时退出循环
        if (entity == null) {
          break;
        }
        ++counter;
        // 添加类型到结果集，自动去重
        if (!types.add(entity.getType())) {
          LOG.warn("Failed to add type " + entity.getType()
              + " to the result set because there is a duplicated copy. ");
        }
        String currType = entity.getType();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Current row key: " + Arrays.toString(currRowKey));
          LOG.debug("New entity type discovered: " + currType);
        }
        // 更新下一轮扫描的起始行键
        currRowKey = getNextRowKey(prefix.getRowKeyPrefix(), currType);
      }
    }
    LOG.debug("Scanned {} records for {} types", counter, types.size());
    return types;
  }

  @Override
  protected void validateParams() {
    if (getContext() == null) {
      throw new NullPointerException("context shouldn't be null");
    }
    if (getContext().getClusterId() == null) {
      throw new NullPointerException("clusterId shouldn't be null");
    }
    if (getContext().getAppId() == null) {
      throw new NullPointerException("appId shouldn't be null");
    }
  }

  /**
   * 根据当前前缀和已发现类型，计算下一轮扫描的起始行键前缀。
   *
   * @param currRowKeyPrefix 当前前缀，包含集群、用户、流、流运行、应用ID信息
   * @param entityType 当前已发现的实体类型
   * @return 下一个可能行键的前缀，用于下一轮扫描
   */
  private static byte[] getNextRowKey(byte[] currRowKeyPrefix,
      String entityType) {
    if (currRowKeyPrefix == null || entityType == null) {
      return null;
    }

    // 编码实体类型并添加分隔符
    byte[] entityTypeEncoded = Separator.QUALIFIERS.join(
        Separator.encode(entityType, Separator.SPACE, Separator.TAB,
            Separator.QUALIFIERS),
        Separator.EMPTY_BYTES);

    // 构造完整行键：前缀 + 编码后的实体类型
    byte[] currRowKey
        = new byte[currRowKeyPrefix.length + entityTypeEncoded.length];
    System.arraycopy(currRowKeyPrefix, 0, currRowKey, 0,
        currRowKeyPrefix.length);
    System.arraycopy(entityTypeEncoded, 0, currRowKey, currRowKeyPrefix.length,
        entityTypeEncoded.length);

    // 计算当前行键的下一个可能前缀，用于下一轮范围扫描
    return HBaseTimelineStorageUtils.calculateTheClosestNextRowKeyForPrefix(
        currRowKey);
  }

  /**
   * 执行HBase扫描获取结果扫描器。
   * @param hbaseConf HBase配置
   * @param conn HBase连接
   * @param filterList 过滤器列表
   * @param startPrefix 扫描起始前缀
   * @param endPrefix 扫描结束前缀
   * @return 结果扫描器
   * @throws IOException 扫描出错时抛出
   */
  private ResultScanner getResult(Configuration hbaseConf, Connection conn,
      FilterList filterList, byte[] startPrefix, byte[] endPrefix)
      throws IOException {
    // 构造Scan对象，设置范围、过滤器和小扫描模式
    Scan scan = new Scan()
        .withStartRow(startPrefix)
        .withStopRow(endPrefix)
        .setFilter(filterList)
        .setSmall(true);
    // 从实体表获取扫描结果
    return ENTITY_TABLE.getResultScanner(hbaseConf, conn, scan);
  }

  /**
   * 从HBase Result解析出实体类型信息。
   * @param result HBase查询结果
   * @return 仅包含类型信息的TimelineEntity对象，无结果时返回null
   * @throws IOException 解析行键出错时抛出
   */
  private TimelineEntity parseEntityForType(Result result)
      throws IOException {
    if (result == null || result.isEmpty()) {
      return null;
    }
    TimelineEntity entity = new TimelineEntity();
    // 从行键解析出完整实体行键信息
    EntityRowKey newRowKey = EntityRowKey.parseRowKey(result.getRow());
    // 提取实体类型设置到返回对象中
    entity.setType(newRowKey.getEntityType());
    return entity;
  }

}
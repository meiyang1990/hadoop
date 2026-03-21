// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.timelineservice.storage.common;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Query;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * HBase时间线服务后端存储的通用工具类集合，提供配置加载、行键计算、时间范围设置等通用功能。
 */
public final class HBaseTimelineStorageUtils {
  private static final Logger LOG =
      LoggerFactory.getLogger(HBaseTimelineStorageUtils.class);

  private HBaseTimelineStorageUtils() {
  }

  /**
   * 加载时间线服务HBase存储的配置，优先使用配置文件中指定的HBase配置文件路径，
   * 若未指定则使用类路径下的默认HBase配置。
   * 
   * @param conf YARN配置对象，不能为null，用于读取HBase配置文件路径
   * @return 合并后的HBase配置对象
   * @throws IOException 当配置文件路径指定但无法读取时抛出IO异常
   */
  public static Configuration getTimelineServiceHBaseConf(Configuration conf)
      throws IOException {
    if (conf == null) {
      throw new NullPointerException();
    }

    Configuration hbaseConf;
    // 从YARN配置中获取HBase配置文件路径
    String timelineServiceHBaseConfFilePath =
        conf.get(YarnConfiguration.TIMELINE_SERVICE_HBASE_CONFIGURATION_FILE);

    if (timelineServiceHBaseConfFilePath != null
          && timelineServiceHBaseConfFilePath.length() > 0) {
      LOG.info("Using hbase configuration at " +
          timelineServiceHBaseConfFilePath);
      // 克隆输入配置，避免修改原配置对象
      hbaseConf = new Configuration(conf);
      Configuration plainHBaseConf = new Configuration(false);
      Path hbaseConfigPath = new Path(timelineServiceHBaseConfFilePath);
      // 自动关闭流资源
      try (FileSystem fs =
          FileSystem.newInstance(hbaseConfigPath.toUri(), conf);
          FSDataInputStream in = fs.open(hbaseConfigPath)) {
        // 加载从HDFS读取的HBase配置
        plainHBaseConf.addResource(in);
        // 将加载的HBase配置合并到基础配置
        HBaseConfiguration.merge(hbaseConf, plainHBaseConf);
      }
    } else {
      // 未指定自定义配置，使用类路径下默认的HBase配置
      hbaseConf = HBaseConfiguration.create(conf);
    }
    return hbaseConf;
  }

  /**
   * 根据给定行键前缀计算出范围扫描用的截止行键，用于HBase前缀扫描，
   * 可以将扫描范围限定为所有以该前缀开头的行。
   *
   * @param rowKeyPrefix 输入的行键前缀字节数组
   * @return 最接近的下一个行键，用于作为HBase扫描的stopRow
   */
  public static byte[] calculateTheClosestNextRowKeyForPrefix(
      byte[] rowKeyPrefix) {
    // 把行键看作无符号大整数，对其执行+1操作
    // 从末尾向前查找第一个不是0xFF的字节位置
    int offset = rowKeyPrefix.length;
    while (offset > 0) {
      if (rowKeyPrefix[offset - 1] != (byte) 0xFF) {
        break;
      }
      offset--;
    }

    if (offset == 0) {
      // 所有字节都是0xFF，已经是最大可能前缀，直接返回表结束标记
      return HConstants.EMPTY_END_ROW;
    }

    // 复制前缀到offset位置，只保留第一个非0xFF之前的部分
    byte[] newStopRow = Arrays.copyOfRange(rowKeyPrefix, 0, offset);
    // 对最后一个字节执行+1，得到下一个行键
    newStopRow[newStopRow.length - 1]++;
    return newStopRow;
  }

  /**
   * 为查询设置指标列族的时间范围过滤，仅查询指定时间范围内的指标数据。
   * @param query HBase查询对象
   * @param metricsCf 指标列族字节数组
   * @param tsBegin 开始时间戳
   * @param tsEnd 结束时间戳
   */
  public static void setMetricsTimeRange(Query query, byte[] metricsCf,
      long tsBegin, long tsEnd) {
    if (tsBegin != 0 || tsEnd != Long.MAX_VALUE) {
      // HBase时间范围是左闭右开，结束点需要+1
      query.setColumnFamilyTimeRange(metricsCf,
          tsBegin, ((tsEnd == Long.MAX_VALUE) ? Long.MAX_VALUE : (tsEnd + 1)));
    }
  }
}
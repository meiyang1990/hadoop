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
package org.apache.hadoop.yarn.server.timelineservice.storage.common;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 时间线服务HBase存储schema相关常量定义，用于存储时间线实体信息。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class TimelineHBaseSchemaConstants {
  private TimelineHBaseSchemaConstants() {
  }

  /**
   * 用户名前缀开头的HBase表预拆分点，用于优化表分区分布。
   * TODO: 这个预拆分点未来需要改为可配置项，允许不同部署根据自身常用用户名前缀调整。
   */
  private final static byte[][] USERNAME_SPLITS = {
      Bytes.toBytes("a"), Bytes.toBytes("ad"), Bytes.toBytes("an"),
      Bytes.toBytes("b"), Bytes.toBytes("ca"), Bytes.toBytes("cl"),
      Bytes.toBytes("d"), Bytes.toBytes("e"), Bytes.toBytes("f"),
      Bytes.toBytes("g"), Bytes.toBytes("h"), Bytes.toBytes("i"),
      Bytes.toBytes("j"), Bytes.toBytes("k"), Bytes.toBytes("l"),
      Bytes.toBytes("m"), Bytes.toBytes("n"), Bytes.toBytes("o"),
      Bytes.toBytes("q"), Bytes.toBytes("r"), Bytes.toBytes("s"),
      Bytes.toBytes("se"), Bytes.toBytes("t"), Bytes.toBytes("u"),
      Bytes.toBytes("v"), Bytes.toBytes("w"), Bytes.toBytes("x"),
      Bytes.toBytes("y"), Bytes.toBytes("z")
  };

  /**
   * 用户名前缀拆分时，用于自动拆分的前缀长度，默认值为4。
   */
  public static final String USERNAME_SPLIT_KEY_PREFIX_LENGTH = "4";

  /**
   * 获取用户名前缀表的预拆分点（返回深拷贝保证内部数据不被修改）。
   * @return 用户名前缀预拆分点数组
   */
  public static byte[][] getUsernameSplits() {
    // 克隆外层数组
    byte[][] kloon = USERNAME_SPLITS.clone();
    // 深拷贝每个拆分点字节数组
    for (int row = 0; row < USERNAME_SPLITS.length; row++) {
      kloon[row] = Bytes.copy(USERNAME_SPLITS[row]);
    }
    return kloon;
  }

}
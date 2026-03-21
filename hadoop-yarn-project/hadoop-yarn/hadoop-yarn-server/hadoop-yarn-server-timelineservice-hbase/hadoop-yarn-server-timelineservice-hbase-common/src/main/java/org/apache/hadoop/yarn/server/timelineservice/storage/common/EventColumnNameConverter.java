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

/**
 * YARN Timeline服务HBase存储层：应用和实体表事件列名编解码工具，
 * 将{@link EventColumnName}对象编码为HBase存储用字节数组，
 * 并将HBase中存储的字节数组解码还原为{@link EventColumnName}对象。
 * <p>
 * 事件列名格式：{@code eventId=timestamp=infokey}，若事件无关联信息则格式为{@code eventId=timestamp=}
 * 其中事件时间戳为long类型，其余部分为字符串类型。
 * 列前缀不包含在本次编码的列名中，会在后续ColumnPrefix实现中按需添加。
 */
public final class EventColumnNameConverter
    implements KeyConverter<EventColumnName> {

  /**
   * 无参构造函数，创建编解码转换器实例。
   */
  public EventColumnNameConverter() {
  }

  /**
   * 分段长度定义，用于拆分解码时确定各字段边界：
   * eventId为变长、时间戳固定占8字节(long)、infoKey为变长。
   * 变长字段遇到分隔符即结束，帮助解码时正确分割不同组件。
   */
  private static final int[] SEGMENT_SIZES = {
      Separator.VARIABLE_SIZE, Bytes.SIZEOF_LONG, Separator.VARIABLE_SIZE };

  /*
   * (non-Javadoc)
   *
   * Encodes EventColumnName into a byte array with each component/field in
   * EventColumnName separated by Separator#VALUES. This leads to an event
   * column name of the form eventId=timestamp=infokey.
   * If timestamp in passed EventColumnName object is null (eventId is not null)
   * this returns a column prefix of the form eventId= and if infokey in
   * EventColumnName is null (other 2 components are not null), this returns a
   * column name of the form eventId=timestamp=
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter
   * #encode(java.lang.Object)
   */
  @Override
  public byte[] encode(EventColumnName key) {
    // 编码事件ID，转义特殊分隔符
    byte[] first = Separator.encode(key.getId(), Separator.SPACE, Separator.TAB,
        Separator.VALUES);
    // 若时间戳为空，仅返回事件ID加分隔符前缀
    if (key.getTimestamp() == null) {
      return Separator.VALUES.join(first, Separator.EMPTY_BYTES);
    }
    // 反转时间戳实现降序排列，转换为字节数组
    byte[] second = Bytes.toBytes(
        LongConverter.invertLong(key.getTimestamp()));
    // 若信息键为空，返回事件ID+时间戳加分隔符前缀
    if (key.getInfoKey() == null) {
      return Separator.VALUES.join(first, second, Separator.EMPTY_BYTES);
    }
    // 编码信息键，拼接完整事件列名
    return Separator.VALUES.join(first, second, Separator.encode(
        key.getInfoKey(), Separator.SPACE, Separator.TAB, Separator.VALUES));
  }

  /*
   * (non-Javadoc)
   *
   * Decodes an event column name of the form eventId=timestamp= or
   * eventId=timestamp=infoKey represented in byte format and converts it into
   * an EventColumnName object.
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter
   * #decode(byte[])
   */
  @Override
  public EventColumnName decode(byte[] bytes) {
    // 按分隔符和分段长度拆分字节数组为三个组件
    byte[][] components = Separator.VALUES.split(bytes, SEGMENT_SIZES);
    // 组件数量不对，列名格式非法
    if (components.length != 3) {
      throw new IllegalArgumentException("the column name is not valid");
    }
    // 解码事件ID，还原转义的分隔符
    String id = Separator.decode(Bytes.toString(components[0]),
        Separator.VALUES, Separator.TAB, Separator.SPACE);
    // 反转时间戳还原原始值
    Long ts = LongConverter.invertLong(Bytes.toLong(components[1]));
    // 若infoKey长度为0则设为null，否则解码还原
    String infoKey = components[2].length == 0 ? null :
        Separator.decode(Bytes.toString(components[2]),
            Separator.VALUES, Separator.TAB, Separator.SPACE);
    // 构建并返回EventColumnName对象
    return new EventColumnName(id, ts, infoKey);
  }
}
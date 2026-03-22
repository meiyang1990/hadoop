// 这个文件已经全部加上中文注释
/*
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

package org.apache.hadoop.mapreduce.util;

import java.text.ParseException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.counters.AbstractCounters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.counters.CounterGroupBase;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.util.StringUtils;

/**
 * 文件：计数器字符串转换工具类，用于处理MapReduce计数器的字符串序列化与反序列化
 * 从Hadoop 0.21版本开始改用JSON格式，该类计划被弃用，主要用于兼容旧版作业历史文件
 */
@InterfaceAudience.Private
public class CountersStrings {
  // 计数器组开始标记
  private static final char GROUP_OPEN = '{';
  // 计数器组结束标记
  private static final char GROUP_CLOSE = '}';
  // 计数器开始标记
  private static final char COUNTER_OPEN = '[';
  // 计数器结束标记
  private static final char COUNTER_CLOSE = ']';
  // 单元（名称/值等）开始标记
  private static final char UNIT_OPEN = '(';
  // 单元结束标记
  private static final char UNIT_CLOSE = ')';
  // 需要转义的特殊字符数组
  private static char[] charsToEscape =  {GROUP_OPEN, GROUP_CLOSE,
                                          COUNTER_OPEN, COUNTER_CLOSE,
                                          UNIT_OPEN, UNIT_CLOSE};
  /**
   * 将单个计数器转换为0.21版本之前的紧凑转义字符串格式
   * 格式：[(实际名称)(显示名称)(值)]，用于兼容旧版作业历史文件
   * @param counter 待转换的计数器对象
   * @return 转换后的紧凑转义字符串
   */
  public static String toEscapedCompactString(Counter counter) {

    String escapedName, escapedDispName;
    long currentValue;
    // 同步锁保证计数器属性读取一致性
    synchronized(counter) {
      escapedName = escape(counter.getName());
      escapedDispName = escape(counter.getDisplayName());
      currentValue = counter.getValue();
    }
    // 预先计算字符串总长度，优化StringBuilder分配
    int length = escapedName.length() + escapedDispName.length() + 4;

    length += 8; // 预留分隔符占用长度
    StringBuilder builder = new StringBuilder(length);
    builder.append(COUNTER_OPEN);

    // 添加计数器实际名称
    builder.append(UNIT_OPEN);
    builder.append(escapedName);
    builder.append(UNIT_CLOSE);

    // 添加计数器显示名称
    builder.append(UNIT_OPEN);
    builder.append(escapedDispName);
    builder.append(UNIT_CLOSE);

    // 添加计数器当前值
    builder.append(UNIT_OPEN);
    builder.append(currentValue);
    builder.append(UNIT_CLOSE);

    builder.append(COUNTER_CLOSE);

    return builder.toString();
  }

  /**
   * 将计数器组转换为紧凑转义字符串格式
   * 格式：{(实际名称)(显示名称) [计数器紧凑字符串...]}，包含组内所有计数器
   * @param <G> 计数器组类型
   * @param group 待转换的计数器组对象
   * @return 转换后的紧凑转义字符串
   */
  public static <G extends CounterGroupBase<?>>
  String toEscapedCompactString(G group) {
    List<String> escapedStrs = Lists.newArrayList();
    int length;
    String escapedName, escapedDispName;
    synchronized(group) {
      // 预先转义名称，提前计算总长度优化缓冲区分配
      escapedName = escape(group.getName());
      escapedDispName = escape(group.getDisplayName());
      int i = 0;
      length = escapedName.length() + escapedDispName.length();
      // 遍历组内所有计数器，逐个转换并累加长度
      for (Counter counter : group) {
        String escapedStr = toEscapedCompactString(counter);
        escapedStrs.add(escapedStr);
        length += escapedStr.length();
      }
    }
    length += 6; // 预留分隔符占用长度
    StringBuilder builder = new StringBuilder(length);
    builder.append(GROUP_OPEN); // 写入组开始标记

    // 添加组实际名称
    builder.append(UNIT_OPEN);
    builder.append(escapedName);
    builder.append(UNIT_CLOSE);

    // 添加组显示名称
    builder.append(UNIT_OPEN);
    builder.append(escapedDispName);
    builder.append(UNIT_CLOSE);

    // 拼接所有计数器的字符串
    for(String escaped : escapedStrs) {
      builder.append(escaped);
    }

    builder.append(GROUP_CLOSE); // 写入组结束标记
    return builder.toString();
  }

  /**
   * 将整个计数器集合转换为紧凑转义字符串格式
   * 格式：多个计数器组字符串拼接，兼容0.21版本之前的格式
   * @param <C> 计数器类型
   * @param <G> 计数器组类型
   * @param <T> 计数器集合类型
   * @param counters 待转换的计数器集合对象
   * @return 转换后的完整紧凑转义字符串
   */
  public static <C extends Counter, G extends CounterGroupBase<C>,
                 T extends AbstractCounters<C, G>>
  String toEscapedCompactString(T counters) {
    StringBuilder builder = new StringBuilder();
    synchronized(counters) {
      // 遍历所有计数器组，逐个转换拼接
      for (G group : counters) {
        builder.append(toEscapedCompactString(group));
      }
    }
    return builder.toString();
  }

  // 对计数器分隔符特殊字符进行转义处理
  private static String escape(String string) {
    return StringUtils.escapeString(string, StringUtils.ESCAPE_CHAR,
                                    charsToEscape);
  }

  // 对计数器分隔符特殊字符进行反转义处理
  private static String unescape(String string) {
    return StringUtils.unEscapeString(string, StringUtils.ESCAPE_CHAR,
                                      charsToEscape);
  }

  /**
   * 从字符串中提取指定定界符包围的数据块，忽略转义序列
   * @param str 源字符串
   * @param open 块开始定界符
   * @param close 块结束定界符
   * @param index 输入为起始搜索位置，输出为块结束后的下一个位置
   * @return 提取出的块内容，未找到返回null，格式错误抛出异常
   * @throws ParseException 块不完整时抛出解析异常
   */
  private static String getBlock(String str, char open, char close,
                                IntWritable index) throws ParseException {
    StringBuilder split = new StringBuilder();
    int next = StringUtils.findNext(str, open, StringUtils.ESCAPE_CHAR,
                                    index.get(), split);
    split.setLength(0); // 清空缓冲区，存储块内容
    if (next >= 0) {
      ++next; // 跳过开始定界符

      next = StringUtils.findNext(str, close, StringUtils.ESCAPE_CHAR,
                                  next, split);
      if (next >= 0) {
        ++next; // 跳过结束定界符
        index.set(next);
        return split.toString(); // 返回提取的块内容
      } else {
        throw new ParseException("Unexpected end of block", next);
      }
    }
    return null; // 未找到任何块
  }

  /**
   * 将0.21版本之前的紧凑转义计数器字符串解析为计数器对象
   * 用于兼容旧版作业历史文件的解析
   * @param <C> 计数器类型
   * @param <G> 计数器组类型
   * @param <T> 计数器集合类型
   * @param compactString 待解析的紧凑转义字符串
   * @param counters 空的计数器集合对象，用于存放解析结果
   * @return 填充完成的计数器集合对象
   * @throws ParseException 字符串格式错误时抛出解析异常
   */
  @SuppressWarnings("deprecation")
  public static <C extends Counter, G extends CounterGroupBase<C>,
                 T extends AbstractCounters<C, G>>
  T parseEscapedCompactString(String compactString, T counters)
      throws ParseException {
    // 当前解析位置，使用IntWritable实现引用传递
    IntWritable index = new IntWritable(0);

    // 提取第一个计数器组块
    String groupString =
      getBlock(compactString, GROUP_OPEN, GROUP_CLOSE, index);

    // 循环解析所有计数器组
    while (groupString != null) {
      IntWritable groupIndex = new IntWritable(0);

      // 提取并反转义组实际名称，使用字符串驻留节省内存
      String groupName =
          StringInterner.weakIntern(getBlock(groupString, UNIT_OPEN, UNIT_CLOSE, groupIndex));
      groupName = StringInterner.weakIntern(unescape(groupName));

      // 提取并反转义组显示名称，使用字符串驻留节省内存
      String groupDisplayName =
          StringInterner.weakIntern(getBlock(groupString, UNIT_OPEN, UNIT_CLOSE, groupIndex));
      groupDisplayName = StringInterner.weakIntern(unescape(groupDisplayName));

      // 获取或创建对应计数器组
      G group = counters.getGroup(groupName);
      group.setDisplayName(groupDisplayName);

      // 提取组内第一个计数器块
      String counterString =
        getBlock(groupString, COUNTER_OPEN, COUNTER_CLOSE, groupIndex);

      // 循环解析组内所有计数器
      while (counterString != null) {
        IntWritable counterIndex = new IntWritable(0);

        // 提取并反转义计数器实际名称，字符串驻留
        String counterName =
            StringInterner.weakIntern(getBlock(counterString, UNIT_OPEN, UNIT_CLOSE, counterIndex));
        counterName = StringInterner.weakIntern(unescape(counterName));

        // 提取并反转义计数器显示名称，字符串驻留
        String counterDisplayName =
            StringInterner.weakIntern(getBlock(counterString, UNIT_OPEN, UNIT_CLOSE, counterIndex));
        counterDisplayName = StringInterner.weakIntern(unescape(counterDisplayName));

        // 提取计数器值并转换为长整型
        long value =
          Long.parseLong(getBlock(counterString, UNIT_OPEN, UNIT_CLOSE,
                                  counterIndex));

        // 获取或创建计数器，设置属性并累加值
        Counter counter = group.findCounter(counterName);
        counter.setDisplayName(counterDisplayName);
        counter.increment(value);

        // 提取下一个计数器块
        counterString =
          getBlock(groupString, COUNTER_OPEN, COUNTER_CLOSE, groupIndex);
      }

      // 提取下一个计数器组块
      groupString = getBlock(compactString, GROUP_OPEN, GROUP_CLOSE, index);
    }
    return counters;
  }
}
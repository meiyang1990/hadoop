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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * 时间线服务HBase存储的分隔符枚举，用于分隔行键、列限定符和复合字段，提供编解码功能避免分隔符冲突。
 */
public enum Separator {

  /**
   * 用于限定符（行键/列限定符）内部的分隔符。
   */
  QUALIFIERS("!", "%0$"),

  /**
   * 用于值内部、复合键/列限定符字段的分隔符。
   */
  VALUES("=", "%1$"),

  /**
   * 空格分隔符，用于转义字符串中原本存在的空格。
   */
  SPACE(" ", "%2$"),

  /**
   * 制表符分隔符，用于转义字符串中原本存在的制表符。
   */
  TAB("\t", "%3$");

  // 保留字符%，用于前缀编码，提前转义数据中自然出现的编码字符串
  // 虽然可以做成枚举实例，但这里作为私有变量对调用者隐藏
  private static final String PERCENT = "%";
  private static final String PERCENT_ENCODED = "%9$";

  private static final Pattern PERCENT_PATTERN =
      Pattern.compile(PERCENT, Pattern.LITERAL);
  private static final String PERCENT_REPLACEMENT =
      Matcher.quoteReplacement(PERCENT);

  private static final Pattern PERCENT_ENCODED_PATTERN =
      Pattern.compile(PERCENT_ENCODED, Pattern.LITERAL);
  private static final String PERCENT_ENCODED_REPLACEMENT =
      Matcher.quoteReplacement(PERCENT_ENCODED);

  /**
   * 分隔符的字符串值。
   */
  private final String value;

  /**
   * 分隔符的字节数组表示。
   */
  private final byte[] bytes;

  // 预编译正则表达式和转义后的替换字符串，用于性能优化
  private final Pattern valuePattern;
  private final String valueReplacement;

  private final Pattern encodedValuePattern;
  private final String encodedValueReplacement;

  /**
   * 表示分段大小可变，分段在遇到分隔符时结束，常用于字符串。
   * 也用于表示不限制返回分段数量，此时返回所有可能的拆分结果。
   */
  public static final int VARIABLE_SIZE = 0;


  /** 空字符串常量。 */
  public static final String EMPTY_STRING = "";

  /** 空字节数组常量。 */
  public static final byte[] EMPTY_BYTES = new byte[0];

  /**
   * 构造分隔符枚举实例。
   * @param value 分隔符字符串，不能为null或空字符串
   * @param encodedValue 分隔符的编码值，选择数据中不太可能出现的字符串，不能为null或空字符串
   */
  private Separator(String value, String encodedValue) {
    this.value = value;

    // 参数校验
    if (value == null || value.length() == 0 || encodedValue == null
        || encodedValue.length() == 0) {
      throw new IllegalArgumentException(
          "Cannot create separator from null or empty string.");
    }

    this.bytes = Bytes.toBytes(value);
    this.valuePattern = Pattern.compile(value, Pattern.LITERAL);
    this.valueReplacement = Matcher.quoteReplacement(value);

    this.encodedValuePattern = Pattern.compile(encodedValue, Pattern.LITERAL);
    this.encodedValueReplacement = Matcher.quoteReplacement(encodedValue);
  }

  /**
   * 获取分隔符原始值。
   * @return 分隔符原始字符串
   */
  public String getValue() {
    return value;
  }

  /**
   * 对令牌进行编码，使令牌可以安全使用当前分隔符，不会产生分割冲突。
   * 必须配合{@link #decode(String)}使用才能正确解码。
   * 如果需要对多个分隔符进行编码，推荐使用{@link #encode(String, Separator...)}
   * 比多次调用本方法得到的编码结果更紧凑。
   *
   * @param token 待编码的令牌
   * @return 编码后的令牌，所有当前分隔符出现位置都被转义
   */
  public String encode(String token) {
    if (token == null || token.length() == 0) {
      // 无需替换，直接返回
      return token;
    }
    // 先对%符号进行编码，转义数据中自然存在的编码值
    String escaped = encodePercent(token);
    return encodeSingle(escaped, this);
  }

  /**
   * 通用替换方法，使用正则替换所有匹配项。
   * @param token 输入字符串
   * @param pattern 匹配模式
   * @param replacement 替换字符串
   * @return 替换完成后的字符串
   */
  private static String replace(String token, Pattern pattern,
      String replacement) {
    return pattern.matcher(token).replaceAll(replacement);
  }

  /**
   * 对单个分隔符进行编码替换。
   * @param token 输入字符串
   * @param separator 待编码的分隔符
   * @return 替换完成后的字符串
   */
  private static String encodeSingle(String token, Separator separator) {
    return replace(token, separator.valuePattern,
        separator.encodedValueReplacement);
  }

  /**
   * 对%符号进行编码转义。
   * @param token 输入字符串
   * @return 编码完成后的字符串
   */
  private static String encodePercent(String token) {
    return replace(token, PERCENT_PATTERN, PERCENT_ENCODED_REPLACEMENT);
  }

  /**
   * 对{@link #encode(String)}编码的令牌进行解码。
   * 必须配合编码方法使用才能恢复原始字符串。
   *
   * @param token 待解码的令牌
   * @return 解码后的原始字符串，所有编码过的分隔符被还原
   */
  public String decode(String token) {
    if (token == null || token.length() == 0) {
      // 无需替换，直接返回
      return token;
    }
    String escaped = decodeSingle(token, this);
    // 对%符号进行解码，取消转义
    return decodePercent(escaped);
  }

  /**
   * 对单个分隔符进行解码替换。
   * @param token 输入字符串
   * @param separator 待解码的分隔符
   * @return 替换完成后的字符串
   */
  private static String decodeSingle(String token, Separator separator) {
    return replace(token, separator.encodedValuePattern,
        separator.valueReplacement);
  }

  /**
   * 对%符号进行解码，取消转义。
   * @param token 输入字符串
   * @return 解码完成后的字符串
   */
  private static String decodePercent(String token) {
    return replace(token, PERCENT_ENCODED_PATTERN, PERCENT_REPLACEMENT);
  }

  /**
   * 对字符串中多个分隔符依次进行编码。
   * 必须配合{@link #decode(byte[], Separator...)}或{@link #decode(String, Separator...)}
   * 使用相同分隔符列表才能正确解码。
   * 如果需要对多个分隔符编码，本方法比多次调用单分隔符编码得到的结果更紧凑。
   *
   * @param token 包含待编码分隔符的输入字符串
   * @param separators 需要编码的分隔符列表
   * @return 编码完成后的字节数组，不会返回null
   */
  public static byte[] encode(String token, Separator... separators) {
    if (token == null || token.length() == 0) {
      return EMPTY_BYTES;
    }
    String result = token;
    // 先对%符号进行编码，转义数据中自然存在的编码值
    result = encodePercent(token);
    for (Separator separator : separators) {
      if (separator != null) {
        result = encodeSingle(result, separator);
      }
    }
    return Bytes.toBytes(result);
  }

  /**
   * 对字节数组中多个分隔符依次进行解码。
   * 必须配合{@link #encode(String, Separator...)}使用相同分隔符列表才能恢复原始字符串。
   *
   * @param token 包含待解码分隔符的输入字节数组
   * @param separators 需要解码的分隔符列表
   * @return 解码完成后的原始字符串
   */
  public static String decode(byte[] token, Separator... separators) {
    if (token == null) {
      return null;
    }
    return decode(Bytes.toString(token), separators);
  }

  /**
   * 对字符串中多个分隔符依次进行解码。
   * 必须配合{@link #encode(String, Separator...)}使用相同分隔符列表才能恢复原始字符串。
   *
   * @param token 包含待解码分隔符的输入字符串
   * @param separators 需要解码的分隔符列表
   * @return 解码完成后的原始字符串
   */
  public static String decode(String token, Separator... separators) {
    if (token == null) {
      return null;
    }
    String result = token;
    for (Separator separator : separators) {
      if (separator != null) {
        result = decodeSingle(result, separator);
      }
    }
    // 对%符号进行解码，取消转义
    return decodePercent(result);
  }

  /**
   * 将多个字节数组组件使用当前分隔符连接成一个字节数组。
   *
   * @param components 待连接的字节数组组件
   * @return 连接完成后的字节数组
   */
  public byte[] join(byte[]... components) {
    if (components == null || components.length == 0) {
      return EMPTY_BYTES;
    }

    int finalSize = 0;
    finalSize = this.value.length() * (components.length - 1);
    for (byte[] comp : components) {
      if (comp != null) {
        finalSize += comp.length;
      }
    }

    byte[] buf = new byte[finalSize];
    int offset = 0;
    for (int i = 0; i < components.length; i++) {
      if (components[i] != null) {
        System.arraycopy(components[i], 0, buf, offset, components[i].length);
        offset += components[i].length;
      }
      if (i < (components.length - 1)) {
        System.arraycopy(this.bytes, 0, buf, offset, this.value.length());
        offset += this.value.length();
      }
    }
    return buf;
  }

  /**
   * 使用当前分隔符连接多个字符串，连接前会对每个项中的分隔符进行编码，保证可逆向拆分。
   *
   * @param items 待连接的字符串数组，每个项中的分隔符会被预先编码，不能为null
   * @return 连接完成后的结果，不会返回null
   */
  public String joinEncoded(String... items) {
    if (items == null || items.length == 0) {
      return "";
    }

    StringBuilder sb = new StringBuilder(encode(items[0].toString()));
    // 从索引1开始，已经处理了第一个项
    for (int i = 1; i < items.length; i++) {
      sb.append(this.value)
          .append(encode(items[i].toString()));
    }

    return sb.toString();
  }

  /**
   * 使用当前分隔符连接多个对象，连接前会对每个项中的分隔符进行编码，保证可逆向拆分。
   *
   * @param items 待连接的可迭代对象，每个项会调用toString()，分隔符会被预先编码，不能为null
   * @return 连接完成后的结果，不会返回null
   */
  public String joinEncoded(Iterable<?> items) {
    if (items == null) {
      return "";
    }
    Iterator<?> i = items.iterator();
    if (!i.hasNext()) {
      return "";
    }

    StringBuilder sb = new StringBuilder(encode(i.next().toString()));
    while (i.hasNext()) {
      sb.append(this.value);
      sb.append(encode(i.next().toString()));
    }

    return sb.toString();
  }

  /**
   * 拆分已编码的复合值，每个分段会自动解码还原原始内容。
   *
   * @param compoundValue 复合值，使用当前分隔符分隔，每个分段已经编码
   * @return 解码拆分后的字符串集合，不会返回null
   */
  public Collection<String> splitEncoded(String compoundValue) {
    List<String> result = new ArrayList<String>();
    if (compoundValue != null) {
      for (String val : valuePattern.split(compoundValue)) {
        result.add(decode(val));
      }
    }
    return result;
  }

  /**
   * 使用当前分隔符拆分字节数组，最多返回limit个分段。每个分段是新分配的字节数组。
   *
   * @param source 待拆分的源字节数组
   * @param limit 最大返回分段数，非正表示不限制数量，返回所有分段
   * @return 拆分后的二维字节数组
   */
  public byte[][] split(byte[] source, int limit) {
    return split(source, this.bytes, limit);
  }

  /**
   * 根据指定的分段大小使用当前分隔符拆分字节数组。
   * 如果固定大小分段中提前出现分隔符，分隔符会被算作分段一部分，直到达到指定大小。
   * 大小为{@value #VARIABLE_SIZE}表示可变长度，遇到分隔符才结束，此类分段会预先编码分隔符，拆分后需要自行解码。
   *
   * @param source 待拆分的源字节数组
   * @param sizes 各个分段的预期大小数组
   * @return 按指定大小拆分后的二维字节数组
   */
  public byte[][] split(byte[] source, int[] sizes) {
    return split(source, this.bytes, sizes);
  }

  /**
   * 使用当前分隔符拆分字节数组，返回所有分段。每个分段是新分配的字节数组。
   *
   * @param source 待拆分的源字节数组
   * @return 拆分后的二维字节数组
   */
  public byte[][] split(byte[] source) {
    return split(source, this.bytes);
  }

  /**
   * 计算拆分后各个分段在源字节数组中的范围区间[start, end)，遵循左闭右开原则。
   * 如果固定大小分段中提前出现分隔符，分隔符会被算作分段一部分，直到达到指定大小。
   * 大小为{@value #VARIABLE_SIZE}表示可变长度，遇到分隔符才结束，此类分段会预先编码分隔符，拆分后需要自行解码。
   *
   * @param source 源字节数组
   * @param separator 分隔符字节数组
   * @param sizes 各个分段的预期大小数组
   * @return 分段范围列表
   */
  private static List<Range> splitRanges(byte[] source, byte[] separator,
      int[] sizes) {
    List<Range> segments = new ArrayList<Range>();
    if (source == null || separator == null) {
      return segments;
    }
    // VARIABLE_SIZE表示不限制返回分段数量
    int limit = VARIABLE_SIZE;
    if (sizes != null && sizes.length > 0) {
      limit = sizes.length;
    }
    int start = 0;
    int currentSegment = 0;
    itersource: for (int i = 0; i < source.length; i++) {
      // 匹配整个分隔符字节序列
      for (int j = 0; j < separator.length; j++) {
        if (source[i + j] != separator[j]) {
          continue itersource;
        }
      }
      // 所有分隔符字节匹配成功
      if (limit > VARIABLE_SIZE) {
        if (segments.size() >= (limit - 1)) {
          // 剩余全部内容放入最后一个分段
          break;
        }
        if (sizes != null) {
          int currentSegExpectedSize = sizes[currentSegment];
          if (currentSegExpectedSize > VARIABLE_SIZE) {
            int currentSegSize = i - start;
            if (currentSegSize < currentSegExpectedSize) {
              // 当前分段还未达到预期大小，继续往后找
              continue itersource;
            } else if (currentSegSize > currentSegExpectedSize) {
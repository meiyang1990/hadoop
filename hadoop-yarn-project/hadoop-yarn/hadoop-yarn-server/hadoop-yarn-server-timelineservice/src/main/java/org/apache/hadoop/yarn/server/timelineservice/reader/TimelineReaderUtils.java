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

package org.apache.hadoop.yarn.server.timelineservice.reader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 时间线读取器通用工具类，提供字符串分割、转义和拼接等通用工具方法，供时间线读取模块各处使用
 */
public final class TimelineReaderUtils {
  private TimelineReaderUtils() {
  }

  /**
   * 字符串拼接默认分隔符
   */
  @VisibleForTesting
  public static final char DEFAULT_DELIMITER_CHAR = '!';

  /**
   * 字符串转义默认转义字符
   */
  @VisibleForTesting
  public static final char DEFAULT_ESCAPE_CHAR = '*';

  /**
   * 分页起始ID查询参数键名
   */
  public static final String FROMID_KEY = "FROM_ID";

  @VisibleForTesting
  public static final String UID_KEY = "UID";

  /**
   * 处理转义字符，按指定分隔符分割字符串。分隔符和转义字符本身若要作为普通字符使用，需要在前面添加转义字符转义。
   * @param str 待分割字符串
   * @param delimiterChar 分隔符
   * @param escapeChar 转义字符，用于转义分隔符和转义字符本身
   * @return 分割后的字符串列表
   * @throws IllegalArgumentException 如果字符串转义格式不正确，则抛出异常
   */
  static List<String> split(final String str, final char delimiterChar,
      final char escapeChar) throws IllegalArgumentException {
    if (str == null) {
      return null;
    }
    int len = str.length();
    if (len == 0) {
      return Collections.emptyList();
    }
    List<String> list = new ArrayList<String>();
    // 当前遍历偏移量
    int offset = 0;
    // 当前分段起始偏移量，遇到转义符或分隔符后重置
    int startOffset = 0;
    StringBuilder builder = new StringBuilder(len);
    // 遍历整个字符串
    while (offset < len) {
      if (str.charAt(offset) == escapeChar) {
        // 转义符不能是字符串最后一个字符，必须跟待转义字符
        if (offset + 1 >= len) {
          throw new IllegalArgumentException(
              "Escape char not properly escaped.");
        }
        char nextChar = str.charAt(offset + 1);
        // 只有转义符和分隔符允许被转义，其他位置不允许出现单独转义符
        if (nextChar != escapeChar && nextChar != delimiterChar) {
          throw new IllegalArgumentException(
              "Escape char or delimiter char not properly escaped.");
        }
        // 复制上一次分段起始到当前转义符之间的字符
        if (startOffset < offset) {
          builder.append(str.substring(startOffset, offset));
        }
        // 添加被转义的原字符
        builder.append(nextChar);
        offset += 2;
        // 重置分段起始偏移量
        startOffset = offset;
        continue;
      } else if (str.charAt(offset) == delimiterChar) {
        // 遇到未转义的分隔符，在此处分割
        builder.append(str.substring(startOffset, offset));
        // 分割结果添加当前分段，去除首尾空格
        list.add(builder.toString().trim());
        // 重置偏移量和缓冲，准备下一分段
        startOffset = ++offset;
        builder = new StringBuilder(len - offset);
        continue;
      }
      offset++;
    }
    // 拼接最后一个分段剩余字符
    if (!str.isEmpty()) {
      builder.append(str.substring(startOffset));
    }
    // 添加最后一个分段到结果
    list.add(builder.toString().trim());
    return list;
  }

  /**
   * 对字符串中存在的分隔符和转义字符进行转义处理
   * @param str 待转义原始字符串
   * @param delimiterChar 需要转义的分隔符
   * @param escapeChar 用于转义的转义字符
   * @return 转义完成后的字符串
   */
  private static String escapeString(final String str, final char delimiterChar,
      final char escapeChar) {
    if (str == null) {
      return null;
    }
    int len = str.length();
    if (len == 0) {
      return "";
    }
    StringBuilder builder = new StringBuilder();
    // 当前遍历偏移量
    int offset = 0;
    // 当前分段起始偏移量
    int startOffset = 0;
    // 遍历整个字符串
    while (offset < len) {
      char charAtOffset = str.charAt(offset);
      if (charAtOffset == escapeChar || charAtOffset == delimiterChar) {
        // 遇到需要转义的字符，复制上一次分段起始到当前位置之间的字符
        if (startOffset < offset) {
          builder.append(str.substring(startOffset, offset));
        }
        // 先添加转义符，再添加原字符，完成转义
        builder.append(escapeChar).append(charAtOffset);
        // 重置分段起始偏移量
        startOffset = offset + 1;
      }
      offset++;
    }
    // 拼接剩余字符
    builder.append(str.substring(startOffset));
    return builder.toString();
  }

  /**
   * 先对每个字符串做转义处理，再使用指定分隔符拼接为一个字符串
   * @param strs 待拼接的字符串数组
   * @param delimiterChar 拼接分隔符
   * @param escapeChar 转义字符
   * @return 转义拼接完成后的字符串
   */
  static String joinAndEscapeStrings(final String[] strs,
      final char delimiterChar, final char escapeChar) {
    int len = strs.length;
    // 逐个对字符串进行转义处理
    for (int index = 0; index < len; index++) {
      if (strs[index] == null) {
        return null;
      }
      strs[index] = escapeString(strs[index], delimiterChar, escapeChar);
    }
    // 使用分隔符拼接转义后的字符串
    return StringUtils.join(strs, delimiterChar);
  }

  /**
   * 使用默认分隔符和转义符分割字符串
   * @param str 待分割字符串
   * @return 分割后的字符串列表
   * @throws IllegalArgumentException 如果字符串转义格式不正确，则抛出异常
   */
  public static List<String> split(final String str)
      throws IllegalArgumentException {
    return split(str, DEFAULT_DELIMITER_CHAR, DEFAULT_ESCAPE_CHAR);
  }

  /**
   * 使用默认分隔符和转义符转义拼接字符串数组
   * @param strs 待拼接字符串数组
   * @return 转义拼接完成后的字符串
   */
  public static String joinAndEscapeStrings(final String[] strs) {
    return joinAndEscapeStrings(strs, DEFAULT_DELIMITER_CHAR,
        DEFAULT_ESCAPE_CHAR);
  }
}
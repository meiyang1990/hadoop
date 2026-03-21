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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

/**
 * 公平调度器工具类，提供公平调度器通用工具方法
 */
public final class FairSchedulerUtilities {

  /**
   * 空白字符表，从Google Guava v19复制而来，包含所有需要识别的空白字符
   * <p>
   * Licensed under the Apache License Version 2.0.
   */
  static final String WHITESPACE_TABLE =
      "\u2002\u3000\r\u0085\u200A\u2005\u2000\u3000"
          + "\u2029\u000B\u3000\u2008\u2003\u205F\u3000\u1680"
          + "\u0009\u0020\u2006\u2001\u202F\u00A0\u000C\u2009"
          + "\u3000\u2004\u3000\u3000\u2028\n\u2007\u3000";

  /** 工具类禁止实例化 */
  private FairSchedulerUtilities() {
    // private constructor because this is a utility class.
  }

  /**
   * 判断字符是否为空白字符
   * @param c 待判断字符
   * @return 是空白字符返回true，否则返回false
   */
  private static boolean isWhitespace(char c) {
    for (int i = 0; i < WHITESPACE_TABLE.length(); i++) {
      if (WHITESPACE_TABLE.charAt(i) == c) {
        return true;
      }
    }
    return false;
  }

  /**
   * 裁剪队列名称首尾的空白字符，处理队列名称输入
   * @param name 原始队列名称
   * @return 裁剪首尾空白后的队列名称，输入为null则返回null
   */
  public static String trimQueueName(String name) {
    if (name == null) {
      return null;
    }
    // 找到第一个非空白字符起始位置
    int start = 0;
    while (start < name.length()
        && isWhitespace(name.charAt(start))
        && start < name.length()) {
      start++;
    }
    // 找到最后一个非空白字符结束位置
    int end = name.length() - 1;
    while (end >= 0
        && isWhitespace(name.charAt(end))
        && end > start) {
      end--;
    }
    // 截取裁剪后的子串
    return name.substring(start, end+1);
  }

}
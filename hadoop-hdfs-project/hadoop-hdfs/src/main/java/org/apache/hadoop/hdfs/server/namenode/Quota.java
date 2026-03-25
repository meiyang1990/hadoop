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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.hdfs.util.EnumCounters;

/**
 * HDFS配额类型枚举，定义HDFS中目录配额的两种计数类型，用于限制目录下的资源使用量。
 */
public enum Quota {
  /** 命名空间配额，统计目录下的名称对象（文件/目录）数量 */
  NAMESPACE,
  /** 存储空间配额，统计目录下包含副本因子在内的总字节使用量 */
  STORAGESPACE;

  /**
   * 配额使用量计数器，基于EnumCounters实现对两种配额类型的计数存储。
   */
  public static class Counts extends EnumCounters<Quota> {
    /**
     * 根据指定的命名空间和存储空间使用量创建新的配额计数对象。
     * @param namespace 命名空间初始使用量
     * @param storagespace 存储空间初始使用量
     * @return 初始化完成的配额计数对象
     */
    public static Counts newInstance(long namespace, long storagespace) {
      final Counts c = new Counts();
      c.set(NAMESPACE, namespace);
      c.set(STORAGESPACE, storagespace);
      return c;
    }

    /**
     * 创建初始值为0的空配额计数对象。
     * @return 零值初始化的配额计数对象
     */
    public static Counts newInstance() {
      return newInstance(0, 0);
    }

    Counts() {
      super(Quota.class);
    }
  }

  /**
   * 检查当前使用量是否超出配额限制。
   * 规则：仅当配额已设置（quota >= 0）且实际使用量大于配额时判定为违反配额。
   * @param quota 配额限制值
   * @param usage 当前实际使用量
   * @return true表示已违反配额限制，false表示未违反
   */
  public static boolean isViolated(final long quota, final long usage) {
    return quota >= 0 && usage > quota;
  }

  /**
   * 检查增加增量后是否会超出配额限制。
   * 规则：仅当配额已设置、增量为正且当前使用量加上增量大于配额时判定为违反配额。
   * @param quota 配额限制值
   * @param usage 当前实际使用量
   * @param delta 即将增加的使用量增量
   * @return true表示增加后会违反配额限制，false表示不会违反
   */
  static boolean isViolated(final long quota, final long usage,
      final long delta) {
    return quota >= 0 && delta > 0 && usage > quota - delta;
  }
}
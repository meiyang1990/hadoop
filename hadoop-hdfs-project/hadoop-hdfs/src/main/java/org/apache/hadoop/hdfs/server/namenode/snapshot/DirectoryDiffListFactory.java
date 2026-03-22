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
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature.DirectoryDiff;
import org.slf4j.Logger;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.IntFunction;

/**
 * 文件级注释：目录差异列表工厂，用于为DirectoryDiff创建合适的差异列表存储实现
 * 支持基于ArrayList的普通实现和基于跳表的高效实现，可通过配置切换
 */
/** For creating {@link DiffList} for {@link DirectoryDiff}. */
public abstract class DirectoryDiffListFactory {
  /**
   * 创建指定初始容量的DirectoryDiff差异列表实例
   * @param capacity 初始容量
   * @return 差异列表实例，具体实现由配置决定
   */
  public static DiffList<DirectoryDiff> createDiffList(int capacity) {
    return constructor.apply(capacity);
  }

  /**
   * 初始化工厂，根据配置选择差异列表的实现方式
   * @param interval 跳表的间隔概率参数
   * @param maxSkipLevels 跳表的最大层级
   * @param log 日志记录器
   */
  public static void init(int interval, int maxSkipLevels, Logger log) {
    DirectoryDiffListFactory.skipInterval = interval;
    DirectoryDiffListFactory.maxLevels = maxSkipLevels;

    if (maxLevels > 0) {
      // 启用跳表实现，提供更高的查询效率
      constructor = c -> new DiffListBySkipList(c);
      log.info("SkipList is enabled with skipInterval=" + skipInterval
          + ", maxLevels=" + maxLevels);
    } else {
      // 禁用跳表，使用普通ArrayList实现
      constructor = c -> new DiffListByArrayList<>(c);
      log.info("SkipList is disabled");
    }
  }

  // 差异列表构造函数，根据配置指向不同实现
  private static volatile IntFunction<DiffList<DirectoryDiff>> constructor
      = c -> new DiffListByArrayList<>(c);

  private static volatile int skipInterval;
  private static volatile int maxLevels;

  /**
   * 为跳表节点随机生成层级，按照跳表概率规则生成
   * @return 生成的节点层级，范围在0到maxLevels之间
   */
  public static int randomLevel() {
    final Random r = ThreadLocalRandom.current();
    for (int level = 0; level < maxLevels; level++) {
      // 以1/skipInterval的概率进入下一层级
      if (r.nextInt(skipInterval) > 0) {
        return level;
      }
    }
    // 达到最大层级限制，返回最大层级
    return maxLevels;
  }


  private DirectoryDiffListFactory() {}
}
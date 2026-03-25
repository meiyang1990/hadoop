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

package org.apache.hadoop.yarn.server.sharedcache;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * YARN共享缓存工具类，提供处理共享缓存目录结构的辅助方法
 */
@Private
@Unstable
public class SharedCacheUtil {

  private static final Logger LOG =
      LoggerFactory.getLogger(SharedCacheUtil.class);

  /**
   * 从配置中获取共享缓存目录层级深度，处理无效配置
   * @param conf YARN配置对象
   * @return 有效的共享缓存目录层级深度
   */
  @Private
  public static int getCacheDepth(Configuration conf) {
    int cacheDepth =
        conf.getInt(YarnConfiguration.SHARED_CACHE_NESTED_LEVEL,
            YarnConfiguration.DEFAULT_SHARED_CACHE_NESTED_LEVEL);

    // 配置值非法时使用默认值并打印警告日志
    if (cacheDepth <= 0) {
      LOG.warn("Specified cache depth was less than or equal to zero."
          + " Using default value instead. Default: {}, Specified: {}",
          YarnConfiguration.DEFAULT_SHARED_CACHE_NESTED_LEVEL, cacheDepth);
      cacheDepth = YarnConfiguration.DEFAULT_SHARED_CACHE_NESTED_LEVEL;
    }

    return cacheDepth;
  }

  /**
   * 根据校验和生成共享缓存条目路径，基于校验和前缀分层存储
   * @param cacheDepth 目录层级深度
   * @param cacheRoot 共享缓存根目录
   * @param checksum 资源校验和
   * @return 完整的缓存条目路径字符串
   */
  @Private
  public static String getCacheEntryPath(int cacheDepth, String cacheRoot,
      String checksum) {

    // 参数校验：缓存深度必须大于0
    if (cacheDepth <= 0) {
      throw new IllegalArgumentException(
          "The cache depth must be greater than 0. Passed value: " + cacheDepth);
    }
    // 参数校验：校验和长度必须不小于层级深度
    if (checksum.length() < cacheDepth) {
      throw new IllegalArgumentException("The checksum passed was too short: "
          + checksum);
    }

    // 根据校验和前缀构建分层路径，例如深度3、校验和3c4f，路径为：根目录/3/c/4/3c4f
    StringBuilder sb = new StringBuilder(cacheRoot);
    // 按层级依次添加校验和每一位作为子目录
    for (int i = 0; i < cacheDepth; i++) {
      sb.append(Path.SEPARATOR_CHAR)
          .append(checksum.charAt(i));
    }
    // 最后添加完整校验和作为资源目录
    sb.append(Path.SEPARATOR_CHAR).append(checksum);

    return sb.toString();
  }

  /**
   * 生成共享缓存条目的glob匹配模式，用于扫描所有缓存条目
   * @param depth 目录层级深度
   * @return glob匹配模式字符串
   */
  @Private
  public static String getCacheEntryGlobPattern(int depth) {
    StringBuilder pattern = new StringBuilder();
    // 每个层级对应一个通配符
    for (int i = 0; i < depth; i++) {
      pattern.append("*/");
    }
    // 最后匹配资源目录本身
    pattern.append("*");
    return pattern.toString();
  }
}
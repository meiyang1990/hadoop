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

package org.apache.hadoop.mapreduce.v2.util;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件级注释：MapReduce分布式缓存工件转换器，用于将分布式缓存配置解析转换为YARN本地资源映射表
 * 为MR应用处理分布式缓存，解析缓存工件并生成YARN可识别的LocalResource集合
 */
/**
 * 辅助工具类，为MapReduce应用解析分布式缓存工件，构建YARN LocalResource资源映射表
 * 负责将旧版MapReduce分布式缓存配置转换为YARN容器可识别的本地资源描述
 */
@SuppressWarnings("deprecation")
@Private
@Unstable
class LocalResourceBuilder {
  public static final Logger LOG =
      LoggerFactory.getLogger(LocalResourceBuilder.class);

  private Configuration conf;
  private LocalResourceType type;
  private URI[] uris;
  private long[] timestamps;
  private long[] sizes;
  private boolean[] visibilities;
  private Map<String, Boolean> sharedCacheUploadPolicies;

  /**
   * 构造空的LocalResourceBuilder实例，通过setter方法注入配置
   */
  LocalResourceBuilder() {
  }

  /**
   * 设置Hadoop配置对象，用于文件系统操作
   * @param c Hadoop配置对象
   */
  void setConf(Configuration c) {
    this.conf = c;
  }

  /**
   * 设置本地资源类型（归档/文件/模式）
   * @param t 本地资源类型枚举
   */
  void setType(LocalResourceType t) {
    this.type = t;
  }

  /**
   * 设置缓存资源的URI数组
   * @param u 缓存资源URI数组
   */
  void setUris(URI[] u) {
    this.uris = u;
  }

  /**
   * 设置缓存资源的时间戳数组
   * @param t 对应每个缓存资源的修改时间戳
   */
  void setTimestamps(long[] t) {
    this.timestamps = t;
  }

  /**
   * 设置缓存资源的大小数组
   * @param s 对应每个缓存资源的文件大小
   */
  void setSizes(long[] s) {
    this.sizes = s;
  }

  /**
   * 设置缓存资源可见性数组，true表示PUBLIC，false表示PRIVATE
   * @param v 可见性布尔数组
   */
  void setVisibilities(boolean[] v) {
    this.visibilities = v;
  }

  /**
   * 设置共享缓存上传策略映射表
   * @param policies 资源URI到上传策略的映射，true表示允许上传到共享缓存
   */
  void setSharedCacheUploadPolicies(Map<String, Boolean> policies) {
    this.sharedCacheUploadPolicies = policies;
  }

  /**
   * 根据配置的缓存资源信息，创建YARN LocalResource并添加到结果映射表
   * @param localResources 输出参数，存储解析完成的资源名称到LocalResource的映射
   * @throws IOException 文件系统操作异常
   * @throws IllegalArgumentException 参数校验不合法时抛出
   */
  void createLocalResources(Map<String, LocalResource> localResources)
      throws IOException {

    if (uris != null) {
      // 校验输入数组长度一致性
      if ((uris.length != timestamps.length) || (uris.length != sizes.length) ||
          (uris.length != visibilities.length)) {
        throw new IllegalArgumentException("Invalid specification for " +
            "distributed-cache artifacts of type " + type + " :" +
            " #uris=" + uris.length +
            " #timestamps=" + timestamps.length +
            " #visibilities=" + visibilities.length
            );
      }

      // 遍历所有缓存资源URI逐个处理
      for (int i = 0; i < uris.length; ++i) {
        URI u = uris[i];
        Path p = new Path(u);
        // 获取对应URI的文件系统实例
        FileSystem remoteFS = p.getFileSystem(conf);
        String linkName = null;

        // 处理通配符路径，将通配符位置替换为父目录
        if (p.getName().equals(DistributedCache.WILDCARD)) {
          p = p.getParent();
          linkName = p.getName() + Path.SEPARATOR + DistributedCache.WILDCARD;
        }

        // 路径规范化，获取文件系统的绝对路径
        p = remoteFS.resolvePath(p.makeQualified(remoteFS.getUri(),
            remoteFS.getWorkingDirectory()));

        // 没有通配符时，尝试从URI fragment获取链接名称
        if (linkName == null) {
          linkName = u.getFragment();

          // 对fragment内容进行合法性校验
          if (linkName != null) {
            Path linkPath = new Path(linkName);

            // 链接名称不能是绝对路径
            if (linkPath.isAbsolute()) {
              throw new IllegalArgumentException("Resource name must be "
                  + "relative");
            }

            // 规范化链接名称
            linkName = linkPath.toUri().getPath();
          }
        } else if (u.getFragment() != null) {
          // 通配符和fragment不能同时存在
          throw new IllegalArgumentException("Invalid path URI: " + p +
              " - cannot contain both a URI fragment and a wildcard");
        }

        // 既没有通配符也没有fragment时，默认使用文件名作为链接名称
        if (linkName == null) {
          linkName = p.getName();
        }

        // 检查是否存在同名资源冲突，冲突则打印警告跳过新资源
        LocalResource orig = localResources.get(linkName);
        if(orig != null && !orig.getResource().equals(URL.fromURI(p.toUri()))) {
          LOG.warn(getResourceDescription(orig.getType()) + orig.getResource()
              + " conflicts with " + getResourceDescription(type) + u);
          continue;
        }
        // 获取当前资源的共享缓存上传策略，默认不上传
        Boolean sharedCachePolicy = sharedCacheUploadPolicies.get(u.toString());
        sharedCachePolicy =
            sharedCachePolicy == null ? Boolean.FALSE : sharedCachePolicy;
        // 创建LocalResource实例并添加到结果映射
        localResources.put(linkName, LocalResource.newInstance(URL.fromURI(p
            .toUri()), type, visibilities[i] ? LocalResourceVisibility.PUBLIC
                : LocalResourceVisibility.PRIVATE,
            sizes[i], timestamps[i], sharedCachePolicy));
      }
    }
  }

  /**
   * 根据资源类型生成资源描述信息，用于日志输出
   * @param type 本地资源类型
   * @return 对应资源类型的描述字符串，包含配置项名称
   */
  private static String getResourceDescription(LocalResourceType type) {
    if (type == LocalResourceType.ARCHIVE
        || type == LocalResourceType.PATTERN) {
      return "cache archive (" + MRJobConfig.CACHE_ARCHIVES + ") ";
    }
    return "cache file (" + MRJobConfig.CACHE_FILES + ") ";
  }
}
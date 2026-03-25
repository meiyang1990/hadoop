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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer;

import java.net.URISyntaxException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;

/**
 * 本地化资源请求封装，用于NodeManager本地化缓存的匹配检索
 * 继承LocalResource并实现Comparable接口，支持在有序集合中存储和比较
 */
public class LocalResourceRequest
    extends LocalResource implements Comparable<LocalResourceRequest> {

  private final Path loc;
  private final long timestamp;
  private final LocalResourceType type;
  private final LocalResourceVisibility visibility;
  private final String pattern;

  /**
   * 根据容器请求的LocalResource构造本地化资源请求
   * @param resource 容器请求的资源
   * @throws URISyntaxException 如果路径格式错误抛出异常
   */
  public LocalResourceRequest(LocalResource resource)
      throws URISyntaxException {
    this(resource.getResource().toPath(),
        resource.getTimestamp(),
        resource.getType(),
        resource.getVisibility(),
        resource.getPattern());
  }

  /**
   * 全参数构造函数
   * @param loc 资源路径
   * @param timestamp 资源时间戳
   * @param type 资源类型
   * @param visibility 资源可见性
   * @param pattern 资源解压模式
   */
  LocalResourceRequest(Path loc, long timestamp, LocalResourceType type,
      LocalResourceVisibility visibility, String pattern) {
    this.loc = loc;
    this.timestamp = timestamp;
    this.type = type;
    this.visibility = visibility;
    this.pattern = pattern;
  }

  @Override
  public int hashCode() {
    // 基于路径、时间戳、类型、模式计算哈希值
    int hash = loc.hashCode() ^
      (int)((timestamp >>> 32) ^ timestamp) *
      type.hashCode();
    if(pattern != null) {
      hash = hash ^ pattern.hashCode();
    }
    return hash;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LocalResourceRequest)) {
      return false;
    }
    final LocalResourceRequest other = (LocalResourceRequest) o;
    String pattern = getPattern();
    String otherPattern = other.getPattern();
    // 先比较模式是否相等
    boolean patternEquals = (pattern == null && otherPattern == null) || 
       (pattern != null && otherPattern != null && pattern.equals(otherPattern)); 
    // 依次比较路径、时间戳、类型、模式
    return getPath().equals(other.getPath()) &&
           getTimestamp() == other.getTimestamp() &&
           getType() == other.getType() &&
           patternEquals;
  }

  @Override
  public int compareTo(LocalResourceRequest other) {
    if (this == other) {
      return 0;
    }
    // 优先比较路径
    int ret = getPath().compareTo(other.getPath());
    if (0 == ret) {
      // 路径相同比较时间戳
      ret = (int)(getTimestamp() - other.getTimestamp());
      if (0 == ret) {
        // 时间戳相同比较类型
        ret = getType().ordinal() - other.getType().ordinal();
        if (0 == ret) {
          // 类型相同比较模式
          String pattern = getPattern();
          String otherPattern = other.getPattern();
          if (pattern == null && otherPattern == null) {
            ret = 0;
          } else if (pattern == null) {
            ret = -1;
          } else if (otherPattern == null) {
            ret = 1;
          } else {
            ret = pattern.compareTo(otherPattern);    
          }
        }
      }
    }
    return ret;
  }

  /** 获取资源路径 */
  public Path getPath() {
    return loc;
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public LocalResourceType getType() {
    return type;
  }

  @Override
  public URL getResource() {
    return URL.fromPath(loc);
  }

  @Override
  public long getSize() {
    return -1L;
  }

  @Override
  public LocalResourceVisibility getVisibility() {
    return visibility;
  }

  @Override
  public String getPattern() {
    return pattern;
  }
  
  @Override
  public boolean getShouldBeUploadedToSharedCache() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setShouldBeUploadedToSharedCache(
      boolean shouldBeUploadedToSharedCache) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setResource(URL resource) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setSize(long size) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setTimestamp(long timestamp) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setType(LocalResourceType type) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setVisibility(LocalResourceVisibility visibility) {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public void setPattern(String pattern) {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{ ");
    sb.append(getPath().toString()).append(", ");
    sb.append(getTimestamp()).append(", ");
    sb.append(getType()).append(", ");
    sb.append(getPattern()).append(" }");
    return sb.toString();
  }
}
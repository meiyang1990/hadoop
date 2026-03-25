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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.container;

import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.SerializationUtils;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 按资源类型存储单个容器分配的资源映射关系
 * 
 * 分配的资源可以是字符串列表形式，例如：
 * "numa": ["numa0"]
 * "gpu": ["0", "1", "2", "3"]
 * "fpga": ["1", "3"]
 * 
 * 该类主要用于NodeManager重启时的容器恢复场景，持久化保存容器资源分配信息
 */
public class ResourceMappings {

  // 按资源类型存储分配的资源信息
  private Map<String, AssignedResources> assignedResourcesMap = new HashMap<>();

  /**
   * 获取指定资源类型的已分配资源列表
   * @param resourceType 资源类型
   * @return 已分配资源列表，不存在对应类型时返回空列表
   */
  public List<Serializable> getAssignedResources(String resourceType) {
    AssignedResources ar = assignedResourcesMap.get(resourceType);
    if (null == ar) {
      return Collections.emptyList();
    }
    return ar.getAssignedResources();
  }

  /**
   * 添加指定资源类型的已分配资源
   *
   * @param resourceType 资源类型
   * @param assigned 待添加的已分配资源对象
   */
  public void addAssignedResources(String resourceType,
      AssignedResources assigned) {
    assignedResourcesMap.put(resourceType, assigned);
  }

  /**
   * 存储容器某一资源类型下分配的具体资源信息，支持序列化
   */
  public static class AssignedResources implements Serializable {
    private static final long serialVersionUID = -1059491941955757926L;
    // 存储已分配的资源列表，所有资源都需要可序列化
    private List<Serializable> resources = Collections.emptyList();

    /**
     * 获取不可修改的已分配资源列表
     * @return 不可修改的已分配资源列表
     */
    public List<Serializable> getAssignedResources() {
      return Collections.unmodifiableList(resources);
    }

    /**
     * 更新已分配资源列表
     * @param list 新的已分配资源列表
     */
    public void updateAssignedResources(List<Serializable> list) {
      this.resources = new ArrayList<>(list);
    }

    /**
     * 从字节数组反序列化恢复AssignedResources对象
     * @param bytes 序列化后的字节数组
     * @return 反序列化得到的AssignedResources对象
     * @throws IOException 反序列化失败时抛出异常
     */
    @SuppressWarnings("unchecked")
    public static AssignedResources fromBytes(byte[] bytes)
        throws IOException {
      final List<Serializable> resources;
      try {
        resources = SerializationUtils.deserialize(bytes);
      } catch (SerializationException e) {
        throw new IOException(e);
      }
      AssignedResources ar = new AssignedResources();
      ar.updateAssignedResources(resources);
      return ar;
    }

    /**
     * 将当前AssignedResources对象序列化为字节数组
     * @return 序列化后的字节数组
     * @throws IOException 序列化失败时抛出异常
     */
    public byte[] toBytes() throws IOException {
      final byte[] bytes;
      try {
        bytes = SerializationUtils.serialize((Serializable) resources);
      } catch (SerializationException e) {
        throw new IOException(e);
      }
      return bytes;
    }
  }
}
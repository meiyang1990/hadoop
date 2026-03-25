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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * 节点资源更新插件抽象基类，用于扩展处理节点上的自定义资源，由NodeStatusUpdater调用更新资源信息
 */
public abstract class NodeResourceUpdaterPlugin {
  /**
   * 更新已配置的节点资源信息，用于将自定义资源信息合并到节点总资源中
   * @param res 由外部模块（如NodeStatusUpdater）传入的节点资源对象
   * @throws YarnException 当更新过程中发生错误时抛出
   */
  public abstract void updateConfiguredResource(Resource res)
      throws YarnException;

}
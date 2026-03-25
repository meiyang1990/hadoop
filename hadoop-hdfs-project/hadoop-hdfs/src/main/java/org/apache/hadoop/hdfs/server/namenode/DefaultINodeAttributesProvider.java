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

/**
 * HDFS INode属性提供器的默认实现，直接返回INode自带的原生属性，不做额外修改
 * 为NameNode提供默认的INode属性获取能力，支持扩展自定义属性提供器
 */
public class DefaultINodeAttributesProvider extends INodeAttributeProvider {

  /** 默认单例实例，供NameNode直接使用 */
  public static INodeAttributeProvider DEFAULT_PROVIDER =
      new DefaultINodeAttributesProvider();

  /**
   * 启动属性提供器，默认实现无操作
   */
  @Override
  public void start() {
    // NO-OP
  }

  /**
   * 停止属性提供器，默认实现无操作
   */
  @Override
  public void stop() {
    // NO-OP
  }

  /**
   * 获取INode的属性信息，默认实现直接返回原始INode属性
   * @param pathElements 路径分段数组
   * @param inode 原始INode属性
   * @return 原始INode属性
   */
  @Override
  public INodeAttributes getAttributes(String[] pathElements,
      INodeAttributes inode) {
    return inode;
  }

}
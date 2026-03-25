// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.namenode.startupprogress;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * NameNode启动进度跟踪的抽象基类，为所有进度跟踪数据结构提供公共基础属性和方法。
 * 对于基本长整型属性，使用{@link Long#MIN_VALUE}作为标记值，表示属性未定义（未设置）。
 * 此类是HDFS NameNode启动阶段进度追踪模块的核心基础类。
 */
@InterfaceAudience.Private
abstract class AbstractTracking implements Cloneable {
  // 启动阶段开始时间，Long.MIN_VALUE表示未设置
  long beginTime = Long.MIN_VALUE;
  // 启动阶段结束时间，Long.MIN_VALUE表示未设置
  long endTime = Long.MIN_VALUE;

  /**
   * 基类属性拷贝方法，供子类克隆时调用，将当前基类所有属性拷贝到目标对象。
   * 
   * @param dest 接收属性拷贝的目标AbstractTracking对象
   */
  protected void copy(AbstractTracking dest) {
    dest.beginTime = beginTime;
    dest.endTime = endTime;
  }
}
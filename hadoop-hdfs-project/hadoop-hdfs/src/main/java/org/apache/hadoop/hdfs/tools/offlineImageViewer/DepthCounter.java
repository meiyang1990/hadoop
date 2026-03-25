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
package org.apache.hadoop.hdfs.tools.offlineImageViewer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * HDFS离线镜像查看器深度计数器工具类。
 * 用于跟踪Visitor类（ImageVisitor、EditsVisitor等）遍历文件系统目录树结构时的当前深度层级，
 * 帮助Visitor维护遍历过程中的层级关系，支持深度递增、递减和当前深度查询操作。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DepthCounter {
  private int depth = 0;

  /**
   * 层级深度加1，进入下一级目录时调用
   */
  public void incLevel() { depth++; }

  /**
   * 层级深度减1，退出当前目录时调用，保证深度不会小于0
   */
  public void decLevel() { if(depth >= 1) depth--; }

  /**
   * 获取当前遍历的层级深度
   * @return 当前深度值，根目录深度为0
   */
  public int  getLevel() { return depth; }
}
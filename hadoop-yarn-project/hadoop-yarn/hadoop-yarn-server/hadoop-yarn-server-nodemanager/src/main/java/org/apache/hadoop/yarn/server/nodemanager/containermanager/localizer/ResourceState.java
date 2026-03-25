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

/**
 * 资源本地化状态枚举，定义了NodeManager本地化资源过程中的所有可能状态
 */
enum ResourceState {
  /** 初始化状态，资源已创建但未开始下载 */
  INIT,
  /** 下载中状态，资源正在从远程下载 */
  DOWNLOADING,
  /** 本地化完成状态，资源已经下载完成可以使用 */
  LOCALIZED,
  /** 下载失败状态，资源本地化过程中出现错误 */
  FAILED
}
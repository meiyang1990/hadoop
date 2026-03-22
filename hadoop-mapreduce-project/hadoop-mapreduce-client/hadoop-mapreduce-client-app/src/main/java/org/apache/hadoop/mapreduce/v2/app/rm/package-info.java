// 这个文件已经全部加上中文注释
/*
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

/**
 * MapReduce ApplicationMaster 与 YARN ResourceManager 交互的核心包
 * <p>
 * 本包负责实现 ApplicationMaster 向 YARN 申请、释放资源，处理 ResourceManager
 * 下发的容器分配与事件通知，是 MapReduce 作业运行过程中对接 YARN 资源调度的核心模块，
 * 包含资源请求构建、容器分配处理、AM与RM通信协议实现等核心逻辑。
 * 该包仅为Hadoop内部私有API，不对外公开使用。
 */
@InterfaceAudience.Private
package org.apache.hadoop.mapreduce.v2.app.rm;
import org.apache.hadoop.classification.InterfaceAudience;
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
 * 文件缓存模块包信息，提供MapReduce任务运行时的分布式文件缓存能力。
 * 该模块负责管理作业依赖文件（如jar包、字典文件、配置文件等）在YARN集群节点
 * 的缓存分发，减少重复文件传输，提升任务启动效率和集群资源利用率。
 * 该包仅为MapReduce框架内部使用，不对外公开API。
 */
@InterfaceAudience.Private
package org.apache.hadoop.mapreduce.filecache;
import org.apache.hadoop.classification.InterfaceAudience;
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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * HDFS数据均衡工具包，提供HDFS集群数据均衡的核心实现。
 * 随着集群运行，数据分布会逐渐出现不均衡，本包提供数据块重新分布能力，
 * 通过在DataNode之间迁移数据块，让集群存储利用率达到均衡，提升集群整体性能。
 */
package org.apache.hadoop.hdfs.server.balancer;
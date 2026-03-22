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

/**
 * HDFS NameNode  fine-grained locking（细粒度锁）模块包，提供基于目录树节点的细粒度锁机制实现。
 * 该机制通过对目录树的单个节点分别加锁，替代NameNode全局根节点锁，提升高并发场景下
 * 文件元数据操作的并发性能，同时保证元数据修改的一致性。
 */
package org.apache.hadoop.hdfs.server.namenode.fgl;
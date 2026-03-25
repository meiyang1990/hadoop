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
 * 本包提供YARN容器放置约束算法所需的各类迭代器实现，
 * 用于在满足放置约束的节点集合上进行遍历搜索，找出符合要求的节点分配方案。
 * 所有代码均为私有API，不对外公开，接口稳定性尚未确定。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.algorithm.iterators;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
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
 * 中间清单输出提交器包，针对对象存储优化MapReduce输出提交流程。
 * <p>
 * 本包的设计优化场景：对象存储目录列表操作慢、目录重命名不保证原子性，
 * 以及多任务尝试输出混合生成深度文件目录树的场景。
 * 核心思路是通过清单文件记录所有输出文件，在作业提交阶段统一完成最终文件移动，
 * 避免依赖对象存储不支持的原子重命名操作，提升输出提交效率与可靠性。
 * <p>
 * 除非特殊说明，本模块下所有类均为私有API，不对外公开，当前处于不稳定状态。
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
package org.apache.hadoop.mapreduce.lib.output.committer.manifest;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
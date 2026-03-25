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
 * 中间清单提交器的阶段实现包
 *
 * 该包包含任务和作业提交流程中各个独立与聚合阶段的核心实现，
 * 基于分阶段设计拆解作业提交流程，为基于清单文件的输出提交器
 * 提供可复用、可扩展的阶段化执行能力，支持文件输出的原子提交。
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
package org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
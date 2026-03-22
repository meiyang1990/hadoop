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
 * MapReduce应用程序提交输出的核心实现包，提供作业输出提交器相关逻辑
 * <p>
 * 该包负责处理MapReduce作业最终输出的提交阶段，实现了不同场景下的输出提交策略，
 * 包括作业成功完成后的输出文件原子提交、失败作业的输出清理等核心流程，
 * 是MapReduce作业执行生命周期的最后阶段，保障输出数据的一致性和正确性。
 * </p>
 */
@InterfaceAudience.Private
package org.apache.hadoop.mapreduce.v2.app.commit;
import org.apache.hadoop.classification.InterfaceAudience;
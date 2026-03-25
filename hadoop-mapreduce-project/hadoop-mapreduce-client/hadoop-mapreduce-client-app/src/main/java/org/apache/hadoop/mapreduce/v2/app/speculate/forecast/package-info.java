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
 * MapReduce推测执行任务运行时间预测模块。
 * 本包提供多种预测算法实现，用于计算MapReduce任务的剩余运行时间，
 * 为推测执行判断是否需要启动备份任务提供决策依据。
 */
@InterfaceAudience.Private
package org.apache.hadoop.mapreduce.v2.app.speculate.forecast;
import org.apache.hadoop.classification.InterfaceAudience;
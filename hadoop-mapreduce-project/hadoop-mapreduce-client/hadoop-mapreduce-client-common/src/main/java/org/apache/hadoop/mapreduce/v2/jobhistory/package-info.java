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
 * MapReduce V2 作业历史记录模块包定义。
 * 本包提供MapReduce作业完成后的历史数据存储、索引、查询相关核心能力，
 * 支撑作业历史服务器展示已完成作业的统计信息、配置和执行日志，
 * 帮助用户排查作业问题、分析作业性能。
 * 本包仅面向Hadoop内部使用，不对外公开API。
 */
@InterfaceAudience.Private
package org.apache.hadoop.mapreduce.v2.jobhistory;
import org.apache.hadoop.classification.InterfaceAudience;
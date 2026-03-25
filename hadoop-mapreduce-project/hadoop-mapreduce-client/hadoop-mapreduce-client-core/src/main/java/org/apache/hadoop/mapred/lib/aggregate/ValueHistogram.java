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

package org.apache.hadoop.mapred.lib.aggregate;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 文件说明：MapReduce旧API版的值直方图聚合器，为兼容旧mapred API实现包装类
 * 
 * 该类继承新版本{@link org.apache.hadoop.mapreduce.lib.aggregate.ValueHistogram}实现，
 * 实现旧mapred API的ValueAggregator接口，用于统计字符串序列中不同值的出现频率分布，
 * 生成输入值的直方图统计结果。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
/**
 * 值直方图聚合器，兼容旧版MapReduce API，用于统计输入字符串的频率分布
 */
public class ValueHistogram 
    extends org.apache.hadoop.mapreduce.lib.aggregate.ValueHistogram 
    implements ValueAggregator<String> {
}
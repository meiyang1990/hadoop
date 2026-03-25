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
 * 文件说明：MapReduce旧API兼容类，为字符串最小值聚合计算提供适配
 * 
 * 该类实现了值聚合器接口，维护字符串序列中的字典序最小值，是新版mapreduce包中同类实现的兼容包装
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class StringValueMin 
    extends org.apache.hadoop.mapreduce.lib.aggregate.StringValueMin 
    implements ValueAggregator<String> {
}
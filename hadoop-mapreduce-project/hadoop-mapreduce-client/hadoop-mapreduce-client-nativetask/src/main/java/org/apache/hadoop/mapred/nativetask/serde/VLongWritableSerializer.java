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

package org.apache.hadoop.mapred.nativetask.serde;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.mapred.nativetask.INativeComparable;

/**
 * VLongWritable 类型的原生任务序列化器
 * 继承默认序列化实现，实现原生可比较接口，为MapReduce原生任务提供VLongWritable类型的序列化与排序支持
 */
@InterfaceAudience.Private
public class VLongWritableSerializer extends DefaultSerializer implements
    INativeComparable {
}
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
package org.apache.hadoop.mapreduce.checkpoint;

import org.apache.hadoop.io.Writable;

/**
 * CheckpointID 接口定义了MapReduce任务检查点的唯一标识符，用于定位和恢复检查点数据。
 * 实现类可以携带少量检查点元数据，必须为CheckpointService提供足够信息以定位加载检查点。
 * 继承Writable接口支持序列化，可在Hadoop RPC中传输。
 */
public interface CheckpointID extends Writable {

}
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
package org.apache.hadoop.mapred.lib.db;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 文件说明：Hadoop MapReduce旧API中数据库可写入对象接口，继承新版MapReduce DBWritable接口
 * 核心职责：定义可读写关系型数据库的对象契约，供DBInputFormat/DBOutputFormat使用
 * 用于MapReduce作业从数据库读取数据、将计算结果写入数据库场景
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface DBWritable 
    extends org.apache.hadoop.mapreduce.lib.db.DBWritable {
	
}
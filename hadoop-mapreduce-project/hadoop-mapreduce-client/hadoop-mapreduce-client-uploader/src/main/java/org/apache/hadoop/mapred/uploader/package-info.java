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
 * @package org.apache.hadoop.mapred.uploader
 * 该包提供MapReduce框架上传工具相关实现，用于将MapReduce框架jar包上传到HDFS分布式缓存，
 * 减少YARN容器启动时从本地磁盘下载框架依赖的开销，提升作业启动速度。
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
package org.apache.hadoop.mapred.uploader;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
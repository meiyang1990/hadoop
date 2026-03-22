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
 * MapReduce V2 公共工具包，提供框架内部使用的通用工具类与辅助方法
 * 主要用于支持作业提交、状态转换、资源转换等核心流程中的通用操作
 * 该包仅对MapReduce框架内部开放，不对外公开API
 */
@InterfaceAudience.Private
package org.apache.hadoop.mapreduce.v2.util;
import org.apache.hadoop.classification.InterfaceAudience;
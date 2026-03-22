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
 * MapReduce 委托令牌安全认证包，提供MapReduce作业提交与运行过程中
 * 委托令牌的生成、验证和管理相关的核心类与接口。
 * 委托令牌用于在Hadoop安全模式下，为用户作业提供跨节点的身份认证能力，
 * 支持YARN和MapReduce服务之间的安全身份传递。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
package org.apache.hadoop.mapreduce.security.token.delegation;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
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
 * MapReduce ApplicationMaster 安全授权包，提供MR应用执行过程中各类操作的访问控制校验能力。
 * 本包主要负责对提交到YARN运行的MapReduce作业进行权限检查，确保只有授权用户才能访问和操作作业。
 */
@InterfaceAudience.Private
package org.apache.hadoop.mapreduce.v2.app.security.authorize;
import org.apache.hadoop.classification.InterfaceAudience;
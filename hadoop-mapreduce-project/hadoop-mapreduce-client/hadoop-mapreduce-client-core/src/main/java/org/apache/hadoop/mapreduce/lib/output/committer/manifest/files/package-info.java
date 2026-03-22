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
 * Manifest输出提交器的持久化数据格式包。
 * 本包定义了任务向作业提交器传递输出数据信息的持久化格式，以及作业成功标记文件(_SUCCESS)的数据结构。
 * 其中任务输出元数据清单由 {@link org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest} 定义，
 * 成功标记数据由 {@link org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.ManifestSuccessData} 定义。
 * 
 * _SUCCESS文件格式与S3A提交器的{@code org.apache.hadoop.fs.s3a.commit.files.ManifestSuccessData}保持JSON级兼容，
 * 目的是支持统一格式加载，方便测试、验证和问题排查，
 * 
 * 访问控制请参考具体格式的声明，其中_SUCCESS文件格式可供测试使用。
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
package org.apache.hadoop.mapreduce.lib.output.committer.manifest.files;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

/**
 * YARN Unmanaged Application Master（非托管应用 masters）服务端核心包，
 * 提供对用户自主管理的 Application Master 的支持，包含相关公共实现和工具类。
 * 非托管AM允许应用不依赖YARN的资源分配自行管理运行，适用于特殊调度场景。
 */
package org.apache.hadoop.yarn.server.uam;
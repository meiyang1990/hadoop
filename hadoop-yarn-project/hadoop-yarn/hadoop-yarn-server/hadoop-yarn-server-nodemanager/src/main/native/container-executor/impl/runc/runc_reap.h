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
 * @file runc_reap.h
 * @brief runC容器镜像层挂载回收模块头文件
 * @details 负责清理NodeManager节点上不常用的runc容器镜像层挂载，
 *          避免节点挂载数过多，通过LRU策略回收未被使用的挂载点。
 */
#ifndef RUNC_RUNC_REAP_H
#define RUNC_RUNC_REAP_H

#include "runc_base_ctx.h"

/**
 * @brief 按照LRU策略回收容器镜像层挂载点，保留指定数量的挂载
 * @details 仍被运行中容器使用的镜像层会被保留，实际保留数量可能超过目标值
 * @param num_preserve 需要保留的镜像层挂载数量
 * @return 0 表示执行成功，非零值表示失败错误码
 */
int reap_runc_layer_mounts(int num_preserve);

/**
 * @brief 使用已创建的runc基础上下文回收镜像层挂载，避免重复创建上下文
 * @details 复用传入的上下文对象，相比无上下文版本减少了初始化开销
 * @param ctx 已初始化的runc基础上下文指针
 * @param num_preserve 需要保留的镜像层挂载数量
 * @return 0 表示执行成功，非零值表示失败错误码
 */
int reap_runc_layer_mounts_with_ctx(runc_base_ctx* ctx, int num_preserve);

#endif /* RUNC_RUNC_REAP_H */
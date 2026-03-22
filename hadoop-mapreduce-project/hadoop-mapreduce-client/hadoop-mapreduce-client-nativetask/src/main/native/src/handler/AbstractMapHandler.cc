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
 * @file AbstractMapHandler.cc
 * @brief  NativeTask Map任务处理抽象基类静态成员定义
 * 
 * 该文件属于Hadoop MapReduce本地任务模块，定义了AbstractMapHandler基类
 * 的静态命令常量，用于和Java侧交互获取Map输出相关路径和组合处理器
 */

#include "lib/commons.h"
#include "util/StringUtil.h"
#include "MCollectorOutputHandler.h"
#include "lib/NativeObjectFactory.h"
#include "lib/MapOutputCollector.h"
#include "CombineHandler.h"

using std::string;
using std::vector;

namespace NativeTask {

/**
 * @brief 获取Map输出文件路径命令定义
 * 命令ID: 100
 */
const Command AbstractMapHandler::GET_OUTPUT_PATH(100, "GET_OUTPUT_PATH");

/**
 * @brief 获取Map输出索引文件路径命令定义
 * 命令ID: 101
 */
const Command AbstractMapHandler::GET_OUTPUT_INDEX_PATH(101, "GET_OUTPUT_INDEX_PATH");

/**
 * @brief 获取Spill溢出文件路径命令定义
 * 命令ID: 102
 */
const Command AbstractMapHandler::GET_SPILL_PATH(102, "GET_SPILL_PATH");

/**
 * @brief 获取Combine处理器命令定义
 * 命令ID: 103
 */
const Command AbstractMapHandler::GET_COMBINE_HANDLER(103, "GET_COMBINE_HANDLER");

} // namespace NativeTask
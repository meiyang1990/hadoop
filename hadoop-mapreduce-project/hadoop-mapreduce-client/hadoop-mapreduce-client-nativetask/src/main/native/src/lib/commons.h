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
 * @file commons.h
 * @brief Hadoop MapReduce原生任务公共头文件
 * @details 统一引入原生任务模块所需的系统头文件、基础工具定义，
 *          为整个原生任务模块提供公共依赖聚合，避免各个模块重复引入相同头文件
 */

#ifndef COMMONS_H_
#define COMMONS_H_

#include <sys/types.h>
#include <sys/stat.h>
#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <memory.h>
#include <fcntl.h>

// 启用格式化宏定义，确保跨平台兼容格式化输出
#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include <limits>
#include <string>
#include <vector>
#include <list>
#include <set>
#include <map>
#include <algorithm>

// 引入项目内部基础类型定义
#include "lib/primitives.h"
// 引入日志工具
#include "lib/Log.h"
// 引入原生任务核心接口
#include "NativeTask.h"

// 引入常量定义
#include "lib/Constants.h"

// 引入迭代器接口
#include "lib/Iterator.h"

#endif /* COMMONS_H_ */
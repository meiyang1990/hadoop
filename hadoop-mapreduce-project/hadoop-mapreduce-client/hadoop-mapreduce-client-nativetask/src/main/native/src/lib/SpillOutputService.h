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

/**
 * @file SpillOutputService.h
 * @brief 原生MapReduce任务溢出输出服务抽象接口，定义获取溢出输出路径和Combine处理器的规范
 * 
 * 该头文件定义了MapReduce本地任务中，溢写（Spill）过程需要的输出服务抽象接口，
 * 为不同实现提供统一的路径获取和Combine处理器获取规范，是本地任务溢写流程的核心抽象层。
 */

#ifndef SPILL_OUTPUT_SERVICE_H_
#define SPILL_OUTPUT_SERVICE_H_

#include <stdint.h>
#include <string>

/**
 * @brief 原生MapReduce任务命名空间
 */
namespace NativeTask {

class CombineHandler;

using std::string;

/**
 * @class SpillOutputService
 * @brief 溢出输出服务抽象接口，定义获取输出路径和Combine处理器的统一规范
 * 
 * 核心职责：
 * 1. 为Map任务溢写过程提供输出文件路径，包括临时溢出路径、最终输出路径、索引文件路径
 * 2. 提供Java端Combine处理器用于本地任务的合并操作
 * 3. 作为抽象接口，允许不同场景下有不同的实现，解耦溢写逻辑和路径/资源获取逻辑
 */
class SpillOutputService {
public:
  /**
   * @brief 虚析构函数，保证子类正确析构
   */
  virtual ~SpillOutputService() {}

  /**
   * @brief 获取临时溢写文件路径
   * @return 指向溢写路径字符串的指针
   */
  virtual string * getSpillPath() = 0;

  /**
   * @brief 获取最终输出文件路径
   * @return 指向输出路径字符串的指针
   */
  virtual string * getOutputPath() = 0;

  /**
   * @brief 获取输出索引文件路径
   * @return 指向输出索引路径字符串的指针
   */
  virtual string * getOutputIndexPath() = 0;

  /**
   * @brief 获取Java端Combine处理器实例
   * @return 指向CombineHandler实例的指针
   */
  virtual CombineHandler * getJavaCombineHandler() = 0;
};

} // namespace NativeTask

#endif /* SPILL_OUTPUT_SERVICE_H_ */
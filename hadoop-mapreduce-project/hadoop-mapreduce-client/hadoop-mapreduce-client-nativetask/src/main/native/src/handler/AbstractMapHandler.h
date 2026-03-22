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
 * @file AbstractMapHandler.h
 * @brief MapReduce本地任务Map处理抽象基类，提供Map任务公共基础设施
 * 
 * 属于Hadoop MapReduce本地任务模块，定义了所有Map处理处理器的公共接口
 * 和通用方法，封装了与Java侧的路径获取、Combine处理器获取等核心交互逻辑
 */

#ifndef ABSTRACT_MAP_HANDLER_H
#define ABSTRACT_MAP_HANDLER_H

#include "NativeTask.h"
#include "BatchHandler.h"
#include "lib/SpillOutputService.h"
#include "lib/Combiner.h"
#include "CombineHandler.h"

namespace NativeTask {

/**
 * @class AbstractMapHandler
 * @brief 抽象Map任务处理器基类
 * 
 * 核心职责：
 * 1. 封装Map任务输出路径、溢写路径等公共资源获取逻辑
 * 2. 提供Java侧Combine处理器的获取能力
 * 3. 继承BatchHandler和SpillOutputService，统一批处理和溢写输出接口
 * 所有具体Map处理器都需要继承该基类实现自定义处理逻辑
 */
class AbstractMapHandler : public BatchHandler,  public SpillOutputService {
public:
  // 命令常量：获取最终输出文件路径
  static const Command GET_OUTPUT_PATH;
  // 命令常量：获取输出索引文件路径
  static const Command GET_OUTPUT_INDEX_PATH;
  // 命令常量：获取溢写临时文件路径
  static const Command GET_SPILL_PATH;
  // 命令常量：获取Combine处理器实例
  static const Command GET_COMBINE_HANDLER;

public:
  AbstractMapHandler() {}

  virtual ~AbstractMapHandler() {}

  /**
   * @brief 配置处理器，保存配置对象指针
   * @param config 配置对象指针
   */
  virtual void configure(Config * config) {
    _config = config;
  }

  /**
   * @brief 向Java侧请求获取Map最终输出文件路径
   * @return 输出路径字符串指针，失败返回NULL
   */
  virtual string * getOutputPath() {
    ResultBuffer * outputPathResult = call(GET_OUTPUT_PATH, NULL);
    if (NULL == outputPathResult) {
      return NULL;
    }
    string * outputPath = outputPathResult->readString();

    delete outputPathResult;
    return outputPath;
  }

  /**
   * @brief 向Java侧请求获取Map输出索引文件路径
   * @return 索引路径字符串指针，失败返回NULL
   */
  virtual string * getOutputIndexPath() {

    ResultBuffer * outputIndexPath = call(GET_OUTPUT_INDEX_PATH, NULL);
    if (NULL == outputIndexPath) {
      return NULL;
    }
    string * indexpath = outputIndexPath->readString();
    delete outputIndexPath;
    return indexpath;
  }


  /**
   * @brief 向Java侧请求获取溢写临时文件路径
   * @return 溢写路径字符串指针，失败返回NULL
   */
  virtual string * getSpillPath() {
    ResultBuffer * spillPathBuffer = call(GET_SPILL_PATH, NULL);
    if (NULL == spillPathBuffer) {
      return NULL;
    }
    string * spillpath = spillPathBuffer->readString();
    delete spillPathBuffer;
    return spillpath;
  }

  /**
   * @brief 向Java侧请求获取Java端Combine处理器实例
   * @return Combine处理器实例指针，未配置则返回NULL
   */
  virtual CombineHandler * getJavaCombineHandler() {

    LOG("[MapOutputCollector::configure] java combiner is configured");

    ResultBuffer * getCombineHandlerResult = call(GET_COMBINE_HANDLER, NULL);
    if (NULL != getCombineHandlerResult) {
      // 重置读取指针到缓冲区开头
      getCombineHandlerResult->setReadPoint(0);

      // 从缓冲区读取处理器指针并转换类型
      CombineHandler * javaCombiner = (CombineHandler *)((BatchHandler * )(getCombineHandlerResult->readPointer()));
      delete getCombineHandlerResult;
      return javaCombiner;
    }
    return NULL;
  }

};

} // namespace NativeTask

#endif /* MMAPPERHANDLER_H_ */
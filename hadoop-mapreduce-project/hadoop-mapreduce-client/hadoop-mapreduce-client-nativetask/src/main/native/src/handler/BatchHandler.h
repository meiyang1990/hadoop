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
 * @file BatchHandler.h
 * @brief MapReduce本地任务批处理处理器头文件，定义Java端NativeBatchProcessor的本地对端处理接口
 *
 * 该文件属于Hadoop MapReduce本地任务模块，提供了批量处理数据的基类，
 * 用于Java层和Native层之间的数据批量交互，提升本地任务执行性能。
 */

#ifndef BATCHHANDLER_H_
#define BATCHHANDLER_H_

#include "NativeTask.h"
#include "lib/Buffers.h"

namespace NativeTask {

/**
 * @class BatchHandler
 * @brief 批量数据处理器基类，对应Java端的NativeBatchProcessor
 *
 * 核心职责是处理Java侧发来的批量数据，完成Native侧的计算处理后，
 * 将结果批量回传给Java侧，减少JNI调用次数，提升处理性能。
 * 子类需要根据具体业务场景实现对应处理方法。
 */
class BatchHandler : public Configurable {
protected:
  ByteBuffer _in;    ///< 输入数据缓冲，存储Java侧发来待处理的数据
  ByteBuffer _out;   ///< 输出数据缓冲，存储Native侧处理完等待回传给Java的结果
  void * _processor; ///< 存储Java侧处理器的全局JNI引用
  Config * _config;   ///< 配置对象指针，存储任务相关配置参数
public:
  /**
   * @brief 构造函数
   */
  BatchHandler();

  /**
   * @brief 析构函数
   */
  virtual ~BatchHandler();

  /**
   * @brief 获取当前Native对象类型
   * @return 返回对象类型BatchHandlerType
   */
  virtual NativeObjectType type() {
    return BatchHandlerType;
  }

  /**
   * @brief 设置Java侧处理器的JNI全局引用
   * @param processor 处理器指针，指向JNI全局引用
   */
  void setProcessor(void * processor) {
    _processor = processor;
  }

  /**
   * @brief 释放Java侧处理器的JNI引用
   */
  void releaseProcessor();

  /**
   * @brief 初始化Native侧批处理器，由Java侧调用
   *
   * 完成配置设置、输入输出缓冲区初始化等准备工作
   * @param config 配置对象指针
   * @param inputBuffer 输入缓冲区地址
   * @param inputBufferCapacity 输入缓冲区容量
   * @param outputBuffer 输出缓冲区地址
   * @param outputBufferCapacity 输出缓冲区容量
   */
  void onSetup(Config * config, char * inputBuffer, uint32_t inputBufferCapacity,
      char * outputBuffer, uint32_t outputBufferCapacity);

  /**
   * @brief 通知Native侧有新的输入数据可用，由Java侧调用
   * @param length 输入缓冲区中可用数据长度
   */
  void onInputData(uint32_t length);

  /**
   * @brief 加载输入数据钩子方法，供子类重写，默认不做任何处理
   */
  virtual void onLoadData() {
  }

  /**
   * @brief 通知Native侧输入已结束，由Java侧调用
   */
  void onFinish() {
    finish();
  }

  /**
   * @brief 处理Java侧发来的命令，由Java侧调用，默认忽略所有命令
   * @param command 命令对象
   * @param param 命令参数读写缓冲区
   * @return 命令处理结果缓冲区，默认返回NULL
   */
  virtual ResultBuffer * onCall(const Command& command, ReadWriteBuffer * param) {
    return NULL;
  }

protected:
  /**
   * @brief 处理命令请求的内部方法，由子类实现
   * @param cmd 命令对象
   * @param param 命令参数缓冲区
   * @return 处理结果缓冲区
   */
  virtual ResultBuffer * call(const Command& cmd, ParameterBuffer * param);

  /**
   * @brief 调用Java侧的flushOutput方法，将当前输出缓冲区数据回传
   *
   * 当输出缓冲区满或需要主动刷新结果时，调用该方法将数据刷回Java侧
   */
  virtual void flushOutput();

  /**
   * @brief 调用Java侧的finishOutput方法，通知输出已完成
   */
  void finishOutput();

  /**
   * @brief 向输出缓冲区写入数据，自动处理缓冲区满刷出逻辑
   * @param buff 待写入数据指针
   * @param length 待写入数据长度
   */
  inline void output(const char * buff, uint32_t length) {
    while (length > 0) {
      uint32_t remain = _out.remain();
      if (length > remain) {
        // 剩余空间不足，先刷出现有数据
        flushOutput();
      }
      uint32_t cp = length < remain ? length : remain;
      simple_memcpy(_out.current(), buff, cp);
      buff += cp;
      length -= cp;
      _out.advance(cp);
    }
  }

  /**
   * @brief 向输出缓冲区写入一个32位整数，自动处理缓冲区满刷出逻辑
   * @param v 待写入的32位无符号整数
   */
  inline void outputInt(uint32_t v) {
    if (4 > _out.remain()) {
      // 剩余空间不足，先刷出现有数据
      flushOutput();
    }
    *(uint32_t*)(_out.current()) = v;
    _out.advance(4);
  }

  /////////////////////////////////////////////////////////////
  // 子类可根据需要重实现以下方法
  /////////////////////////////////////////////////////////////

  /**
   * @brief 配置处理器，由onSetup调用，默认不做处理
   * @param config 配置对象指针
   * 子类可重写该方法完成自定义初始化
   */
  virtual void configure(Config * config) {
  }

  /**
   * @brief 完成处理，由onFinish调用，默认刷新并关闭输出
   * 子类可重写该方法完成自定义收尾工作
   */
  virtual void finish() {
    flushOutput();
    finishOutput();
  }
  ;

  /**
   * @brief 处理输入数据，由onInputData调用，默认不做处理
   * @param byteBuffer 输入数据缓冲区
   * 子类可重写该方法实现具体的业务处理逻辑
   */
  virtual void handleInput(ByteBuffer & byteBuffer) {
  }
};

} // namespace NativeTask

#endif /* BATCHHANDLER_H_ */
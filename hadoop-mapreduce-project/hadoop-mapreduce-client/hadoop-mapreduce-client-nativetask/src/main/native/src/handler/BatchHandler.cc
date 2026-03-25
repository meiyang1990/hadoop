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
 * @file BatchHandler.cc
 * @brief MapReduce本地任务批量处理处理器实现，负责Java侧与Native侧的数据交互与处理
 * @details 该文件实现了NativeBatchProcessor对应的Native侧处理器，
 * 提供批量数据处理能力，通过直接内存缓冲区减少Java/Native拷贝开销，
 * 加速MapReduce任务的本地执行。
 */

#ifndef QUICK_BUILD
#include "org_apache_hadoop_mapred_nativetask_NativeBatchProcessor.h"
#endif
#include "lib/commons.h"
#include "jni_md.h"
#include "lib/jniutils.h"
#include "BatchHandler.h"
#include "lib/NativeObjectFactory.h"

///////////////////////////////////////////////////////////////
// NativeBatchProcessor jni util methods
///////////////////////////////////////////////////////////////

/** Java类字段ID缓存：输入缓冲区 */
static jfieldID InputBufferFieldID = NULL;
/** Java类字段ID缓存：输出缓冲区 */
static jfieldID OutputBufferFieldID = NULL;
/** Java方法ID缓存：刷新输出数据到Java侧 */
static jmethodID FlushOutputMethodID = NULL;
/** Java方法ID缓存：结束输出处理 */
static jmethodID FinishOutputMethodID = NULL;
/** Java方法ID缓存：发送命令到Java侧处理 */
static jmethodID SendCommandToJavaMethodID = NULL;

///////////////////////////////////////////////////////////////
// BatchHandler methods
///////////////////////////////////////////////////////////////

namespace NativeTask {

/**
 * @brief 将Java字节数组转换为Native读写缓冲区
 * @param jenv JNI环境指针
 * @param src Java字节数组源
 * @return 转换后的Native读写缓冲区，输入为NULL时返回NULL
 */
ReadWriteBuffer * JNU_ByteArraytoReadWriteBuffer(JNIEnv * jenv, jbyteArray src) {
  if (NULL == src) {
    return NULL;
  }
  jsize len = jenv->GetArrayLength(src);

  ReadWriteBuffer * ret = new ReadWriteBuffer(len);
  jenv->GetByteArrayRegion(src, 0, len, (jbyte*)ret->getBuff());
  ret->setWritePoint(len);
  return ret;
}

/**
 * @brief 将Native读写缓冲区转换为Java字节数组
 * @param jenv JNI环境指针
 * @param result Native读写缓冲区
 * @return 转换后的Java字节数组，输入缓冲区为空或长度为0时返回NULL
 */
jbyteArray JNU_ReadWriteBufferToByteArray(JNIEnv * jenv, ReadWriteBuffer * result) {
  if (NULL == result || result->getWritePoint() == 0) {
    return NULL;
  }

  jbyteArray ret = jenv->NewByteArray(result->getWritePoint());
  jenv->SetByteArrayRegion(ret, 0, result->getWritePoint(), (jbyte*)result->getBuff());
  return ret;
}

/**
 * @class BatchHandler
 * @brief 批量数据处理处理器基类，负责Java侧与Native侧的批量数据交互
 * @details 协调Java端输入输出缓冲区，处理数据输入、输出刷新、命令调用等逻辑，
 * 具体业务处理由子类实现，基于直接内存减少跨拷贝开销提升处理性能。
 */

/** 构造函数 */
BatchHandler::BatchHandler()
    : _processor(NULL), _config(NULL) {
}

/** 析构函数，释放资源 */
BatchHandler::~BatchHandler() {
  releaseProcessor();
  if (NULL != _config) {
    delete _config;
    _config = NULL;
  }
}

/**
 * @brief 释放Java处理器对象的全局引用
 */
void BatchHandler::releaseProcessor() {
  if (_processor != NULL) {
    JNIEnv * env = JNU_GetJNIEnv();
    env->DeleteGlobalRef((jobject)_processor);
    _processor = NULL;
  }
}

/**
 * @brief 处理Java侧传入的输入数据
 * @param length 输入数据长度
 */
void BatchHandler::onInputData(uint32_t length) {
  _in.rewind(0, length);
  handleInput(_in);
}

/**
 * @brief 将Native侧输出缓冲区数据刷新到Java侧处理
 */
void BatchHandler::flushOutput() {

  if (NULL == _out.base()) {
    return;
  }

  uint32_t length = _out.position();
  _out.position(0);

  if (length == 0) {
    return;
  }

  JNIEnv * env = JNU_GetJNIEnv();
  // 调用Java侧flush方法输出数据
  env->CallVoidMethod((jobject)_processor, FlushOutputMethodID, (jint)length);
  if (env->ExceptionCheck()) {
    THROW_EXCEPTION(JavaException, "FlushOutput throw exception");
  }
}

/**
 * @brief 通知Java侧输出处理完成
 */
void BatchHandler::finishOutput() {
  if (NULL == _out.base()) {
    return;
  }
  JNIEnv * env = JNU_GetJNIEnv();
  // 调用Java侧finish方法结束输出
  env->CallVoidMethod((jobject)_processor, FinishOutputMethodID);
  if (env->ExceptionCheck()) {
    THROW_EXCEPTION(JavaException, "FinishOutput throw exception");
  }
}

/**
 * @brief 初始化处理器，配置输入输出缓冲区
 * @param config 配置对象
 * @param inputBuffer 输入缓冲区地址
 * @param inputBufferCapacity 输入缓冲区容量
 * @param outputBuffer 输出缓冲区地址
 * @param outputBufferCapacity 输出缓冲区容量
 */
void BatchHandler::onSetup(Config * config, char * inputBuffer, uint32_t inputBufferCapacity,
    char * outputBuffer, uint32_t outputBufferCapacity) {
  this->_config = config;
  _in.reset(inputBuffer, inputBufferCapacity);
  if (NULL != outputBuffer) {
    // 检查输出缓冲区容量是否满足最小要求
    if (outputBufferCapacity <= 1024) {
      THROW_EXCEPTION(IOException, "Output buffer size too small for BatchHandler");
    }
    _out.reset(outputBuffer, outputBufferCapacity);
    _out.rewind(0, outputBufferCapacity);

    LOG("[BatchHandler::onSetup] input Capacity %d, output capacity %d",
        inputBufferCapacity, _out.limit());
  }
  configure(_config);
}

/**
 * @brief 调用Java侧处理命令，返回处理结果
 * @param cmd 命令对象
 * @param param 命令参数缓冲区
 * @return Java侧处理结果缓冲区
 */
ResultBuffer * BatchHandler::call(const Command& cmd, ParameterBuffer * param) {
  JNIEnv * env = JNU_GetJNIEnv();
  // 将Native参数转换为Java字节数组
  jbyteArray jcmdData = JNU_ReadWriteBufferToByteArray(env, param);
  // 调用Java侧命令处理方法
  jbyteArray ret = (jbyteArray)env->CallObjectMethod((jobject)_processor, SendCommandToJavaMethodID,
      cmd.id(), jcmdData);


  if (env->ExceptionCheck()) {
    THROW_EXCEPTION(JavaException, "SendCommandToJava throw exception");
  }
  // 将Java返回结果转换为Native缓冲区
  return JNU_ByteArraytoReadWriteBuffer(env, ret);
}

} // namespace NativeTask

///////////////////////////////////////////////////////////////
// NativeBatchProcessor jni methods
///////////////////////////////////////////////////////////////
using namespace NativeTask;

/*
 * Class:     org_apache_hadoop_mapred_nativetask_NativeBatchProcessor
 * Method:    setupHandler
 * Signature: (J)V
 */
/**
 * @brief JNI方法：初始化Native侧处理器
 * @param jenv JNI环境指针
 * @param processor Java侧NativeBatchProcessor对象
 * @param handler Native处理器指针地址
 * @param configs 配置键值对数组
 */
void JNICALL Java_org_apache_hadoop_mapred_nativetask_NativeBatchProcessor_setupHandler(
    JNIEnv * jenv, jobject processor, jlong handler, jobjectArray configs) {
  try {
    // 创建配置对象，从Java数组加载配置
    NativeTask::Config * config = new NativeTask::Config();
    jsize len = jenv->GetArrayLength(configs);
    // 遍历键值对，每两个元素对应一个配置项
    for (jsize i = 0; i + 1 < len; i += 2) {
      jbyteArray key_obj = (jbyteArray)jenv->GetObjectArrayElement(configs, i);
      jbyteArray val_obj = (jbyteArray)jenv->GetObjectArrayElement(configs, i + 1);
      config->set(JNU_ByteArrayToString(jenv, key_obj), JNU_ByteArrayToString(jenv, val_obj));
    }

    NativeTask::BatchHandler * batchHandler = (NativeTask::BatchHandler *)((void*)handler);
    if (NULL == batchHandler) {
      JNU_ThrowByName(jenv, "java/lang/IllegalArgumentException", "BatchHandler is null");
      return;
    }
    // 获取Java侧输入直接缓冲区地址和容量
    jobject jinputBuffer = jenv->GetObjectField(processor, InputBufferFieldID);
    char * inputBufferAddr = NULL;
    uint32_t inputBufferCapacity = 0;
    if (NULL != jinputBuffer) {
      inputBufferAddr = (char*)(jenv->GetDirectBufferAddress(jinputBuffer));
      inputBufferCapacity = jenv->GetDirectBufferCapacity(jinputBuffer);
    }
    // 获取Java侧输出直接缓冲区地址和容量
    jobject joutputBuffer = jenv->GetObjectField(processor, OutputBufferFieldID);
    char * outputBufferAddr = NULL;
    uint32_t outputBufferCapacity = 0;
    if (NULL != joutputBuffer) {
      outputBufferAddr = (char*)(jenv->GetDirectBufferAddress(joutputBuffer));
      outputBufferCapacity = jenv->GetDirectBufferCapacity(joutputBuffer);
    }
    // 创建Java处理器全局引用，初始化处理器
    batchHandler->setProcessor(jenv->NewGlobalRef(processor));
    batchHandler->onSetup(config, inputBufferAddr, inputBufferCapacity, outputBufferAddr,
        outputBufferCapacity);
  // 异常处理：将Native异常转换为Java异常抛出
  } catch (NativeTask::UnsupportException & e) {
    JNU_ThrowByName(jenv, "java/lang/UnsupportedOperationException", e.what());
  } catch (NativeTask::OutOfMemoryException & e) {
    JNU_ThrowByName(jenv, "java/lang/OutOfMemoryError", e.what());
  } catch (NativeTask::IOException & e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  } catch (NativeTask::JavaException & e) {
    LOG("JavaException: %s", e.what());
    // Do nothing, let java side handle
  } catch (std::exception & e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  } catch (...) {
    JNU_ThrowByName(jenv, "java/io/IOException", "Unknown exception");
  }
}

/*
 * Class:     org_apache_hadoop_mapred_nativetask_NativeBatchProcessor
 * Method:    nativeProcessInput
 * Signature: (JI)V
 */
/**
 * @brief JNI方法：处理Java侧传入的批量输入数据
 * @param jenv JNI环境指针
 * @param processor Java侧NativeBatchProcessor对象
 * @param handler Native处理器指针地址
 * @param length 输入数据长度
 */
void JNICALL Java_org_apache_hadoop_mapred_nativetask_NativeBatchProcessor_nativeProcessInput(
    JNIEnv * jenv, jobject processor, jlong handler, jint length) {

  try {
    NativeTask::BatchHandler * batchHandler = (NativeTask::BatchHandler *)((void*)handler);
    if (NULL == batchHandler) {
      JNU_ThrowByName(jenv, "java/lang/IllegalArgumentException",
          "handler not instance of BatchHandler");
      return;
    }
    batchHandler->onInputData(length);
  } catch (NativeTask::UnsupportException & e) {
    JNU_ThrowByName(jenv, "java/lang/UnsupportedOperationException", e.what());
  } catch (NativeTask::OutOfMemoryException & e) {
    JNU_ThrowByName(jenv, "java/lang/OutOfMemoryError", e.what());
  } catch (NativeTask::IOException & e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  } catch (NativeTask::JavaException & e) {
    LOG("JavaException: %s", e.what());
    // Do nothing, let java side handle
  } catch (std::exception & e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  } catch (...) {
    JNU_ThrowByName(jenv, "java/io/IOException", "Unknown exception");
  }
}

/*
 * Class:     org_apache_hadoop_mapred_nativetask_NativeBatchProcessor
 * Method:    nativeFinish
 * Signature: (J)V
 */
/**
 * @brief JNI方法：通知Native侧处理完成
 * @param jenv JNI环境指针
 * @param processor Java侧NativeBatchProcessor对象
 * @param handler Native处理器指针地址
 */
void JNICALL Java_org_apache_hadoop_mapred_nativetask_NativeBatchProcessor_nativeFinish(
    JNIEnv * jenv, jobject processor, jlong handler) {
  try {
    NativeTask::BatchHandler * batchHandler = (NativeTask::BatchHandler *)((void*)handler);
    if (NULL == batchHandler) {
      JNU_ThrowByName(jenv, "java/lang/IllegalArgumentException",
          "handler not instance of BatchHandler");
      return;
    }
    batchHandler->onFinish();
  } catch (NativeTask::UnsupportException & e) {
    JNU_ThrowByName(jenv, "java/lang/UnsupportedOperationException", e.what());
  } catch (NativeTask::OutOfMemoryException & e) {
    JNU_ThrowByName(jenv, "java/lang/OutOfMemoryError", e.what());
  } catch (NativeTask::IOException & e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  } catch (NativeTask::JavaException & e) {
    LOG("JavaException: %s", e.what());
    // Do nothing, let java side handle
  } catch (std::exception & e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  } catch (...) {
    JNU_ThrowByName(jenv, "java/io/IOException", "Unknown exception");
  }
}

/**
 * @brief JNI方法：通知Native侧加载数据
 * @param jenv JNI环境指针
 * @param processor Java侧NativeBatchProcessor对象
 * @param handler Native处理器指针地址
 */
void JNICALL Java_org_apache_hadoop_mapred_nativetask_NativeBatchProcessor_nativeLoadData(
    JNIEnv * jenv, jobject processor, jlong handler) {
  try {
    NativeTask::BatchHandler * batchHandler = (NativeTask::BatchHandler *)((void*)handler);
    if (NULL == batchHandler) {
      JNU_ThrowByName(jenv, "java/lang/IllegalArgumentException",
          "handler not instance of BatchHandler");
      return;
    }
    batchHandler->onLoadData();
  } catch (NativeTask::UnsupportException & e) {
    JNU_ThrowByName(jenv, "java/lang/UnsupportedOperationException", e.what());
  } catch (NativeTask::OutOfMemoryException & e) {
    JNU_ThrowByName(jenv, "java/lang/OutOfMemoryError", e.what());
  } catch (NativeTask::IOException & e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  } catch (NativeTask::JavaException & e) {
    LOG("JavaException: %s", e.what());
    // Do nothing, let java side handle
  } catch (std::exception & e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  } catch (...) {
    JNU_ThrowByName(jenv, "java/io/IOException", "Unknown exception");
  }
}

/*
 * Class:     org_apache_hadoop_mapred_nativetask_NativeBatchProcessor
 * Method:    nativeCommand
 * Signature: (J[B)[B
 */
/**
 * @brief JNI方法：向Native处理器发送命令，返回处理结果
 * @param jenv JNI环境指针
 * @param processor Java侧NativeBatchProcessor
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
 * @file NativeRuntimeJniImpl.cc
 * @brief MapReduce原生任务运行时JNI实现，提供Java侧对原生运行时的调用接口
 * 
 * 该文件实现了NativeRuntime类所有JNI方法，是Java层MapReduce任务与C++原生任务
 * 运行时交互的入口，负责配置加载、原生对象创建销毁、压缩编解码支持查询等功能。
 */

#ifndef QUICK_BUILD
#include "org_apache_hadoop_mapred_nativetask_NativeRuntime.h"
#endif
#include "config.h"
#include "lib/commons.h"
#include "lib/jniutils.h"
#include "lib/NativeObjectFactory.h"

using namespace NativeTask;

///////////////////////////////////////////////////////////////
// NativeRuntime JNI methods
///////////////////////////////////////////////////////////////

/**
 * @brief 查询原生运行时是否支持指定压缩编解码器
 * @param jenv JNI环境指针
 * @param clazz NativeRuntime Java类对象
 * @param codec 压缩编解码器全类名字节数组
 * @return JNI_TRUE表示支持，JNI_FALSE表示不支持
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_mapred_nativetask_NativeRuntime_supportsCompressionCodec
  (JNIEnv *jenv, jclass clazz, jbyteArray codec) {
  // 将Java字节数组转换为C++字符串
  const std::string codecString = JNU_ByteArrayToString(jenv, codec);
  if ("org.apache.hadoop.io.compress.GzipCodec" == codecString) {
    return JNI_TRUE;
  } else if ("org.apache.hadoop.io.compress.Lz4Codec" == codecString) {
    return JNI_TRUE;
  } else if ("org.apache.hadoop.io.compress.SnappyCodec" == codecString) {
// 根据编译选项判断是否支持Snappy压缩
#if defined HADOOP_SNAPPY_LIBRARY
    return JNI_TRUE;
#else
    return JNI_FALSE;
#endif
  } else {
    return JNI_FALSE;
  }
}

/**
 * @brief 释放原生运行时全局资源
 * @param jenv JNI环境指针
 * @param nativeRuntimeClass NativeRuntime Java类对象
 */
JNIEXPORT void JNICALL Java_org_apache_hadoop_mapred_nativetask_NativeRuntime_JNIRelease(
    JNIEnv * jenv, jclass nativeRuntimeClass) {
  try {
    NativeTask::NativeObjectFactory::Release();
  } catch (NativeTask::UnsupportException & e) {
    // 将C++异常转换为Java异常抛出
    JNU_ThrowByName(jenv, "java/lang/UnsupportedOperationException", e.what());
  } catch (NativeTask::OutOfMemoryException & e) {
    JNU_ThrowByName(jenv, "java/lang/OutOfMemoryError", e.what());
  } catch (NativeTask::IOException & e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  } catch (NativeTask::JavaException & e) {
    LOG("[NativeRuntimeJniImpl] JavaException: %s", e.what());
    // 不主动抛出，交由Java侧处理
  } catch (std::exception & e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  } catch (...) {
    JNU_ThrowByName(jenv, "java/io/IOException", "[NativeRuntimeJniImpl] Unkown std::exception");
  }
}

/**
 * @brief 配置原生运行时，将Java侧配置加载到原生环境
 * @param jenv JNI环境指针
 * @param nativeRuntimeClass NativeRuntime Java类对象
 * @param configs 键值对格式的配置数组，每个元素为字节数组
 */
JNIEXPORT void JNICALL Java_org_apache_hadoop_mapred_nativetask_NativeRuntime_JNIConfigure(
    JNIEnv * jenv, jclass nativeRuntimeClass, jobjectArray configs) {
  try {
    // 获取原生运行时全局配置对象
    NativeTask::Config & config = NativeTask::NativeObjectFactory::GetConfig();
    // 获取配置数组长度
    jsize len = jenv->GetArrayLength(configs);
    // 遍历配置数组，两两一组为键值对
    for (jsize i = 0; i + 1 < len; i += 2) {
      // 获取配置键字节数组对象
      jbyteArray key_obj = (jbyteArray)jenv->GetObjectArrayElement(configs, i);
      // 获取配置值字节数组对象
      jbyteArray val_obj = (jbyteArray)jenv->GetObjectArrayElement(configs, i + 1);
      // 转换为字符串后存入原生配置对象
      config.set(JNU_ByteArrayToString(jenv, key_obj), JNU_ByteArrayToString(jenv, val_obj));
    }
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
    JNU_ThrowByName(jenv, "java/io/IOException", "Unkown std::exception");
  }
}

/**
 * @brief 根据类型创建自定义原生对象
 * @param jenv JNI环境指针
 * @param nativeRuntimeClass NativeRuntime Java类对象
 * @param clazz 原生对象类型名称字节数组
 * @return 原生对象指针转换为long值，Java侧保存该地址用于后续操作
 */
jlong JNICALL Java_org_apache_hadoop_mapred_nativetask_NativeRuntime_JNICreateNativeObject(
    JNIEnv * jenv, jclass nativeRuntimeClass, jbyteArray clazz) {
  try {
    std::string typeString = JNU_ByteArrayToString(jenv, clazz);
    return (jlong)(NativeTask::NativeObjectFactory::CreateObject(typeString));
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
  return 0;
}

/**
 * @brief 创建默认类型的原生对象
 * @param jenv JNI环境指针
 * @param nativeRuntimeClass NativeRuntime Java类对象
 * @param type 原生对象类型枚举名称字节数组
 * @return 原生对象指针转换为long值，Java侧保存该地址用于后续操作
 */
JNIEXPORT jlong JNICALL Java_org_apache_hadoop_mapred_nativetask_NativeRuntime_JNICreateDefaultNativeObject(
    JNIEnv * jenv, jclass nativeRuntimeClass, jbyteArray type) {
  try {
    std::string typeString = JNU_ByteArrayToString(jenv, type);
    // 将字符串类型名称转换为原生对象类型枚举
    NativeTask::NativeObjectType type = NativeTask::NativeObjectTypeFromString(typeString.c_str());
    return (jlong)(NativeTask::NativeObjectFactory::CreateDefaultObject(type));
  } catch (NativeTask::UnsupportException & e) {
    JNU_ThrowByName(jenv, "java/lang/UnsupportedOperationException", e.what());
  } catch (NativeTask::OutOfMemoryException & e) {
    JNU_ThrowByName(jenv, "java/lang/OutOfMemoryError", e.what());
  } catch (NativeTask::IOException & e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  } catch (NativeTask::JavaException & e) {
    LOG("[NativeRuntimeJniImpl] JavaException: %s", e.what());
    // Do nothing, let java side handle
  } catch (std::exception & e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  } catch (...) {
    JNU_ThrowByName(jenv, "java/io/IOException", "[NativeRuntimeJniImpl] Unknown exception");
  }
  return 0;
}

/**
 * @brief 释放指定原生对象
 * @param jenv JNI环境指针
 * @param nativeRuntimeClass NativeRuntime Java类对象
 * @param objectAddr 原生对象内存地址（Java侧传入）
 */
JNIEXPORT void JNICALL Java_org_apache_hadoop_mapred_nativetask_NativeRuntime_JNIReleaseNativeObject(
    JNIEnv * jenv, jclass nativeRuntimeClass, jlong objectAddr) {
  try {
    // 将地址转换为原生对象指针
    NativeTask::NativeObject * nobj = ((NativeTask::NativeObject *)objectAddr);
    // 检查地址合法性
    if (NULL == nobj) {
      JNU_ThrowByName(jenv, "java/lang/IllegalArgumentException",
          "Object addr not instance of NativeObject");
      return;
    }
    // 通过工厂释放对象
    NativeTask::NativeObjectFactory::ReleaseObject(nobj);
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
 * @brief 注册外部动态链接库模块，扩展原生对象工厂
 * @param jenv JNI环境指针
 * @param nativeRuntimeClass NativeRuntime Java类对象
 * @param modulePath 动态链接库文件路径
 * @param moduleName 模块名称
 * @return 0表示注册成功，非0表示注册失败
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_mapred_nativetask_NativeRuntime_JNIRegisterModule(
    JNIEnv * jenv, jclass nativeRuntimeClass, jbyteArray modulePath, jbyteArray moduleName) {
  try {
    std::string pathString = JNU_ByteArrayToString(jenv, modulePath);
    std::string nameString = JNU_ByteArrayToString(jenv, moduleName);
    // 调用工厂注册动态库
    if (NativeTask::NativeObjectFactory::RegisterLibrary(pathString, nameString)) {
      return 0;
    }
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
  return 1;
}

/**
 * @brief 获取任务状态更新数据，返回给Java侧
 * @param jenv JNI环境指针
 * @param nativeRuntimeClass NativeRuntime Java类对象
 * @return 状态数据字节数组，异常时返回NULL
 */
JNIEXPORT jbyteArray JNICALL Java_org_apache_hadoop_mapred_nativetask_NativeRuntime_JNIUpdateStatus(
    JNIEnv * jenv, jclass nativeRuntimeClass) {
  try {
    std::string statusData;
    // 从工厂获取最新任务状态更新
    NativeTask::NativeObjectFactory::GetTaskStatusUpdate(statusData);
    // 创建Java字节数组存储状态数据
    jbyteArray ret = jenv->NewByteArray(statusData.length());
    // 将C++字符串内容拷贝到Java数组
    jenv->SetByteArrayRegion(ret, 0, statusData.length(), (jbyte*)statusData.c_str());
    return ret;
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
  return NULL;
}
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
 * @file jniutils.cc
 * @brief MapReduce本地任务JNI工具类实现
 * 提供Java虚拟机获取、线程附着分离、异常抛出、类型转换等JNI基础能力，
 * 支撑MapReduce本地任务原生代码与Java层的交互
 */

#include "lib/commons.h"
#include "util/StringUtil.h"
#include "util/SyncUtils.h"
#include "lib/jniutils.h"

using namespace NativeTask;

/**
 * @brief 获取全局Java虚拟机实例
 * @return 全局JavaVM指针，若获取失败抛出异常
 * 采用双重检查锁实现单例模式，若已有JVM则返回，若无则尝试从已创建JVM中获取，仍无则创建新JVM
 */
JavaVM * JNU_GetJVM(void) {
  static JavaVM * gJVM = NULL;
  static Lock GJVMLock;
  // 已创建直接返回
  if (gJVM != NULL) {
    return gJVM;
  }
  // 加锁防止并发创建
  {
    ScopeLock<Lock> autolock(GJVMLock);
    // 双重检查，避免重复创建
    if (gJVM == NULL) {
      jint rv = 0;
      jint noVMs = 0;
      // 获取已创建的JavaVM
      rv = JNI_GetCreatedJavaVMs(&gJVM, 1, &noVMs);
      if (rv != 0) {
        THROW_EXCEPTION(NativeTask::HadoopException, "JNI_GetCreatedJavaVMs failed");
      }
      // 没有已创建的JVM则新建
      if (noVMs == 0) {
        // 从环境变量获取CLASSPATH
        char *hadoopClassPath = getenv("CLASSPATH");
        if (hadoopClassPath == NULL) {
          THROW_EXCEPTION(NativeTask::HadoopException, "Environment variable CLASSPATH not set!");
          return NULL;
        }
        // 构造类路径JVM参数
        const char *hadoopClassPathVMArg = "-Djava.class.path=";
        size_t optHadoopClassPathLen = strlen(hadoopClassPath) + strlen(hadoopClassPathVMArg) + 1;
        char *optHadoopClassPath = (char*)malloc(sizeof(char) * optHadoopClassPathLen);
        snprintf(optHadoopClassPath, optHadoopClassPathLen, "%s%s", hadoopClassPathVMArg,
            hadoopClassPath);
        // 初始化JVM启动参数
        int noArgs = 1;
        JavaVMOption options[noArgs];
        options[0].optionString = optHadoopClassPath;

        // 创建Java虚拟机
        JavaVMInitArgs vm_args;
        vm_args.version = JNI_VERSION_1_6;
        vm_args.options = options;
        vm_args.nOptions = noArgs;
        vm_args.ignoreUnrecognized = 1;
        JNIEnv * jenv;
        rv = JNI_CreateJavaVM(&gJVM, (void**)&jenv, &vm_args);
        if (rv != 0) {
          THROW_EXCEPTION(NativeTask::HadoopException, "JNI_CreateJavaVM failed");
          return NULL;
        }
        // 释放参数内存
        free(optHadoopClassPath);
      }
    }
  }
  return gJVM;
}

/**
 * @brief 获取当前线程关联的JNIEnv环境指针
 * @return 当前线程的JNIEnv指针，失败抛出异常
 * 若当前线程未附着到JVM，会自动执行附着操作
 */
JNIEnv* JNU_GetJNIEnv(void) {
  JNIEnv * env;
  jint rv = JNU_GetJVM()->AttachCurrentThread((void **)&env, NULL);
  if (rv != 0) {
    THROW_EXCEPTION(NativeTask::HadoopException, "Call to AttachCurrentThread failed");
  }
  return env;
}

/**
 * @brief 将当前线程附着到JVM
 * 无返回值，附着失败直接抛出异常
 */
void JNU_AttachCurrentThread() {
  JNU_GetJNIEnv();
}

/**
 * @brief 将当前线程从JVM分离
 * 分离失败直接抛出异常，避免资源泄漏
 */
void JNU_DetachCurrentThread() {
  jint rv = JNU_GetJVM()->DetachCurrentThread();
  if (rv != 0) {
    THROW_EXCEPTION(NativeTask::HadoopException, "Call to DetachCurrentThread failed");
  }
}

/**
 * @brief 通过类名向Java层抛出异常
 * @param jenv JNI环境指针
 * @param name 异常类全限定名
 * @param msg 异常消息文本
 */
void JNU_ThrowByName(JNIEnv *jenv, const char *name, const char *msg) {
  jclass cls = jenv->FindClass(name);
  if (cls != NULL) {
    jenv->ThrowNew(cls, msg);
  }
  // 释放本地引用
  jenv->DeleteLocalRef(cls);
}

/**
 * @brief 将Java字节数组转换为C++ std::string
 * @param jenv JNI环境指针
 * @param src Java字节数组对象
 * @return 转换后的C++字符串，输入为NULL返回空字符串
 */
std::string JNU_ByteArrayToString(JNIEnv * jenv, jbyteArray src) {
  if (NULL != src) {
    // 获取数组长度
    jsize len = jenv->GetArrayLength(src);
    // 分配对应长度字符串
    std::string ret(len, '\0');
    // 拷贝字节数据到C++字符串
    jenv->GetByteArrayRegion(src, 0, len, (jbyte*)ret.data());
    return ret;
  }
  return std::string();
}
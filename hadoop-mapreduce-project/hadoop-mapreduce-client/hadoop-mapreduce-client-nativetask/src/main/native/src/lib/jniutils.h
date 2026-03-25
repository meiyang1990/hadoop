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
 * @file jniutils.h
 * Hadoop MapReduce本地任务Native层JNI工具函数头文件
 * 提供Native代码与Java虚拟机交互的基础工具能力
 */

#ifndef JNIUTILS_H_
#define JNIUTILS_H_

#include <string>
#include <jni.h>

/**
 * 获取当前Java虚拟机实例，如果不存在则尝试创建新实例
 * @return JavaVM实例指针
 */
JavaVM * JNU_GetJVM(void);

/**
 * 获取当前线程对应的JNI环境对象
 * @return 当前线程的JNIEnv环境指针
 */
JNIEnv* JNU_GetJNIEnv(void);

/**
 * 将当前Native线程附着到Java虚拟机，效果等同于JNU_GetJNIEnv
 * 用于把Native创建的线程关联到Java虚拟机，获取JNI环境
 */
void JNU_AttachCurrentThread();

/**
 * 将当前线程从Java虚拟机分离
 * 当Native侧创建的线程此前调用过JNU_AttachCurrentThread后，
 * 需要在线程退出前调用本方法释放资源，避免内存泄漏
 */
void JNU_DetachCurrentThread();

/**
 * 抛出指定类型的Java异常
 * @param jenv 当前线程的JNI环境指针
 * @param name 要抛出的异常类全限定名
 * @param msg 异常描述信息
 */
void JNU_ThrowByName(JNIEnv *jenv, const char *name, const char *msg);

/**
 * 将Java字节数组转换为C++ std::string
 * @param jenv 当前线程的JNI环境指针
 * @param src Java侧输入字节数组
 * @return 转换后的C++字符串
 */
std::string JNU_ByteArrayToString(JNIEnv * jenv, jbyteArray src);

#endif /* JNIUTILS_H_ */
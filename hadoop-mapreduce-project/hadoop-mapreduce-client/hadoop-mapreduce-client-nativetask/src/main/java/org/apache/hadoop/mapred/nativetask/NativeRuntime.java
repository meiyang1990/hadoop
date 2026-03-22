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

package org.apache.hadoop.mapred.nativetask;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Task.TaskReporter;
import org.apache.hadoop.mapred.nativetask.util.ConfigUtil;
import org.apache.hadoop.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MapReduce原生任务运行时管理类，负责对接C++实现的原生任务执行环境
 * 核心职责：
 * 1. 加载原生任务JNI库，为Map、Reduce、输出收集器等组件创建原生处理句柄
 * 2. 将MapReduce作业配置传递给原生运行时环境
 * 3. 为原生代码提供Hadoop文件系统访问能力，让原生任务可以直接读写HDFS等存储
 */
@InterfaceAudience.Private
public class NativeRuntime {
  private static final Logger LOG =
      LoggerFactory.getLogger(NativeRuntime.class);
  // 标记原生库是否加载成功
  private static boolean nativeLibraryLoaded = false;

  // 保存当前作业配置，用于传递给原生运行时
  private static Configuration conf = new Configuration();

  static {
    try {
      // 加载nativetask动态链接库
      System.loadLibrary("nativetask");
      LOG.info("Nativetask JNI library loaded.");
      nativeLibraryLoaded = true;
    } catch (final Throwable t) {
      // 加载失败不终止，仅记录错误日志
      LOG.error("Failed to load nativetask JNI library with error: " + t);
      LOG.info("java.library.path=" + System.getProperty("java.library.path"));
      LOG.info("LD_LIBRARY_PATH=" + System.getenv("LD_LIBRARY_PATH"));
    }
  }

  /**
   * 检查原生库是否加载成功，若未加载则抛出运行时异常
   */
  private static void assertNativeLibraryLoaded() {
    if (!nativeLibraryLoaded) {
      throw new RuntimeException("Native runtime library not loaded");
    }
  }

  /**
   * 获取原生库是否加载成功的状态
   * @return 原生库加载成功返回true，否则返回false
   */
  public static boolean isNativeLibraryLoaded() {
    return nativeLibraryLoaded;
  }

  /**
   * 将MapReduce作业配置传递给原生运行时环境
   * @param jobConf MapReduce作业配置对象
   */
  public static void configure(Configuration jobConf) {
    assertNativeLibraryLoaded();
    conf = new Configuration(jobConf);
    conf.set(Constants.NATIVE_HADOOP_VERSION, VersionInfo.getVersion());
    // 调用JNI方法完成原生端配置
    JNIConfigure(ConfigUtil.toBytes(conf));
  }

  /**
   * 在原生运行空间创建原生对象，用于创建各类原生处理句柄
   * @param clazz 需要创建的原生对象类名
   * @return 原生对象的内存地址指针，创建失败返回0
   */
  public synchronized static long createNativeObject(String clazz) {
    assertNativeLibraryLoaded();
    final long ret = JNICreateNativeObject(clazz.getBytes(StandardCharsets.UTF_8));
    if (ret == 0) {
      LOG.warn("Can't create NativeObject for class " + clazz + ", probably not exist.");
    }
    return ret;
  }

  /**
   * 注册自定义原生动态链接库，加载其中的原生处理类
   * @param libraryName 自定义原生库路径
   * @param clazz 需要创建的原生对象类名
   * @return 原生对象的内存地址指针，创建失败返回0
   */
  public synchronized static long registerLibrary(String libraryName, String clazz) {
    assertNativeLibraryLoaded();
    final long ret = JNIRegisterModule(libraryName.getBytes(StandardCharsets.UTF_8),
                                       clazz.getBytes(StandardCharsets.UTF_8));
    if (ret != 0) {
      LOG.warn("Can't create NativeObject for class " + clazz + ", probably not exist.");
    }
    return ret;
  }

  /**
   * 释放原生空间中的原生对象，回收内存，用于销毁原生处理句柄
   * @param addr 需要释放的原生对象内存地址指针
   */
  public synchronized static void releaseNativeObject(long addr) {
    assertNativeLibraryLoaded();
    JNIReleaseNativeObject(addr);
  }

  /**
   * 从原生运行空间拉取状态更新，并同步给MapReduce任务报告器
   * 包括进度、状态信息和计数器增量更新
   * @param reporter MapReduce任务状态报告器
   * @throws IOException 反序列化状态数据时可能抛出IO异常
   */
  public static void reportStatus(TaskReporter reporter) throws IOException {
    assertNativeLibraryLoaded();
    synchronized (reporter) {
      // 从原生端获取序列化后的状态字节数组
      final byte[] statusBytes = JNIUpdateStatus();
      final DataInputBuffer ib = new DataInputBuffer();
      ib.reset(statusBytes, statusBytes.length);
      
      // 读取并更新任务进度
      final FloatWritable progress = new FloatWritable();
      progress.readFields(ib);
      reporter.setProgress(progress.get());
      
      // 读取并更新任务状态信息
      final Text status = new Text();
      status.readFields(ib);
      if (status.getLength() > 0) {
        reporter.setStatus(status.toString());
      }
      
      // 读取需要更新的计数器数量
      final IntWritable numCounters = new IntWritable();
      numCounters.readFields(ib);
      if (numCounters.get() == 0) {
        return;
      }
      
      // 遍历更新所有计数器增量
      final Text group = new Text();
      final Text name = new Text();
      final LongWritable amount = new LongWritable();
      for (int i = 0; i < numCounters.get(); i++) {
        group.readFields(ib);
        name.readFields(ib);
        amount.readFields(ib);
        reporter.incrCounter(group.toString(), name.toString(), amount.get());
      }
    }
  }


  /*******************************************************
   *** 以下为JNI方法声明，对应C++实现的原生逻辑
   ********************************************************/

  /**
   * 检查原生端是否支持指定压缩编解码器
   * @param codec 编解码器名称字节数组
   * @return 支持返回true，否则返回false
   */
  public native static boolean supportsCompressionCodec(byte[] codec);

  /**
   * 原生端配置方法，传入序列化后的Hadoop配置
   * @param configs 序列化后的配置二维字节数组
   */
  private native static void JNIConfigure(byte[][] configs);

  /**
   * 在原生空间创建指定类名的原生对象
   * @param clazz 类名字节数组
   * @return 原生对象地址指针
   */
  private native static long JNICreateNativeObject(byte[] clazz);

  /**
   * 为指定类型创建默认原生对象，已废弃
   * @param type 类型名称字节数组
   * @return 原生对象地址指针
   */
  @Deprecated
  private native static long JNICreateDefaultNativeObject(byte[] type);

  /**
   * 释放指定地址的原生对象
   * @param addr 原生对象地址指针
   */
  private native static void JNIReleaseNativeObject(long addr);

  /**
   * 从原生端获取更新后的状态数据，序列化格式为：
   *  progress:float 进度
   *  status:Text 状态信息
   *  number: int 计数器数量
   *  Counters: 数组，每个元素为 [group:Text, name:Text, incrCount:Long]
   * @return 序列化后的状态字节数组
   */
  private native static byte[] JNIUpdateStatus();

  /**
   * 未使用
   */
  private native static void JNIRelease();

  /**
   * 注册原生模块，加载自定义原生库中的类
   * @param path 原生库路径字节数组
   * @param name 类名字节数组
   * @return 0表示成功，非0表示失败
   */
  private native static int JNIRegisterModule(byte[] path, byte[] name);
}
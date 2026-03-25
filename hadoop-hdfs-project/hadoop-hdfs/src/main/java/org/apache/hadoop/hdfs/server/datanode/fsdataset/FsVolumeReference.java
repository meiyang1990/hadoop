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
package org.apache.hadoop.hdfs.server.datanode.fsdataset;

import java.io.Closeable;
import java.io.IOException;

/**
 * FsVolume的引用计数包装器，实现为可自动关闭资源，用于安全管理磁盘卷生命周期。
 * 构造时会将卷的引用计数加1，在{@link #close()}方法中将引用计数减1。
 * 配合try-with-resources语法使用，可保证引用计数正确释放，避免资源泄漏。
 *
 * <pre>
 *  {@code
 *    try (FsVolumeReference ref = volume.obtainReference()) {
 *      // 在卷上执行IO操作
 *      volume.createRwb(...);
 *      ...
 *    }
 *  }
 * </pre>
 */
public interface FsVolumeReference extends Closeable {
  /**
   * 减少对应卷的引用计数，释放该引用。
   * @throws IOException 本方法不会实际抛出IOException
   */
  @Override
  void close() throws IOException;

  /**
   * 获取该引用对应的底层卷对象，如果引用已经被释放则返回null。
   * @return 底层FsVolumeSpi实例
   */
  FsVolumeSpi getVolume();
}
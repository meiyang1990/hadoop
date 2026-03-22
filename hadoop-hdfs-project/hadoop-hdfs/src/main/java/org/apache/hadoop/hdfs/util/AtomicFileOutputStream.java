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
package org.apache.hadoop.hdfs.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.nio.file.Files;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.io.nativeio.NativeIOException;

/**
 * 保证文件写入的原子输出流，仅当写入完整完成后才会将最终文件展示给其他进程可见，实现原子文件更新。
 * 写入过程中使用.tmp临时文件存储，关闭流时会先刷盘然后将临时文件重命名覆盖目标文件。
 * <b>注意：在Windows平台无法实现原子替换，会先删除原文件再移动临时文件。
 */
public class AtomicFileOutputStream extends FilterOutputStream {

  // 临时文件扩展名
  private static final String TMP_EXTENSION = ".tmp";
  
  private final static Logger LOG = LoggerFactory.getLogger(
      AtomicFileOutputStream.class);
  
  // 最终目标文件
  private final File origFile;
  // 写入过程使用的临时文件
  private final File tmpFile;
  
  /**
   * 构造原子文件输出流，写入指定目标文件
   * @param f 最终要写入的目标文件
   * @throws FileNotFoundException 如果父目录不存在或无法创建临时文件
   */
  public AtomicFileOutputStream(File f) throws FileNotFoundException {
    // Code unfortunately must be duplicated below since we can't assign anything
    // before calling super
    super(new FileOutputStream(new File(f.getParentFile(), f.getName() + TMP_EXTENSION)));
    origFile = f.getAbsoluteFile();
    tmpFile = new File(f.getParentFile(), f.getName() + TMP_EXTENSION).getAbsoluteFile();
  }

  @Override
  /**
   * 关闭原子输出流，完成数据刷盘和原子提交
   * @throws IOException 如果刷盘、关闭、重命名过程发生IO异常
   */
  public void close() throws IOException {
    boolean triedToClose = false, success = false;
    try {
      // 刷新输出流缓冲区
      flush();
      // 强制将所有数据和元数据刷写到磁盘
      ((FileOutputStream)out).getChannel().force(true);

      triedToClose = true;
      // 关闭底层输出流
      super.close();
      success = true;
    } finally {
      if (success) {
        // 尝试将临时文件重命名为目标文件
        boolean renamed = tmpFile.renameTo(origFile);
        if (!renamed) {
          // Windows平台renameTo无法覆盖已存在文件，需要特殊处理
          if (origFile.exists()) {
            try {
              // 删除已存在的原文件
              Files.delete(origFile.toPath());
            } catch (IOException e) {
              throw new IOException("Could not delete original file " + origFile, e);
            }
          }
          try {
            // 使用NativeIO重命名，保证跨平台正确性
            NativeIO.renameTo(tmpFile, origFile);
          } catch (NativeIOException e) {
            throw new IOException("Could not rename temporary file " + tmpFile
              + " to " + origFile + " due to failure in native rename. "
              + e.toString());
          }
        }
      } else {
        if (!triedToClose) {
          // 刷新阶段失败，主动关闭流避免文件描述符泄漏
          IOUtils.closeStream(out);
        }
        // 关闭失败，删除临时文件清理
        if (!tmpFile.delete()) {
          LOG.warn("Unable to delete tmp file " + tmpFile);
        }
      }
    }
  }

  /**
   * 终止写入操作，关闭流并清理临时文件，不提交写入结果到目标文件
   * 用于写入失败时的回滚清理
   */
  public void abort() {
    try {
      super.close();
    } catch (IOException ioe) {
      LOG.warn("Unable to abort file " + tmpFile, ioe);
    }
    // 删除临时文件清理残留
    if (!tmpFile.delete()) {
      LOG.warn("Unable to delete tmp file during abort " + tmpFile);
    }
  }

}
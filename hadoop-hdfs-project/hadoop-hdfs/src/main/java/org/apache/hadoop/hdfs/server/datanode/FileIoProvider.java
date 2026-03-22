// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.HardLink;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.io.nativeio.NativeIOException;
import org.apache.hadoop.net.SocketOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.Flushable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.apache.hadoop.hdfs.server.datanode.FileIoProvider.OPERATION.*;

/**
 * 文件IO提供者抽象类，封装DataNode所有文件IO操作，在每次IO操作前后插入性能统计和故障注入钩子。
 * 可通过配置开启性能采样和故障注入功能，默认均关闭。
 * 多数方法接受可选FsVolumeSpi参数用于插桩统计和日志记录，保留多版本move/rename/list方法是为了兼容现有代码行为。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class FileIoProvider {
  public static final Logger LOG = LoggerFactory.getLogger(
      FileIoProvider.class);

  // 性能统计事件钩子实例
  private final ProfilingFileIoEvents profilingEventHook;
  // 故障注入事件钩子实例
  private final FaultInjectorFileIoEvents faultInjectorEventHook;
  // 所属DataNode实例，用于IO错误后的磁盘检查回调
  private final DataNode datanode;

  // int类型占4字节，用于单字节读写时的统计
  private static final int LEN_INT = 4;

  /**
   * 构造FileIoProvider实例，根据配置初始化性能统计和故障注入钩子。
   * @param conf Hadoop配置对象，可为null，为null时所有事件钩子为空操作
   * @param datanode 所属DataNode实例，用于IO错误触发异步磁盘检查
   */
  public FileIoProvider(@Nullable Configuration conf,
                        final DataNode datanode) {
    profilingEventHook = new ProfilingFileIoEvents(conf);
    faultInjectorEventHook = new FaultInjectorFileIoEvents(conf);
    this.datanode = datanode;
  }

  /**
   * 文件IO操作类型枚举，传递给IO钩子以实现针对不同操作的定制行为。
   */
  public enum OPERATION {
    OPEN,
    EXISTS,
    LIST,
    DELETE,
    MOVE,
    MKDIRS,
    TRANSFER,
    SYNC,
    FADVISE,
    READ,
    WRITE,
    FLUSH,
    NATIVE_COPY
  }

  /**
   * 对Flushable对象执行flush操作，插入IO事件钩子。
   * @param  volume 目标存储卷，不可用则传null
   * @throws IOException flush操作抛出的异常
   */
  public void flush(
      @Nullable FsVolumeSpi volume, Flushable f) throws IOException {
    final long begin = profilingEventHook.beforeFileIo(volume, FLUSH, 0);
    try {
      faultInjectorEventHook.beforeFileIo(volume, FLUSH, 0);
      f.flush();
      profilingEventHook.afterFileIo(volume, FLUSH, begin, 0);
    } catch (Exception e) {
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * 对FileOutputStream执行sync落盘操作，插入IO事件钩子。
   * @param  volume 目标存储卷，不可用则传null
   * @throws IOException sync操作抛出的异常
   */
  public void sync(
      @Nullable FsVolumeSpi volume, FileOutputStream fos) throws IOException {
    final long begin = profilingEventHook.beforeFileIo(volume, SYNC, 0);
    try {
      faultInjectorEventHook.beforeFileIo(volume, SYNC, 0);
      IOUtils.fsync(fos.getChannel(), false);
      profilingEventHook.afterFileIo(volume, SYNC, begin, 0);
    } catch (Exception e) {
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * 对目录执行同步操作，将目录变更持久化到存储设备，插入IO事件钩子。
   * @throws IOException 同步操作抛出的异常
   */
  public void dirSync(@Nullable FsVolumeSpi volume, File dir)
      throws IOException {
    final long begin = profilingEventHook.beforeFileIo(volume, SYNC, 0);
    try {
      faultInjectorEventHook.beforeFileIo(volume, SYNC, 0);
      IOUtils.fsync(dir);
      profilingEventHook.afterFileIo(volume, SYNC, begin, 0);
    } catch (Exception e) {
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * 对指定文件描述符执行sync_file_range系统调用，插入IO事件钩子。
   * @param  volume 目标存储卷，不可用则传null
   * @throws NativeIOException 同步操作抛出的原生IO异常
   */
  public void syncFileRange(
      @Nullable FsVolumeSpi volume, FileDescriptor outFd,
      long offset, long numBytes, int flags) throws NativeIOException {
    final long begin = profilingEventHook.beforeFileIo(volume, SYNC, 0);
    try {
      faultInjectorEventHook.beforeFileIo(volume, SYNC, 0);
      NativeIO.POSIX.syncFileRangeIfPossible(outFd, offset, numBytes, flags);
      profilingEventHook.afterFileIo(volume, SYNC, begin, 0);
    } catch (Exception e) {
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * 对指定文件描述符执行posix_fadvise系统调用，插入IO事件钩子。
   * @param  volume 目标存储卷，不可用则传null
   * @throws NativeIOException posix_fadvise操作抛出的原生IO异常
   */
  public void posixFadvise(
      @Nullable FsVolumeSpi volume, String identifier, FileDescriptor outFd,
      long offset, long length, int flags) throws NativeIOException {
    final long begin = profilingEventHook.beforeMetadataOp(volume, FADVISE);
    try {
      faultInjectorEventHook.beforeMetadataOp(volume, FADVISE);
      NativeIO.POSIX.getCacheManipulator().posixFadviseIfPossible(
          identifier, outFd, offset, length, flags);
      profilingEventHook.afterMetadataOp(volume, FADVISE, begin);
    } catch (Exception e) {
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * 删除指定文件，插入元数据操作事件钩子。
   * @param volume 目标存储卷，不可用则传null
   * @param f 待删除文件
   * @return 删除成功返回true，否则返回false
   */
  public boolean delete(@Nullable FsVolumeSpi volume, File f) {
    final long begin = profilingEventHook.beforeMetadataOp(volume, DELETE);
    try {
      faultInjectorEventHook.beforeMetadataOp(volume, DELETE);
      boolean deleted = f.delete();
      profilingEventHook.afterMetadataOp(volume, DELETE, begin);
      return deleted;
    } catch (Exception e) {
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * 删除指定文件，删除前先检查文件是否存在，插入元数据操作事件钩子。
   * @param volume 目标存储卷，不可用则传null
   * @param f 待删除文件
   * @return 文件不存在或删除成功返回true，删除失败返回false
   */
  public boolean deleteWithExistsCheck(@Nullable FsVolumeSpi volume, File f) {
    final long begin = profilingEventHook.beforeMetadataOp(volume, DELETE);
    try {
      faultInjectorEventHook.beforeMetadataOp(volume, DELETE);
      boolean deleted = !f.exists() || f.delete();
      profilingEventHook.afterMetadataOp(volume, DELETE, begin);
      if (!deleted) {
        LOG.warn("Failed to delete file {}", f);
      }
      return deleted;
    } catch (Exception e) {
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * 将FileChannel中的数据全量传输到SocketOutputStream，插入IO事件钩子。
   * @param volume 目标存储卷，不可用则传null
   * @param sockOut 目标Socket输出流
   * @param fileCh 源文件通道
   * @param position 传输起始位置
   * @param count 传输字节数
   * @param waitTime 输出参数，返回socket等待可写的纳秒数
   * @param transferTime 输出参数，返回实际传输数据的纳秒数
   * @throws IOException 传输过程中抛出的异常
   */
  public void transferToSocketFully(
      @Nullable FsVolumeSpi volume, SocketOutputStream sockOut,
      FileChannel fileCh, long position, int count,
      LongWritable waitTime, LongWritable transferTime) throws IOException {
    final long begin = profilingEventHook.beforeFileIo(volume, TRANSFER, count);
    try {
      faultInjectorEventHook.beforeFileIo(volume, TRANSFER, count);
      sockOut.transferToFully(fileCh, position, count,
          waitTime, transferTime);
      profilingEventHook.afterFileIo(volume, TRANSFER, begin, count);
    } catch (Exception e) {
      // 管道破裂、连接重置属于客户端正常断开，不触发磁盘错误检查
      String em = e.getMessage();
      if (em != null) {
        if (!em.startsWith("Broken pipe")
            && !em.startsWith("Connection reset")) {
          onFailure(volume, begin);
        }
      } else {
        onFailure(volume, begin);
      }
      throw e;
    }
  }

  /**
   * 创建新文件，插入元数据操作事件钩子。
   * @param volume 目标存储卷，不可用则传null
   * @param f 待创建文件
   * @return 文件不存在且创建成功返回true，文件已存在返回false
   * @throws IOException 创建过程抛出的异常
   */
  public boolean createFile(
      @Nullable FsVolumeSpi volume, File f) throws IOException {
    final long begin = profilingEventHook.beforeMetadataOp(volume, OPEN);
    try {
      faultInjectorEventHook.beforeMetadataOp(volume, OPEN);
      boolean created = f.createNewFile();
      profilingEventHook.afterMetadataOp(volume, OPEN, begin);
      return created;
    } catch (Exception e) {
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * 创建包装过的FileInputStream，对read操作插入IO事件钩子。
   * @param volume 目标存储卷，不可用则传null
   * @param f 待打开文件
   * @return 包装后的FileInputStream实例
   * @throws  FileNotFoundException 文件不存在抛出异常
   */
  public FileInputStream getFileInputStream(
      @Nullable FsVolumeSpi volume, File f) throws FileNotFoundException {
    final long begin = profilingEventHook.beforeMetadataOp(volume, OPEN);
    FileInputStream fis = null;
    try {
      faultInjectorEventHook.beforeMetadataOp(volume, OPEN);
      fis = new WrappedFileInputStream(volume, f);
      profilingEventHook.afterMetadataOp(volume, OPEN, begin);
      return fis;
    } catch(Exception e) {
      IOUtils.closeStream(fis);
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * 创建包装过的FileOutputStream，对write操作插入IO事件钩子。
   * @param volume 目标存储卷，不可用则传null
   * @param f 待打开文件
   * @param append 是否追加写入
   * @return 包装后的FileOutputStream实例
   * @throws FileNotFoundException 文件不存在抛出异常
   */
  public FileOutputStream getFileOutputStream(
      @Nullable FsVolumeSpi volume, File f,
      boolean append) throws FileNotFoundException {
    final long begin = profilingEventHook.beforeMetadataOp(volume, OPEN);
    FileOutputStream fos = null;
    try {
      faultInjectorEventHook.beforeMetadataOp(volume, OPEN);
      fos = new WrappedFileOutputStream(volume, f, append);
      profilingEventHook.afterMetadataOp(volume, OPEN, begin);
      return fos;
    } catch(Exception e) {
      IOUtils.closeStream(fos);
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * 创建包装过的覆盖写入FileOutputStream，对write操作插入IO事件钩子。
   * @param volume 目标存储卷，不可用则传null
   * @param f 待打开文件
   * @return 包装后的FileOutputStream实例
   * @throws  FileNotFoundException 文件不存在抛出异常
   */
  public FileOutputStream getFileOutputStream(
      @Nullable FsVolumeSpi volume, File f) throws FileNotFoundException {
    return getFileOutputStream(volume, f, false);
  }

  /**
   * 基于文件描述符创建包装过的FileOutputStream，对write操作插入IO事件钩子。
   * @param volume 目标存储卷，不可用则传null
   * @param fd 文件描述符
   * @return 包装后的FileOutputStream实例
   */
  public FileOutputStream getFileOutputStream(
      @Nullable FsVolumeSpi volume, FileDescriptor fd) {
    return new WrappedFileOutputStream(volume, fd);
  }

  /**
   * 获取支持共享删除的FileInputStream，对read操作插入IO事件钩子。
   * @param volume 目标存储卷，不可用则传null
   * @param f 待打开文件
   * @param offset 文件指针起始偏移量
   * @return 包装后的FileInputStream实例
   * @throws FileNotFoundException 文件不存在抛出异常
   */
  public FileInputStream getShareDeleteFileInputStream(
      @Nullable FsVolumeSpi volume, File f,
      long offset) throws IOException {
    final long begin = profilingEventHook.beforeMetadataOp(volume, OPEN);
    FileInputStream fis = null;
    try {
      faultInjectorEventHook.beforeMetadataOp(volume, OPEN);
      fis = new WrappedFileInputStream(volume,
          NativeIO.getShareDeleteFileDescriptor(f, offset));
      profilingEventHook.afterMetadataOp(volume, OPEN, begin);
      return fis;
    } catch(Exception e) {
      IOUtils.closeStream(fis);
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * 打开文件并定位到指定偏移量，返回包装过的FileInputStream，对read操作插入IO事件钩子。
   * @param volume 目标存储卷，不可用则传null
   * @param f 待打开文件
   * @param offset 文件指针起始偏移量
   * @throws FileNotFoundException 文件不存在抛出异常
   */
  public FileInputStream openAndSeek(
      @Nullable FsVolumeSpi volume, File f, long offset) throws IOException {
    final long begin = profilingEventHook.beforeMetadataOp(volume, OPEN);
    FileInputStream fis = null;
    try {
      faultInjectorEventHook.beforeMetadataOp(volume, OPEN);
      fis = new WrappedFileInputStream(volume,
          FsDatasetUtil.openAndSeek(f, offset));
      profilingEventHook.afterMetadataOp(volume, OPEN, begin);
      return fis;
    } catch(Exception e) {
      IOUtils.closeStream(fis);
      onFailure(volume
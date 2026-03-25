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
package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IOUtils;

import org.apache.hadoop.mapred.IFileInputStream;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.MapOutputFile;

import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.security.IntermediateEncryptedStream;
import org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl.CompressAwarePath;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * Map输出结果写入磁盘的实现类，Reduce端Shuffle阶段将Map结果落地到本地磁盘
 * 当Map输出大小超过内存阈值时使用该实现，将结果暂存到本地磁盘等待后续合并
 * @param <K> 键类型
 * @param <V> 值类型
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class OnDiskMapOutput<K, V> extends IFileWrappedMapOutput<K, V> {
  private static final Logger LOG =
      LoggerFactory.getLogger(OnDiskMapOutput.class);
  // 存储文件所在的文件系统实例
  private final FileSystem fs;
  // 临时输出文件路径，写入完成后会重命名为正式路径
  private final Path tmpOutputPath;
  // 正式输出文件路径
  private final Path outputPath;
  // 输出文件流，用于写入Map输出数据
  private final OutputStream disk; 
  // 压缩后的Map输出总大小
  private long compressedSize;

  /**
   * @deprecated 保留向后兼容的旧构造方法，使用新构造方法替代
   * 构造OnDiskMapOutput实例，负责将Map输出写入本地磁盘
   * @param mapId Map任务尝试ID
   * @param reduceId Reduce任务尝试ID
   * @param merger 合并管理器实例
   * @param size Map输出大小
   * @param conf 作业配置
   * @param mapOutputFile Map输出文件工具
   * @param fetcher 拉取器编号
   * @param primaryMapOutput 是否是主Map输出
   * @throws IOException 初始化或创建文件失败时抛出
   */
  @Deprecated
  public OnDiskMapOutput(TaskAttemptID mapId, TaskAttemptID reduceId,
                         MergeManagerImpl<K,V> merger, long size,
                         JobConf conf,
                         MapOutputFile mapOutputFile,
                         int fetcher, boolean primaryMapOutput)
      throws IOException {
    this(mapId, merger, size, conf, fetcher,
        primaryMapOutput, FileSystem.getLocal(conf).getRaw(),
        mapOutputFile.getInputFileForWrite(mapId.getTaskID(), size));
  }

  /**
   * @deprecated 保留向后兼容的旧构造方法，使用新构造方法替代
   * 构造OnDiskMapOutput实例，指定文件系统和输出路径
   * @param mapId Map任务尝试ID
   * @param reduceId Reduce任务尝试ID
   * @param merger 合并管理器实例
   * @param size Map输出大小
   * @param conf 作业配置
   * @param mapOutputFile Map输出文件工具
   * @param fetcher 拉取器编号
   * @param primaryMapOutput 是否是主Map输出
   * @param fs 文件系统实例
   * @param outputPath 正式输出路径
   * @throws IOException 初始化或创建文件失败时抛出
   */
  @Deprecated
  OnDiskMapOutput(TaskAttemptID mapId, TaskAttemptID reduceId,
                         MergeManagerImpl<K,V> merger, long size,
                         JobConf conf,
                         MapOutputFile mapOutputFile,
                         int fetcher, boolean primaryMapOutput,
                         FileSystem fs, Path outputPath) throws IOException {
    this(mapId, merger, size, conf, fetcher, primaryMapOutput, fs, outputPath);
  }

  /**
   * 构造OnDiskMapOutput实例，创建临时输出文件并打开输出流
   * @param mapId Map任务尝试ID
   * @param merger 合并管理器实例
   * @param size Map输出大小
   * @param conf 作业配置
   * @param fetcher 拉取器编号
   * @param primaryMapOutput 是否是主Map输出
   * @param fs 文件系统实例
   * @param outputPath 正式输出路径
   * @throws IOException 创建文件或打开流失败时抛出
   */
  OnDiskMapOutput(TaskAttemptID mapId,
                  MergeManagerImpl<K, V> merger, long size,
                  JobConf conf,
                  int fetcher, boolean primaryMapOutput,
                  FileSystem fs, Path outputPath) throws IOException {
    super(conf, merger, mapId, size, primaryMapOutput);
    this.fs = fs;
    this.outputPath = outputPath;
    tmpOutputPath = getTempPath(outputPath, fetcher);
    disk = IntermediateEncryptedStream.wrapIfNecessary(conf,
        fs.create(tmpOutputPath), tmpOutputPath);
  }

  /**
   * 生成临时文件路径，在正式路径后添加拉取器编号作为后缀
   * @param outPath 正式输出路径
   * @param fetcher 拉取器编号
   * @return 临时文件路径
   */
  @VisibleForTesting
  static Path getTempPath(Path outPath, int fetcher) {
    return outPath.suffix(String.valueOf(fetcher));
  }

  /**
   * 执行Shuffle拷贝，将Map输出从输入流写入本地磁盘临时文件
   * @param host Map输出所在主机信息
   * @param input 输入流，来自远程Map输出的压缩数据
   * @param compressedLength 压缩后的数据总长度
   * @param decompressedLength 解压后的数据总长度
   * @param metrics Shuffle阶段指标统计
   * @param reporter 任务进度汇报器
   * @throws IOException 读取或写入数据失败、数据不完整时抛出
   */
  @Override
  protected void doShuffle(MapHost host, IFileInputStream input,
                      long compressedLength, long decompressedLength,
                      ShuffleClientMetrics metrics,
                      Reporter reporter) throws IOException {
    // 剩余待拷贝字节数
    long bytesLeft = compressedLength;
    try {
      // 单次读取缓冲区大小64KB
      final int BYTES_TO_READ = 64 * 1024;
      byte[] buf = new byte[BYTES_TO_READ];
      // 循环读取直到所有数据写入磁盘
      while (bytesLeft > 0) {
        // 从输入流读取带校验的数据，最多读取剩余字节数或缓冲区大小
        int n = input.readWithChecksum(buf, 0,
                                      (int) Math.min(bytesLeft, BYTES_TO_READ));
        // 提前读到流尾，抛出异常
        if (n < 0) {
          throw new IOException("read past end of stream reading " + 
                                getMapId());
        }
        // 将读取到的数据写入磁盘临时文件
        disk.write(buf, 0, n);
        // 更新剩余待拷贝字节数
        bytesLeft -= n;
        // 统计输入字节数指标
        metrics.inputBytes(n);
        // 汇报任务进度，防止被JobTracker判定为超时
        reporter.progress();
      }

      LOG.info("Read " + (compressedLength - bytesLeft) + 
               " bytes from map-output for " + getMapId());
      // 写入完成关闭磁盘流
      disk.close();
    } catch (IOException ioe) {
      // 异常时清理关闭流
      IOUtils.cleanupWithLogger(LOG, disk);
      // 抛出异常让上层处理
      throw ioe;
    }

    // 完整性校验，确认所有数据都已接收
    if (bytesLeft != 0) {
      throw new IOException("Incomplete map output received for " +
                            getMapId() + " from " +
                            host.getHostName() + " (" + 
                            bytesLeft + " bytes missing of " + 
                            compressedLength + ")");
    }
    // 保存压缩后大小，后续合并使用
    this.compressedSize = compressedLength;
  }

  /**
   * 提交输出，将临时文件重命名为正式文件，通知合并管理器添加该磁盘文件
   * @throws IOException 重命名文件失败时抛出
   */
  @Override
  public void commit() throws IOException {
    // 临时文件重命名为正式输出路径
    fs.rename(tmpOutputPath, outputPath);
    // 创建带压缩信息的路径包装对象
    CompressAwarePath compressAwarePath = new CompressAwarePath(outputPath,
        getSize(), this.compressedSize);
    // 通知合并管理器该磁盘文件已就绪，可以参与合并
    getMerger().closeOnDiskFile(compressAwarePath);
  }
  
  /**
   * 中止输出，清理已创建的临时文件
   */
  @Override
  public void abort() {
    try {
      // 删除临时文件
      fs.delete(tmpOutputPath, false);
    } catch (IOException ie) {
      LOG.info("failure to clean up " + tmpOutputPath, ie);
    }
  }

  /**
   * 获取输出类型描述
   * @return 输出类型描述字符串
   */
  @Override
  public String getDescription() {
    return "DISK";
  }

}
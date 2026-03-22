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
package org.apache.hadoop.mapreduce.checkpoint;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 基于Hadoop文件系统实现的检查点服务，为MapReduce任务提供检查点持久化能力
 * 将检查点数据存储在HDFS文件系统中，支持创建、提交、读取、删除检查点操作
 */
public class FSCheckpointService implements CheckpointService {

  private final Path base;
  private final FileSystem fs;
  private final CheckpointNamingService namingPolicy;
  private final short replication;

  /**
   * 构造基于文件系统的检查点服务实例
   * @param fs 用于存储检查点的文件系统实例
   * @param base 检查点存储的基础路径
   * @param namingPolicy 检查点文件命名策略
   * @param replication 检查点文件的副本数
   */
  public FSCheckpointService(FileSystem fs, Path base,
      CheckpointNamingService namingPolicy, short replication) {
    this.fs = fs;
    this.base = base;
    this.namingPolicy = namingPolicy;
    this.replication = replication;
  }

  /**
   * 创建一个新的检查点写入通道，准备写入检查点数据
   * @return 检查点写入通道
   * @throws IOException 创建过程中IO异常
   */
  public CheckpointWriteChannel create()
      throws IOException {

    // 生成新的检查点文件名
    String name = namingPolicy.getNewName();

    Path p = new Path(name);
    // 检查路径合法性，不允许使用绝对路径
    if (p.isUriPathAbsolute()) {
      throw new IOException("Checkpoint cannot be an absolute path");
    }
    // 拼接完整路径后创建内部通道
    return createInternal(new Path(base, p));
  }

  /**
   * 内部方法，创建检查点写入通道核心逻辑
   * @param name 检查点最终存储路径
   * @return 检查点写入通道
   * @throws IOException 创建过程中IO异常
   */
  CheckpointWriteChannel createInternal(Path name) throws IOException {

    // 创建临时文件，文件已存在则创建失败
    return new FSCheckpointWriteChannel(name, fs.create(tmpfile(name),
          replication));
  }

  /**
   * 基于文件系统的检查点写入通道实现，封装对检查点文件的写入操作
   */
  private static class FSCheckpointWriteChannel
      implements CheckpointWriteChannel {
    private boolean isOpen = true;
    private final Path finalDst;
    private final WritableByteChannel out;

    /**
     * 构造检查点写入通道实例
     * @param finalDst 检查点最终目标路径
     * @param out 检查点文件输出流
     */
    FSCheckpointWriteChannel(Path finalDst, FSDataOutputStream out) {
      this.finalDst = finalDst;
      this.out = Channels.newChannel(out);
    }

    @Override
    public int write(ByteBuffer b) throws IOException {
      return out.write(b);
    }

    /**
     * 获取检查点最终目标路径
     * @return 最终路径对象
     */
    public Path getDestination() {
      return finalDst;
    }

    @Override
    public void close() throws IOException {
      isOpen=false;
      out.close();
    }

    @Override
    public boolean isOpen() {
      return isOpen;
    }

  }

  @Override
  /**
   * 根据检查点ID打开已提交的检查点，返回读取通道
   * @param id 检查点唯一标识
   * @return 检查点读取通道
   * @throws IOException 打开过程IO异常
   * @throws InterruptedException 操作被中断
   */
  public CheckpointReadChannel open(CheckpointID id)
      throws IOException, InterruptedException {
      // 检查检查点ID类型是否匹配
      if (!(id instanceof FSCheckpointID)) {
        throw new IllegalArgumentException(
            "Mismatched checkpoint type: " + id.getClass());
      }
      // 打开检查点文件，包装为读取通道返回
      return new FSCheckpointReadChannel(
          fs.open(((FSCheckpointID) id).getPath()));
  }

  /**
   * 基于文件系统的检查点读取通道实现，封装对检查点文件的读取操作
   */
  private static class FSCheckpointReadChannel
      implements CheckpointReadChannel {

    private boolean isOpen = true;
    private final ReadableByteChannel in;

    /**
     * 构造检查点读取通道实例
     * @param in 检查点文件输入流
     */
    FSCheckpointReadChannel(FSDataInputStream in){
      this.in = Channels.newChannel(in);
    }

    @Override
    public int read(ByteBuffer bb) throws IOException {
      return in.read(bb);
    }

    @Override
    public void close() throws IOException {
      isOpen = false;
      in.close();
    }

    @Override
    public boolean isOpen() {
      return isOpen;
    }

  }

  @Override
  /**
   * 提交已写入完成的检查点，将临时文件重命名为正式检查点文件
   * @param ch 已完成写入的检查点写入通道
   * @return 提交后的检查点唯一标识
   * @throws IOException 提交过程IO异常
   * @throws InterruptedException 操作被中断
   */
  public CheckpointID commit(CheckpointWriteChannel ch)
      throws IOException, InterruptedException {
    // 如果通道还打开则先关闭
    if (ch.isOpen()) {
      ch.close();
    }
    // 转换为具体实现类
    FSCheckpointWriteChannel hch = (FSCheckpointWriteChannel)ch;
    Path dst = hch.getDestination();
    // 将临时文件重命名为正式文件，提交成功后才对外可见
    if (!fs.rename(tmpfile(dst), dst)) {
      // 重命名失败，清理临时文件并抛出异常
      abort(ch);
      throw new IOException("Failed to promote checkpoint" +
      		 tmpfile(dst) + " -> " + dst);
    }
    // 返回包含正式路径的检查点ID
    return new FSCheckpointID(hch.getDestination());
  }

  @Override
  /**
   * 中止检查点创建过程，清理临时文件
   * @param ch 需要中止的检查点写入通道
   * @throws IOException 清理过程IO异常
   */
  public void abort(CheckpointWriteChannel ch) throws IOException {
    // 如果通道还打开则先关闭
    if (ch.isOpen()) {
      ch.close();
    }
    FSCheckpointWriteChannel hch = (FSCheckpointWriteChannel)ch;
    Path tmp = tmpfile(hch.getDestination());
    try {
      // 删除临时文件，删除失败抛出异常
      if (!fs.delete(tmp, false)) {
        throw new IOException("Failed to delete checkpoint during abort");
      }
    } catch (FileNotFoundException e) {
      // 文件不存在说明已经被删除，直接忽略
    }
  }

  @Override
  /**
   * 删除指定ID的已提交检查点
   * @param id 需要删除的检查点唯一标识
   * @return 删除操作是否成功
   * @throws IOException 删除过程IO异常
   * @throws InterruptedException 操作被中断
   */
  public boolean delete(CheckpointID id) throws IOException,
      InterruptedException {
    // 检查检查点ID类型是否匹配
    if (!(id instanceof FSCheckpointID)) {
      throw new IllegalArgumentException(
          "Mismatched checkpoint type: " + id.getClass());
    }
    Path tmp = ((FSCheckpointID)id).getPath();
    try {
      // 删除检查点文件
      return fs.delete(tmp, false);
    } catch (FileNotFoundException e) {
      // 文件不存在，忽略异常
    }
    // 文件不存在视为删除成功
    return true;
  }

  /**
   * 根据正式检查点路径生成对应临时文件路径
   * @param p 正式检查点路径
   * @return 临时文件路径（后缀为.tmp）
   */
  static final Path tmpfile(Path p) {
    return new Path(p.getParent(), p.getName() + ".tmp");
  }

}
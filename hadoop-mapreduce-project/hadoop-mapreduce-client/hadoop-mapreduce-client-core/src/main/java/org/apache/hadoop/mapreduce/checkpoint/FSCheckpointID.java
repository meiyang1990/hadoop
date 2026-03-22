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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

/**
 * 基于文件系统的检查点ID实现，存储检查点在文件系统中的保存路径
 * 用于MapReduce任务检查点机制中标识检查点的存储位置
 */
public class FSCheckpointID implements CheckpointID {

  private Path path;

  /**
   * 空构造函数，供反序列化使用
   */
  public FSCheckpointID(){
  }

  /**
   * 根据指定路径构造文件系统检查点ID
   * @param path 检查点在文件系统中的存储路径
   */
  public FSCheckpointID(Path path) {
    this.path = path;
  }

  /**
   * 获取检查点存储路径
   * @return 检查点在文件系统中的路径
   */
  public Path getPath() {
    return path;
  }

  @Override
  public String toString() {
    return path.toString();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, path.toString());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.path = new Path(Text.readString(in));
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof FSCheckpointID
      && path.equals(((FSCheckpointID)other).path);
  }

  @Override
  public int hashCode() {
    return path.hashCode();
  }

}
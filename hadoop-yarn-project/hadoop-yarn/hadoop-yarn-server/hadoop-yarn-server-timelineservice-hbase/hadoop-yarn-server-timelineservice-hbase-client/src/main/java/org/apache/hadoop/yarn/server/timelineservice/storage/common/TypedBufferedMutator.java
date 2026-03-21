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
package org.apache.hadoop.yarn.server.timelineservice.storage.common;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Mutation;

/**
 * YARN时间线服务HBase存储层，泛型类型安全的BufferedMutator包装类，将底层HBase的写入器与具体表类型绑定，保证类型安全。
 *
 * @param <T> 指代要写入的HBase表的类型，必须继承自BaseTable
 */
public class TypedBufferedMutator<T extends BaseTable<T>> {

  private final BufferedMutator bufferedMutator;

  /**
   * 构造方法，包装底层HBase的BufferedMutator实现。
   * @param bufferedMutator 用于委托操作的底层BufferedMutator实例，不可为null
   */
  public TypedBufferedMutator(BufferedMutator bufferedMutator) {
    this.bufferedMutator = bufferedMutator;
  }

  public TableName getName() {
    return bufferedMutator.getName();
  }

  public Configuration getConfiguration() {
    return bufferedMutator.getConfiguration();
  }

  public void mutate(Mutation mutation) throws IOException {
    bufferedMutator.mutate(mutation);
  }

  public void mutate(List<? extends Mutation> mutations) throws IOException {
    bufferedMutator.mutate(mutations);
  }

  public void close() throws IOException {
    bufferedMutator.close();
  }

  public void flush() throws IOException {
    bufferedMutator.flush();
  }

  public long getWriteBufferSize() {
    return bufferedMutator.getWriteBufferSize();
  }

}
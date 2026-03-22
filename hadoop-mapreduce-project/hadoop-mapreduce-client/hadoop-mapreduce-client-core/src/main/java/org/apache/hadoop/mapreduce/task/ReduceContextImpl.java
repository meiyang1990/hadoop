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

package org.apache.hadoop.mapreduce.task;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.BackupStore;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.Progressable;

/**
 * Reduce任务执行时传递给{@link Reducer}的上下文实现，提供对输入键值对、输出写入、进度报告等能力的访问
 * @param <KEYIN> 输入键的类型
 * @param <VALUEIN> 输入值的类型
 * @param <KEYOUT> 输出键的类型
 * @param <VALUEOUT> 输出值的类型
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ReduceContextImpl<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
    extends TaskInputOutputContextImpl<KEYIN,VALUEIN,KEYOUT,VALUEOUT> 
    implements ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
  private RawKeyValueIterator input;
  private Counter inputValueCounter;
  private Counter inputKeyCounter;
  private RawComparator<KEYIN> comparator;
  private KEYIN key;                                  // 当前处理键
  private VALUEIN value;                              // 当前处理值
  private boolean firstValue = false;                 // 是否为当前键的第一个值
  private boolean nextKeyIsSame = false;              // 下一个键是否与当前键相同
  private boolean hasMore;                            // 输入是否还有更多数据
  protected Progressable reporter;
  private Deserializer<KEYIN> keyDeserializer;
  private Deserializer<VALUEIN> valueDeserializer;
  private DataInputBuffer buffer = new DataInputBuffer();
  private BytesWritable currentRawKey = new BytesWritable();
  private ValueIterable iterable = new ValueIterable();
  private boolean isMarked = false;
  private BackupStore<KEYIN,VALUEIN> backupStore;
  private final SerializationFactory serializationFactory;
  private final Class<KEYIN> keyClass;
  private final Class<VALUEIN> valueClass;
  private final Configuration conf;
  private final TaskAttemptID taskid;
  private int currentKeyLength = -1;
  private int currentValueLength = -1;
  
  /**
   * 构造Reduce上下文实例，完成反序列化初始化并读取第一个键值对
   * @param conf 作业配置
   * @param taskid Reduce任务尝试ID
   * @param input 排序后的键值对迭代器
   * @param inputKeyCounter 输入键计数器
   * @param inputValueCounter 输入值计数器
   * @param output 输出记录写入器
   * @param committer 输出提交器
   * @param reporter 状态报告器
   * @param comparator 键排序比较器
   * @param keyClass 输入键类型
   * @param valueClass 输入值类型
   * @throws InterruptedException 线程中断异常
   * @throws IOException IO异常
   */
  public ReduceContextImpl(Configuration conf, TaskAttemptID taskid,
                           RawKeyValueIterator input, 
                           Counter inputKeyCounter,
                           Counter inputValueCounter,
                           RecordWriter<KEYOUT,VALUEOUT> output,
                           OutputCommitter committer,
                           StatusReporter reporter,
                           RawComparator<KEYIN> comparator,
                           Class<KEYIN> keyClass,
                           Class<VALUEIN> valueClass
                          ) throws InterruptedException, IOException{
    super(conf, taskid, output, committer, reporter);
    this.input = input;
    this.inputKeyCounter = inputKeyCounter;
    this.inputValueCounter = inputValueCounter;
    this.comparator = comparator;
    this.serializationFactory = new SerializationFactory(conf);
    this.keyDeserializer = serializationFactory.getDeserializer(keyClass);
    this.keyDeserializer.open(buffer);
    this.valueDeserializer = serializationFactory.getDeserializer(valueClass);
    this.valueDeserializer.open(buffer);
    hasMore = input.next();
    this.keyClass = keyClass;
    this.valueClass = valueClass;
    this.conf = conf;
    this.taskid = taskid;
  }

  /**
   * 开始处理下一个唯一键，跳过当前键剩余的所有值
   * @return 是否存在下一个唯一键
   * @throws IOException IO异常
   * @throws InterruptedException 线程中断异常
   */
  public boolean nextKey() throws IOException,InterruptedException {
    while (hasMore && nextKeyIsSame) {
      nextKeyValue();
    }
    if (hasMore) {
      if (inputKeyCounter != null) {
        inputKeyCounter.increment(1);
      }
      return nextKeyValue();
    } else {
      return false;
    }
  }

  /**
   * 移动到下一个键值对，反序列化键和值并判断下一个键是否与当前键相同
   */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (!hasMore) {
      key = null;
      value = null;
      return false;
    }
    firstValue = !nextKeyIsSame;
    DataInputBuffer nextKey = input.getKey();
    currentRawKey.set(nextKey.getData(), nextKey.getPosition(), 
                      nextKey.getLength() - nextKey.getPosition());
    buffer.reset(currentRawKey.getBytes(), 0, currentRawKey.getLength());
    key = keyDeserializer.deserialize(key);
    DataInputBuffer nextVal = input.getValue();
    buffer.reset(nextVal.getData(), nextVal.getPosition(), nextVal.getLength()
        - nextVal.getPosition());
    value = valueDeserializer.deserialize(value);

    currentKeyLength = nextKey.getLength() - nextKey.getPosition();
    currentValueLength = nextVal.getLength() - nextVal.getPosition();

    if (isMarked) {
      backupStore.write(nextKey, nextVal);
    }

    hasMore = input.next();
    if (hasMore) {
      nextKey = input.getKey();
      nextKeyIsSame = comparator.compare(currentRawKey.getBytes(), 0, 
                                     currentRawKey.getLength(),
                                     nextKey.getData(),
                                     nextKey.getPosition(),
                                     nextKey.getLength() - nextKey.getPosition()
                                         ) == 0;
    } else {
      nextKeyIsSame = false;
    }
    inputValueCounter.increment(1);
    return true;
  }

  @Override
  public KEYIN getCurrentKey() {
    return key;
  }

  @Override
  public VALUEIN getCurrentValue() {
    return value;
  }
  
  BackupStore<KEYIN,VALUEIN> getBackupStore() {
    return backupStore;
  }
  
  /**
   * 为当前键的所有值实现支持mark/reset的迭代器，支持用户对值组的重复遍历
   */
  protected class ValueIterator implements ReduceContext.ValueIterator<VALUEIN> {

    private boolean inReset = false;
    private boolean clearMarkFlag = false;

    @Override
    public boolean hasNext() {
      try {
        if (inReset && backupStore.hasNext()) {
          return true;
        } 
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("hasNext failed", e);
      }
      return firstValue || nextKeyIsSame;
    }

    @Override
    public VALUEIN next() {
      if (inReset) {
        try {
          if (backupStore.hasNext()) {
            backupStore.next();
            DataInputBuffer next = backupStore.nextValue();
            buffer.reset(next.getData(), next.getPosition(), next.getLength()
                - next.getPosition());
            value = valueDeserializer.deserialize(value);
            return value;
          } else {
            inReset = false;
            backupStore.exitResetMode();
            if (clearMarkFlag) {
              clearMarkFlag = false;
              isMarked = false;
            }
          }
        } catch (IOException e) {
          e.printStackTrace();
          throw new RuntimeException("next value iterator failed", e);
        }
      } 

      // 第一个值无需前进，直接返回当前值
      if (firstValue) {
        firstValue = false;
        return value;
      }
      // 已到当前键最后一个值，抛出异常
      if (!nextKeyIsSame) {
        throw new NoSuchElementException("iterate past last value");
      }
      // 前进到下一个值并返回
      try {
        nextKeyValue();
        return value;
      } catch (IOException ie) {
        throw new RuntimeException("next value iterator failed", ie);
      } catch (InterruptedException ie) {
        // this is bad, but we can't modify the exception list of java.util
        throw new RuntimeException("next value iterator interrupted", ie);        
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("remove not implemented");
    }

    /**
     * 标记当前遍历位置，将已遍历的数据备份到BackupStore中，用于后续reset
     * @throws IOException IO异常
     */
    @Override
    public void mark() throws IOException {
      if (getBackupStore() == null) {
        backupStore = new BackupStore<KEYIN,VALUEIN>(conf, taskid);
      }
      isMarked = true;
      if (!inReset) {
        backupStore.reinitialize();
        if (currentKeyLength == -1) {
          // 用户尚未调用next()，没有当前记录需要备份
          return;
        }
        assert (currentValueLength != -1);
        int requestedSize = currentKeyLength + currentValueLength + 
          WritableUtils.getVIntSize(currentKeyLength) +
          WritableUtils.getVIntSize(currentValueLength);
        DataOutputStream out = backupStore.getOutputStream(requestedSize);
        writeFirstKeyValueBytes(out);
        backupStore.updateCounters(requestedSize);
      } else {
        backupStore.mark();
      }
    }

    /**
     * 将迭代器重置到之前标记的位置，重新开始遍历当前键的值
     * @throws IOException IO异常，无标记时抛出
     */
    @Override
    public void reset() throws IOException {
      // 清除标记后调用reset，抛出异常
      if (clearMarkFlag) {
        clearMarkFlag = false;
        backupStore.clearMark();
        throw new IOException("Reset called without a previous mark");
      }
      
      if (!isMarked) {
        throw new IOException("Reset called without a previous mark");
      }
      inReset = true;
      backupStore.reset();
    }

    /**
     * 清除之前设置的标记，释放备份存储空间
     * @throws IOException IO异常
     */
    @Override
    public void clearMark() throws IOException {
      if (getBackupStore() == null) {
        return;
      }
      if (inReset) {
        clearMarkFlag = true;
        backupStore.clearMark();
      } else {
        inReset = isMarked = false;
        backupStore.reinitialize();
      }
    }
    
    /**
     * 切换到下一个键时重置备份存储，清空当前键的备份数据
     * @throws IOException IO异常
     */
    public void resetBackupStore() throws IOException {
      if (getBackupStore() == null) {
        return;
      }
      inReset = isMarked = false;
      backupStore.reinitialize();
      currentKeyLength = -1;
    }

    /**
     * 将标记前最后一次返回的键值对序列化写入备份存储
     * @param out 备份输出流
     * @throws IOException IO异常
     */
    private void writeFirstKeyValueBytes(DataOutputStream out) 
    throws IOException {
      assert (getCurrentKey() != null && getCurrentValue() != null);
      WritableUtils.writeVInt(out, currentKeyLength);
      WritableUtils.writeVInt(out, currentValueLength);
      Serializer<KEYIN> keySerializer = 
        serializationFactory.getSerializer(keyClass);
      keySerializer.open(out);
      keySerializer.serialize(getCurrentKey());

      Serializer<VALUEIN> valueSerializer = 
        serializationFactory.getSerializer(valueClass);
      valueSerializer.open(out);
      valueSerializer.serialize(getCurrentValue());
    }
  }

  /**
   * 可迭代值组包装类，提供ValueIterator实例给Reducer遍历
   */
  protected class ValueIterable implements Iterable<VALUEIN> {
    private ValueIterator iterator = new ValueIterator();
    @Override
    public Iterator<VALUEIN> iterator() {
      return iterator;
    } 
  }
  
  /**
   * 获取当前键对应的所有值的可迭代对象，供Reducer遍历处理
   * @return 当前键的值组可迭代对象
   * @throws IOException IO异常
   * @throws InterruptedException 线程中断异常
   */
  public 
  Iterable<VALUEIN> getValues() throws IOException, InterruptedException {
    return iterable;
  }
}
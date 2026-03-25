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

package org.apache.hadoop.mapreduce.jobhistory;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 文件级：MapReduce作业历史事件写入工具类，负责将作业执行过程中的各类事件序列化写入输出流
 * Event Writer is an utility class used to write events to the underlying
 * stream. Typically, one event writer (which translates to one stream) 
 * is created per job 
 * 
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class EventWriter {
  // Avro-JSON格式版本标识
  static final String VERSION = "Avro-Json";
  // Avro二进制格式版本标识
  static final String VERSION_BINARY = "Avro-Binary";

  private FSDataOutputStream out;
  private DatumWriter<Event> writer =
    new SpecificDatumWriter<Event>(Event.class);
  private Encoder encoder;
  private static final Logger LOG = LoggerFactory.getLogger(EventWriter.class);

  /**
   * 枚举类：EventWriter支持的Avro编码格式
   */
  public enum WriteMode { JSON, BINARY }
  private final WriteMode writeMode;
  private final boolean jsonOutput;  // Cache value while we have 2 modes

  /**
   * 构造EventWriter实例，初始化输出流和Avro编码器，写入版本头和Schema信息
   * @param out 输出流，写入作业历史数据
   * @param mode 编码格式模式
   * @throws IOException 写入头信息失败时抛出异常
   */
  @VisibleForTesting
  public EventWriter(FSDataOutputStream out, WriteMode mode)
      throws IOException {
    this.out = out;
    this.writeMode = mode;
    if (this.writeMode==WriteMode.JSON) {
      this.jsonOutput = true;
      // 写入JSON格式版本标识
      out.writeBytes(VERSION);
    } else if (this.writeMode==WriteMode.BINARY) {
      this.jsonOutput = false;
      // 写入二进制格式版本标识
      out.writeBytes(VERSION_BINARY);
    } else {
      throw new IOException("Unknown mode: " + mode);
    }
    // 换行分隔版本和Schema
    out.writeBytes("\n");
    // 写入Event的Avro Schema定义
    out.writeBytes(Event.SCHEMA$.toString());
    out.writeBytes("\n");
    // 根据格式创建对应Avro编码器
    if (!this.jsonOutput) {
      this.encoder = EncoderFactory.get().binaryEncoder(out, null);
    } else {
      this.encoder = EncoderFactory.get().jsonEncoder(Event.SCHEMA$, out);
    }
  }
  
  /**
   * 同步写入一个作业历史事件到输出流
   * @param event 要写入的历史事件对象
   * @throws IOException 写入失败时抛出异常
   */
  synchronized void write(HistoryEvent event) throws IOException { 
    // 创建Avro Event包装对象
    Event wrapper = new Event();
    // 设置事件类型
    wrapper.setType(event.getEventType());
    // 设置事件具体数据
    wrapper.setEvent(event.getDatum());
    // 通过Avro写入编码器
    writer.write(wrapper, encoder);
    // JSON格式每次写入后刷新并换行，保证每条事件独立可解析
    if (this.jsonOutput) {
      encoder.flush();
      out.writeBytes("\n");
    }
  }
  
  /**
   * 刷新所有缓冲数据到输出流
   * @throws IOException 刷新失败时抛出异常
   */
  void flush() throws IOException {
    encoder.flush();
    out.flush();
    out.hflush();
  }

  /**
   * 关闭EventWriter，释放资源并关闭输出流
   * @throws IOException 关闭失败时抛出异常
   */
  @VisibleForTesting
  public void close() throws IOException {
    try {
      encoder.flush();
      out.close();
      out = null;
    } finally {
      IOUtils.cleanupWithLogger(LOG, out);
    }
  }

  // 计数器组数组的Avro Schema定义
  private static final Schema GROUPS =
    Schema.createArray(JhCounterGroup.SCHEMA$);

  // 计数器数组的Avro Schema定义
  private static final Schema COUNTERS =
    Schema.createArray(JhCounter.SCHEMA$);

  /**
   * 将MapReduce原生Counters对象转换为Avro格式JhCounters对象，默认名称为COUNTERS
   * @param counters 原生MapReduce计数器对象
   * @return 转换完成的Avro格式计数器对象
   */
  static JhCounters toAvro(Counters counters) {
    return toAvro(counters, "COUNTERS");
  }

  /**
   * 将MapReduce原生Counters对象转换为Avro格式JhCounters对象，用于作业历史存储
   * @param counters 原生MapReduce计数器对象
   * @param name 计数器集合名称
   * @return 转换完成的Avro格式计数器对象
   */
  static JhCounters toAvro(Counters counters, String name) {
    JhCounters result = new JhCounters();
    result.setName(new Utf8(name));
    result.setGroups(new ArrayList<JhCounterGroup>(0));
    if (counters == null) return result;
    // 遍历所有计数器组
    for (CounterGroup group : counters) {
      // 创建Avro计数器组对象
      JhCounterGroup g = new JhCounterGroup();
      g.setName(new Utf8(group.getName()));
      g.setDisplayName(new Utf8(group.getDisplayName()));
      g.setCounts(new ArrayList<JhCounter>(group.size()));
      // 遍历组内所有计数器
      for (Counter counter : group) {
        // 创建Avro计数器对象
        JhCounter c = new JhCounter();
        c.setName(new Utf8(counter.getName()));
        c.setDisplayName(new Utf8(counter.getDisplayName()));
        c.setValue(counter.getValue());
        g.getCounts().add(c);
      }
      result.getGroups().add(g);
    }
    return result;
  }

}
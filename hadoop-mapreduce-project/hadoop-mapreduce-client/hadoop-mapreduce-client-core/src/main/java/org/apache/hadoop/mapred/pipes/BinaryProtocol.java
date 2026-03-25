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

package org.apache.hadoop.mapred.pipes;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @file BinaryProtocol.java
 * @brief Pipes协议的二进制实现，负责Java服务端与C++客户端之间的二进制协议通信
 * 实现DownwardProtocol接口，处理Java到C++方向的下行消息发送，同时启动上行读取线程处理C++到Java方向的上行消息
 */
/**
 * This protocol is a binary implementation of the Pipes protocol.
 */
class BinaryProtocol<K1 extends WritableComparable, V1 extends Writable,
                     K2 extends WritableComparable, V2 extends Writable>
  implements DownwardProtocol<K1, V1> {
  
  public static final int CURRENT_PROTOCOL_VERSION = 0;
  /**
   * The buffer size for the command socket
   */
  private static final int BUFFER_SIZE = 128*1024;

  private DataOutputStream stream;
  private DataOutputBuffer buffer = new DataOutputBuffer();
  private static final Logger LOG =
      LoggerFactory.getLogger(BinaryProtocol.class.getName());
  private UplinkReaderThread uplink;

  /**
   * 消息类型枚举，定义了Java与C++之间交互的所有消息类型编码
   * 编码必须与C++端完全一致，否则会导致通信错误
   * The integer codes to represent the different messages. These must match
   * the C++ codes or massive confusion will result.
   */
  private enum MessageType { START(0),
                                    SET_JOB_CONF(1),
                                    SET_INPUT_TYPES(2),
                                    RUN_MAP(3),
                                    MAP_ITEM(4),
                                    RUN_REDUCE(5),
                                    REDUCE_KEY(6),
                                    REDUCE_VALUE(7),
                                    CLOSE(8),
                                    ABORT(9),
                                    AUTHENTICATION_REQ(10),
                                    OUTPUT(50),
                                    PARTITIONED_OUTPUT(51),
                                    STATUS(52),
                                    PROGRESS(53),
                                    DONE(54),
                                    REGISTER_COUNTER(55),
                                    INCREMENT_COUNTER(56),
                                    AUTHENTICATION_RESP(57);
    final int code;
    MessageType(int code) {
      this.code = code;
    }
  }

  /**
   * @brief 上行消息读取线程，负责从C++客户端读取上行消息并分发处理
   * 继承SubjectInheritingThread，继承安全主体上下文信息，保证认证上下文传递
   */
  private static class UplinkReaderThread<K2 extends WritableComparable,
                                          V2 extends Writable>  
    extends SubjectInheritingThread {
    
    private DataInputStream inStream;
    private UpwardProtocol<K2, V2> handler;
    private K2 key;
    private V2 value;
    private boolean authPending = true;
    
    /**
     * @brief 构造上行读取线程
     * @param stream 输入流，来自Socket的输入
     * @param handler 上行消息处理器，处理接收到的C++消息
     * @param key 复用的key对象，用于反序列化key
     * @param value 复用的value对象，用于反序列化value
     * @throws IOException 初始化流失败抛出异常
     */
    public UplinkReaderThread(InputStream stream,
                              UpwardProtocol<K2, V2> handler, 
                              K2 key, V2 value) throws IOException{
      inStream = new DataInputStream(new BufferedInputStream(stream, 
                                                             BUFFER_SIZE));
      this.handler = handler;
      this.key = key;
      this.value = value;
    }

    /**
     * @brief 关闭输入连接
     * @throws IOException 关闭失败抛出异常
     */
    public void closeConnection() throws IOException {
      inStream.close();
    }

    /**
     * @brief 线程主工作循环，持续读取并处理C++发送的上行消息
     */
    public void work() {
      while (true) {
        try {
          // 检查线程是否被中断，中断则退出循环
          if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException();
          }
          // 读取命令编码
          int cmd = WritableUtils.readVInt(inStream);
          LOG.debug("Handling uplink command " + cmd);
          // 处理认证响应消息
          if (cmd == MessageType.AUTHENTICATION_RESP.code) {
            String digest = Text.readString(inStream);
            authPending = !handler.authenticate(digest);
          } else if (authPending) {
            // 认证完成前收到其他消息，忽略并警告
            LOG.warn("Message " + cmd + " received before authentication is "
                + "complete. Ignoring");
            continue;
          } else if (cmd == MessageType.OUTPUT.code) {
            // 处理普通输出消息
            readObject(key);
            readObject(value);
            handler.output(key, value);
          } else if (cmd == MessageType.PARTITIONED_OUTPUT.code) {
            // 处理带分区的输出消息
            int part = WritableUtils.readVInt(inStream);
            readObject(key);
            readObject(value);
            handler.partitionedOutput(part, key, value);
          } else if (cmd == MessageType.STATUS.code) {
            // 处理状态更新消息
            handler.status(Text.readString(inStream));
          } else if (cmd == MessageType.PROGRESS.code) {
            // 处理进度更新消息
            handler.progress(inStream.readFloat());
          } else if (cmd == MessageType.REGISTER_COUNTER.code) {
            // 处理计数器注册消息
            int id = WritableUtils.readVInt(inStream);
            String group = Text.readString(inStream);
            String name = Text.readString(inStream);
            handler.registerCounter(id, group, name);
          } else if (cmd == MessageType.INCREMENT_COUNTER.code) {
            // 处理计数器增量更新消息
            int id = WritableUtils.readVInt(inStream);
            long amount = WritableUtils.readVLong(inStream);
            handler.incrementCounter(id, amount);
          } else if (cmd == MessageType.DONE.code) {
            // 任务完成消息，处理完成后退出线程
            LOG.debug("Pipe child done");
            handler.done();
            return;
          } else {
            // 未知命令，抛出IO异常
            throw new IOException("Bad command code: " + cmd);
          }
        } catch (InterruptedException e) {
          // 线程被中断，正常退出
          return;
        } catch (Throwable e) {
          // 处理过程中发生异常，通知处理器并退出线程
          LOG.error(StringUtils.stringifyException(e));
          handler.failed(e);
          return;
        }
      }
    }
    
    /**
     * @brief 从输入流读取并反序列化Writable对象
     * 对BytesWritable和Text做特殊处理，保证和C++端自然兼容
     * @param obj 目标对象，读取结果存入该对象
     * @throws IOException 读取失败抛出异常
     */
    private void readObject(Writable obj) throws IOException {
      int numBytes = WritableUtils.readVInt(inStream);
      byte[] buffer;
      // For BytesWritable and Text, use the specified length to set the length
      // this causes the "obvious" translations to work. So that if you emit
      // a string "abc" from C++, it shows up as "abc".
      if (obj instanceof BytesWritable) {
        buffer = new byte[numBytes];
        inStream.readFully(buffer);
        ((BytesWritable) obj).set(buffer, 0, numBytes);
      } else if (obj instanceof Text) {
        buffer = new byte[numBytes];
        inStream.readFully(buffer);
        ((Text) obj).set(buffer);
      } else {
        obj.readFields(inStream);
      }
    }
  }

  /**
   * @brief  Tee输出流，用于调试时将下行数据同时保存到本地文件
   * 继承FilterOutputStream，将写出数据同时写出到基础流和调试文件
   * An output stream that will save a copy of the data into a file.
   */
  private static class TeeOutputStream extends FilterOutputStream {
    private OutputStream file;
    /**
     * @brief 构造Tee输出流
     * @param filename 调试保存文件名
     * @param base 基础输出流，原始通信输出流
     * @throws IOException 创建文件失败抛出异常
     */
    TeeOutputStream(String filename, OutputStream base) throws IOException {
      super(base);
      file = new FileOutputStream(filename);
    }
    public void write(byte b[], int off, int len) throws IOException {
      file.write(b, off, len);
      out.write(b, off, len);
    }

    public void write(int b) throws IOException {
      file.write(b);
      out.write(b);
    }

    public void flush() throws IOException {
      file.flush();
      out.flush();
    }

    public void close() throws IOException {
      try {
        flush();
      } finally {
        IOUtils.closeStream(file);
        IOUtils.closeStream(out);
      }
    }
  }

  /**
   * @brief 构造二进制协议处理器，建立Java与C++之间的通信通道
   * Create a proxy object that will speak the binary protocol on a socket.
   * Upward messages are passed on the specified handler and downward
   * downward messages are public methods on this object.
   * @param sock 通信Socket，用于Java与C++进程通信
   * @param handler 上行消息处理器，处理C++发送的消息
   * @param key 复用的key对象，用于反序列化上行key
   * @param value 复用的value对象，用于反序列化上行value
   * @param config 作业配置对象
   * @throws IOException 初始化流或线程失败抛出异常
   */
  public BinaryProtocol(Socket sock, 
                        UpwardProtocol<K2, V2> handler,
                        K2 key,
                        V2 value,
                        JobConf config) throws IOException {
    OutputStream raw = sock.getOutputStream();
    // If we are debugging, save a copy of the downlink commands to a file
    if (Submitter.getKeepCommandFile(config)) {
      raw = new TeeOutputStream("downlink.data", raw);
    }
    stream = new DataOutputStream(new BufferedOutputStream(raw, 
                                                           BUFFER_SIZE)) ;
    uplink = new UplinkReaderThread<K2, V2>(sock.getInputStream(),
                                            handler, key, value);
    uplink.setName("pipe-uplink-handler");
    uplink.start();
  }

  /**
   * @brief 关闭通信连接，终止上行线程
   * Close the connection and shutdown the handler thread.
   * @throws IOException IO异常
   * @throws InterruptedException 线程中断异常
   */
  public void close() throws IOException, InterruptedException {
    LOG.debug("closing connection");
    stream.close();
    uplink.closeConnection();
    uplink.interrupt();
    uplink.join();
  }
  
  /**
   * @brief 发送认证请求消息到C++端
   * @param digest 摘要信息
   * @param challenge 挑战码
   * @throws IOException 发送失败抛出异常
   */
  public void authenticate(String digest, String challenge)
      throws IOException {
    LOG.debug("Sending AUTHENTICATION_REQ, digest=" + digest + ", challenge="
        + challenge);
    WritableUtils.writeVInt(stream, MessageType.AUTHENTICATION_REQ.code);
    Text.writeString(stream, digest);
    Text.writeString(stream, challenge);
  }

  /**
   * @brief 发送启动消息到C++端
   * @throws IOException 发送失败抛出异常
   */
  public void start() throws IOException {
    LOG.debug("starting downlink");
    WritableUtils.writeVInt(stream, MessageType.START.code);
    WritableUtils.writeVInt(stream, CURRENT_PROTOCOL_VERSION);
  }

  /**
   * @brief 发送作业配置到C++端
   * @param job 作业配置对象
   * @throws IOException 发送失败抛出异常
   */
  public void setJobConf(JobConf job) throws IOException {
    WritableUtils.writeVInt(stream, MessageType.SET_JOB_CONF.code);
    List<String> list = new ArrayList<String>();
    for(Map.Entry<String, String> itm: job) {
      list.add(itm.getKey());
      list.add(itm.getValue());
    }
    WritableUtils.writeVInt(stream, list.size());
    for(String entry: list){
      Text.writeString(stream, entry);
    }
  }

  /**
   * @brief 发送输入键值类型信息到C++端
   * @param keyType key类型类名
   * @param valueType value类型类名
   * @throws IOException 发送失败抛出异常
   */
  public void setInputTypes(String keyType, 
                            String valueType) throws IOException {
    WritableUtils.writeVInt(stream, MessageType.SET_INPUT_TYPES.code);
    Text.writeString(stream, keyType);
    Text.writeString(stream, valueType);
  }

  /**
   * @brief 发送运行Map任务命令到C++端
   * @param split 输入分片信息
   * @param numReduces Reduce任务数量
   * @param pipedInput 是否使用管道输入
   * @throws IOException 发送失败抛出异常
   */
  public void runMap(InputSplit split, int numReduces, 
                     boolean pipedInput) throws IOException {
    WritableUtils.writeVInt(stream, MessageType.RUN_MAP.code);
    writeObject(split);
    WritableUtils.writeVInt(stream, numReduces);
    WritableUtils.writeVInt(stream, pipedInput ? 1 : 0);
  }

  /**
   * @brief 发送一条Map输入记录到C++端
   * @param key 输入key
   * @param value 输入value
   * @throws IOException 发送失败抛出异常
   */
  public void mapItem(WritableComparable key, 
                      Writable value) throws IOException {
    WritableUtils.writeVInt(stream, MessageType.MAP_ITEM.code);
    writeObject(key);
    writeObject(value);
  }

  /**
   * @brief 发送运行Reduce任务命令到C++端
   * @param reduce 当前Reduce编号
   * @param pipedOutput 是否使用管道输出
   * @throws IOException 发送失败抛出异常
   */
  public void runReduce(int reduce, boolean pipedOutput) throws IOException {
    WritableUtils.writeVInt(stream, MessageType.RUN_REDUCE.code);
    WritableUtils.writeVInt(stream, reduce);
    WritableUtils.writeVInt(stream, pipedOutput ? 1 : 0);
  }

  /**
   * @brief 发送一个Reduce输入key到C++端
   * @param key 输入key
   * @throws IOException 发送失败抛出异常
   */
  public void reduceKey(WritableComparable key) throws IOException {
    WritableUtils.writeVInt(stream, MessageType.REDUCE_KEY.code);
    writeObject(key);
  }

  /**
   * @brief 发送一个Reduce输入value到C++端
   * @param value 输入value
   * @throws IOException 发送失败抛出异常
   */
  public void reduceValue(Writable value) throws IOException {
    WritableUtils.writeVInt(stream, MessageType.REDUCE_VALUE.code);
    writeObject(value);
  }

  /**
   * @brief 发送输入结束消息，通知C++端输入已经完成
   * @throws IOException 发送失败抛出异常
   */
  public void endOfInput() throws IOException {
    WritableUtils.writeVInt(stream, MessageType.CLOSE.code);
    LOG.debug("Sent close command");
  }
  
  /**
   * @brief 发送中止任务消息，通知C++端中止执行
   * @throws IOException 发送失败抛出异常
   */
  public void abort() throws
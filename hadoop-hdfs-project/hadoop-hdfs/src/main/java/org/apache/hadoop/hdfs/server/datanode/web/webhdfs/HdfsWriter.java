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
package org.apache.hadoop.hdfs.server.datanode.web.webhdfs;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.LastHttpContent;
import org.slf4j.Logger;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.OutputStream;

import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;

/**
 * WebHDFS写请求处理Handler，负责将HTTP请求中的数据写入HDFS输出流，
 * 处理连接生命周期和异常场景，完成后释放资源。
 */
class HdfsWriter extends SimpleChannelInboundHandler<HttpContent> {
  private final DFSClient client;
  private final OutputStream out;
  private final DefaultHttpResponse response;
  private static final Logger LOG = WebHdfsHandler.LOG;

  /**
   * 构造HdfsWriter实例，初始化写操作所需资源
   * @param client HDFS客户端，用于访问HDFS
   * @param out HDFS输出流，用于写入请求数据
   * @param response HTTP响应对象，用于返回写结果
   */
  HdfsWriter(DFSClient client, OutputStream out, DefaultHttpResponse response) {
    this.client = client;
    this.out = out;
    this.response = response;
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    ctx.flush();
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, HttpContent chunk)
    throws IOException {
    // 将当前HTTP内容块写入HDFS输出流
    chunk.content().readBytes(out, chunk.content().readableBytes());
    // 判断是否为最后一个内容块，即请求传输完成
    if (chunk instanceof LastHttpContent) {
      try {
        // 关闭HDFS资源，抛出异常会被捕获
        releaseDfsResourcesAndThrow();
        // 设置连接关闭头
        response.headers().set(CONNECTION, CLOSE);
        // 写入响应并关闭连接
        ctx.write(response).addListener(ChannelFutureListener.CLOSE);
      } catch (Exception cause) {
        // 处理异常情况
        exceptionCaught(ctx, cause);
      }
    }
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    // 连接断开时释放HDFS资源
    releaseDfsResources();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    // 发生异常时先释放HDFS资源
    releaseDfsResources();
    // 构造异常响应
    DefaultHttpResponse resp = ExceptionHandler.exceptionCaught(cause);
    resp.headers().set(CONNECTION, CLOSE);
    // 返回异常响应并关闭连接
    ctx.writeAndFlush(resp).addListener(ChannelFutureListener.CLOSE);
    // 调试日志记录异常信息
    if (LOG != null && LOG.isDebugEnabled()) {
      LOG.debug("Exception in channel handler ", cause);
    }
  }

  /**
   * 安全释放HDFS写操作相关资源，记录释放过程中的异常日志
   */
  private void releaseDfsResources() {
    IOUtils.cleanupWithLogger(LOG, out);
    IOUtils.cleanupWithLogger(LOG, client);
  }

  /**
   * 主动关闭HDFS写操作相关资源，遇到异常直接抛出
   * @throws Exception 关闭资源过程中发生的异常
   */
  private void releaseDfsResourcesAndThrow() throws Exception {
    out.close();
    client.close();
  }
}
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
package org.apache.hadoop.yarn.server.nodemanager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIO.Windows;
import org.apache.hadoop.io.nativeio.NativeIOException;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.CommandExecutor;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.server.nodemanager.executor.LocalizerStartContext;

/**
 * Windows平台安全容器执行器。
 * 本类为Windows平台提供类似LinuxContainerExecutor的安全容器执行能力，由于NodeManager默认不运行在高权限上下文，
 * 本类会将需要提升权限的操作委托给以Windows服务身份运行的hadoopwintuilsvc（winutils.exe）处理，
 * 通过JNI和LRPC与特权服务进行通信。
 */
public class WindowsSecureContainerExecutor extends DefaultContainerExecutor {
  
  private static final Logger LOG = LoggerFactory
      .getLogger(WindowsSecureContainerExecutor.class);
  
  public static final String LOCALIZER_PID_FORMAT = "STAR_LOCALIZER_%s";
  
  
  /**
   * 封装WSCE使用的JNI Win32本地方法。
   */
  private static class Native {

    private static boolean nativeLoaded = false;

    static {
      if (NativeCodeLoader.isNativeCodeLoaded()) {
        try {
          // 初始化WSCE本地库
          initWsceNative();
          nativeLoaded = true;
        } catch (Throwable t) {
          LOG.info("Unable to initialize WSCE Native libraries", t);
        }
      }
    }

    /** Initialize the JNI method ID and class ID cache */
    private static native void initWsceNative();
    
    
    /**
     * 封装WindowsSecureContainerExecutor需要特权执行的文件系统操作方法。
     */
    public static class Elevated {
      private static final int MOVE_FILE = 1;
      private static final int COPY_FILE = 2;

      public static void mkdir(Path dirName) throws IOException {
        if (!nativeLoaded) {
          throw new IOException("Native WSCE libraries are required for mkdir");
        }
        elevatedMkDirImpl(dirName.toString());
      }
      
      private static native void elevatedMkDirImpl(String dirName) 
          throws IOException;
      
      public static void chown(Path fileName, String user, String group) 
          throws IOException {
        if (!nativeLoaded) {
          throw new IOException("Native WSCE libraries are required for chown");
        }
        elevatedChownImpl(fileName.toString(), user, group);
      }
      
      private static native void elevatedChownImpl(String fileName, String user, 
          String group) throws IOException;
      
      public static void move(Path src, Path dst, boolean replaceExisting) 
          throws IOException {
        if (!nativeLoaded) {
          throw new IOException("Native WSCE libraries are required for move");
        }
        elevatedCopyImpl(MOVE_FILE, src.toString(), dst.toString(), 
            replaceExisting);
      }
      
      public static void copy(Path src, Path dst, boolean replaceExisting) 
          throws IOException {
        if (!nativeLoaded) {
          throw new IOException("Native WSCE libraries are required for copy");
        }
        elevatedCopyImpl(COPY_FILE, src.toString(), dst.toString(), 
            replaceExisting);
      }
      
      private static native void elevatedCopyImpl(int operation, String src, 
          String dst, boolean replaceExisting) throws IOException;
      
      public static void chmod(Path fileName, int mode) throws IOException {
        if (!nativeLoaded) {
          throw new IOException("Native WSCE libraries are required for chmod");
        }
        elevatedChmodImpl(fileName.toString(), mode);
      }
      
      private static native void elevatedChmodImpl(String path, int mode) 
          throws IOException;
      
      public static void killTask(String containerName) throws IOException {
        if (!nativeLoaded) {
          throw new IOException("Native WSCE libraries are required for killTask");
        }
        elevatedKillTaskImpl(containerName);
      }
      
      private static native void elevatedKillTaskImpl(String containerName) 
          throws IOException;

      public static OutputStream create(Path f, boolean append)  
          throws IOException {
        if (!nativeLoaded) {
          throw new IOException("Native WSCE libraries are required for create");
        }
        // 配置Windows文件创建参数
        long desiredAccess = Windows.GENERIC_WRITE;
        long shareMode = 0L;
        long creationDisposition = append ? 
            Windows.OPEN_ALWAYS : Windows.CREATE_ALWAYS;
        long flags = Windows.FILE_ATTRIBUTE_NORMAL;
        
        String fileName = f.toString();
        // 将路径分隔符转换为Windows格式
        fileName = fileName.replace('/', '\\');
        
        long hFile = elevatedCreateImpl(
            fileName, desiredAccess, shareMode, creationDisposition, flags);
        // 将本地文件句柄转换为Java FileDescriptor并包装为输出流
        return new FileOutputStream(
            WinutilsProcessStub.getFileDescriptorFromHandle(hFile));
      }
      
      private static native long elevatedCreateImpl(String path, 
          long desiredAccess, long shareMode,
          long creationDisposition, long flags) throws IOException;
      
      
      public static boolean deleteFile(Path path) throws IOException {
        if (!nativeLoaded) {
          throw new IOException("Native WSCE libraries are required for deleteFile");
        }
        
        return elevatedDeletePathImpl(path.toString(), false);
      }

      public static boolean deleteDirectory(Path path) throws IOException {
        if (!nativeLoaded) {
          throw new IOException("Native WSCE libraries are required for deleteDirectory");
        }
        
        return elevatedDeletePathImpl(path.toString(), true);
      }

      public native static boolean elevatedDeletePathImpl(String path, 
          boolean isDir) throws IOException;
    }

    /**
     * 包装由winutils服务helper启动的进程。
     *
     */
    public static class WinutilsProcessStub extends Process {
      
      private final long hProcess;
      private final long hThread;
      private boolean disposed = false;
      
      private final InputStream stdErr;
      private final InputStream stdOut;
      private final OutputStream stdIn;
      
      public WinutilsProcessStub(long hProcess, long hThread, long hStdIn, 
          long hStdOut, long hStdErr) {
        this.hProcess = hProcess;
        this.hThread = hThread;
        // 将winutils返回的句柄转换为Java标准流
        this.stdIn = new FileOutputStream(getFileDescriptorFromHandle(hStdIn));
        this.stdOut = new FileInputStream(getFileDescriptorFromHandle(hStdOut));
        this.stdErr = new FileInputStream(getFileDescriptorFromHandle(hStdErr));
      }
      
      public static native FileDescriptor getFileDescriptorFromHandle(long handle);
      
      @Override
      public native void destroy();
      
      @Override
      public native int exitValue();
      
      @Override
      public InputStream getErrorStream() {
        return stdErr;
      }
      @Override
      public InputStream getInputStream() {
        return stdOut;
      }
      @Override
      public OutputStream getOutputStream() {
        return stdIn;
      }
      @Override
      public native int waitFor() throws InterruptedException;

      public synchronized native void dispose();

      public native void resume() throws NativeIOException;
    }
    
    /**
     * 以目标用户身份创建任务，加锁保证Windows进程创建线程安全。
     */
    public synchronized static WinutilsProcessStub createTaskAsUser(
        String cwd, String jobName, String user, String pidFile, String cmdLine)
      throws IOException {
      if (!nativeLoaded) {
        throw new IOException(
            "Native WSCE  libraries are required for createTaskAsUser");
      }
      synchronized(Shell.WindowsProcessLaunchLock) {
        return createTaskAsUser0(cwd, jobName, user, pidFile, cmdLine);
      }
    }

    private static native WinutilsProcessStub createTaskAsUser0(
      String cwd, String jobName, String user, String pidFile, String cmdLine)
      throws NativeIOException;
  }

  /**
   * WSCE的shell脚本包装器构建器。
   * 覆盖默认行为，不在脚本中创建PID文件，PID文件由winutils在启动任务时创建。
   */
  private class WindowsSecureWrapperScriptBuilder 
    extends LocalWrapperScriptBuilder {

    public WindowsSecureWrapperScriptBuilder(Path containerWorkDir) {
      super(containerWorkDir);
    }

    @Override
    protected void writeLocalWrapperScript(Path launchDst, Path pidFile, PrintStream pout) {
      pout.format("@call \"%s\"", launchDst);
    }
  }

  /**
   * 用于提升权限操作的包装文件系统。
   * WSCE需要在local/usercache/$user目录下创建容器目录，但该目录本身属于对应用户，权限为750，NodeManager没有写入权限，
   * 因此必须将写入操作委托给特权服务hadoopwintuilsvc处理。
   */
  private static class ElevatedFileSystem extends DelegateToFileSystem {

    /**
     * 覆盖RawLocalFileSystem的部分操作，让这些操作通过特权进程执行。
     * 
     */
    private static class ElevatedRawLocalFilesystem extends RawLocalFileSystem {
      
      @Override
      protected boolean mkOneDirWithMode(Path path, File p2f,
          FsPermission permission) throws IOException {
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("EFS:mkOneDirWithMode: %s %s", path,
              permission));
        }
        boolean ret = false;

        // 保持与File.mkdir一致的行为：不抛异常，仅返回false
        try {
          // 特权创建目录
          Native.Elevated.mkdir(path);
          // 设置目录权限
          setPermission(path, permission);
          ret = true;
        }
        catch(Throwable e) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("EFS:mkOneDirWithMode: %s",
                org.apache.hadoop.util.StringUtils.stringifyException(e)));
          }
        }
        return ret;
      }
      
      @Override
      public void setPermission(Path p, FsPermission permission) 
          throws IOException {
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("EFS:setPermission: %s %s", p, permission));
        }
        // 特权修改权限
        Native.Elevated.chmod(p, permission.toShort());
      }
      
      @Override
      public void setOwner(Path p, String username, String groupname) 
          throws IOException {
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("EFS:setOwner: %s %s %s", 
              p, username, groupname));
        }
        // 特权修改所有者
        Native.Elevated.chown(p, username, groupname);
      }
      
      @Override
      protected OutputStream createOutputStreamWithMode(Path f, boolean append,
          FsPermission permission) throws IOException {
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("EFS:createOutputStreamWithMode: %s %b %s", f,
              append, permission));
        }
        boolean success = false;
        // 特权创建文件
        OutputStream os = Native.Elevated.create(f, append);
        try {
          // 设置文件权限
          setPermission(f, permission);
          success = true;
          return os;
        } finally {
          if (!success) {
            IOUtils.cleanupWithLogger(LOG, os);
          }
        }
      }
      
      @Override
      public boolean delete(Path p, boolean recursive) throws IOException {
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("EFS:delete: %s %b", p, recursive));
        }
        
        File f = pathToFile(p);
        if (!f.exists()) {
          // 路径不存在，返回false表示无文件可删除
          return false;
        }
        else if (f.isFile()) {
          // 特权删除文件
          return Native.Elevated.deleteFile(p);
        } 
        else if (f.isDirectory()) {
          
          // 尽力尝试删除，允许竞态条件（删除过程中新增文件不会影响整体正确性）
          File[] files = FileUtil.listFiles(f);
          int childCount = files.length;
          
          if (recursive) {
            // 递归删除所有子文件
            for(File child:files) {
              if (delete(new Path(child.getPath()), recursive)) {
                --childCount;
              }
            }
          }
          // 所有子文件删除成功后，删除当前目录
          if (childCount == 0) {
            return Native.Elevated.deleteDirectory(p);
          } 
          else {
            throw new IOException("Directory " + f.toString() + " is not empty");
          }
        }
        else {
          // 竞态条件下可能出现：路径存在但既不是文件也不是目录
          throw new IOException("Path " + f.toString() + 
              " exists, but is neither a file nor a directory");
        }
      }
    }

    protected ElevatedFileSystem() throws IOException, URISyntaxException {
      super(FsConstants.LOCAL_FS_URI,
          new ElevatedRawLocalFilesystem(), 
          new Configuration(),
          FsConstants.LOCAL_FS_URI.getScheme(),
          false);
    }
  }
  
  /**
   * 基于Winutils进程桩的命令执行器实现。
   */
  private static class WintuilsProcessStubExecutor 
  implements Shell.CommandExecutor {
    private Native.WinutilsProcessStub processStub;
    private StringBuilder output = new StringBuilder();
    private int exitCode;
    
    private enum State {
      INIT,
      RUNNING,
      COMPLETE
    };
    
    private State state;
    
    private final String cwd;
    private final String jobName;
    private final String userName;
    private final String pidFile;
    private final String cmdLine;

    public WintuilsProcessStubExecutor(
        String cwd, 
        String jobName, 
        String userName, 
        String pidFile,
        String cmdLine) {
      this.cwd = cwd;
      this.jobName = jobName;
      this.userName = userName;
      this.pidFile = pidFile;
      this.cmdLine = cmdLine;
      this.state = State.INIT;
    }    
    
    /**
     * 检查进程是否已执行完成，未完成则抛出异常。
     */
    private void assertComplete() throws IOException {
      if (state != State.COMPLETE) {
        throw new IOException("Process is not complete");
      }
    }
    
    public String getOutput () throws IOException {
      assertComplete();
      return output.toString();
    }
    
    public int getExitCode() throws IOException {
      assertComplete();
      return exitCode;
    }
    
    public void validateResult() throws IOException {
      assertComplete();
      if (0 != exitCode) {
        LOG.warn(output.toString());
        throw new IOException("Processs exit code is:" + exitCode);
      }
    }
    
    /**
     * 启动线程读取进程输出，收集到output缓冲区。
     */
    private Thread startStreamReader(final InputStream stream) 
        throws IOException {
      Thread streamReaderThread = new SubjectInheritingThread() {
        
        @Override
        public void work() {
          try (BufferedReader lines = new BufferedReader(
                   new InputStreamReader(stream, StandardCharsets.UTF_8))) {
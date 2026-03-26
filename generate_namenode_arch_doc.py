#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hadoop NameNode 核心架构设计文档生成脚本
基于 release-3.3.5-RC0 版本源码深度分析
"""
import os
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.lib.colors import HexColor, black, white, grey
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
)
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.graphics.shapes import Drawing, Line, Rect, String

# === 字体注册 ===
def register_fonts():
    for fp in ["/System/Library/Fonts/STHeiti Light.ttc", "/System/Library/Fonts/PingFang.ttc",
               "/System/Library/Fonts/Hiragino Sans GB.ttc", "/Library/Fonts/Arial Unicode.ttf"]:
        if os.path.exists(fp):
            try:
                pdfmetrics.registerFont(TTFont('CF', fp))
                pdfmetrics.registerFont(TTFont('CFB', fp))
                return
            except Exception:
                continue
    pdfmetrics.registerAlias('CF', 'Helvetica')
    pdfmetrics.registerAlias('CFB', 'Helvetica-Bold')
register_fonts()

# === 颜色与样式 ===
DB = HexColor('#1a237e'); MB = HexColor('#1565c0'); LB = HexColor('#e3f2fd')
AG = HexColor('#2e7d32'); AO = HexColor('#e65100'); AP = HexColor('#7b1fa2')
TH_BG = HexColor('#1565c0'); ALT = HexColor('#f5f5f5'); BD = HexColor('#bdbdbd')

s_title = ParagraphStyle('T', fontName='CFB', fontSize=28, leading=36, alignment=TA_CENTER, textColor=DB, spaceAfter=6*mm)
s_sub = ParagraphStyle('S', fontName='CF', fontSize=14, leading=20, alignment=TA_CENTER, textColor=HexColor('#616161'), spaceAfter=10*mm)
s_h1 = ParagraphStyle('H1', fontName='CFB', fontSize=20, leading=28, textColor=DB, spaceBefore=12*mm, spaceAfter=6*mm)
s_h2 = ParagraphStyle('H2', fontName='CFB', fontSize=15, leading=22, textColor=MB, spaceBefore=8*mm, spaceAfter=4*mm)
s_h3 = ParagraphStyle('H3', fontName='CFB', fontSize=13, leading=18, textColor=AG, spaceBefore=6*mm, spaceAfter=3*mm)
s_body = ParagraphStyle('B', fontName='CF', fontSize=10.5, leading=17, alignment=TA_JUSTIFY, spaceBefore=1*mm, spaceAfter=2*mm)
s_code = ParagraphStyle('C', fontName='Courier', fontSize=8.5, leading=12.5, leftIndent=8*mm, backColor=HexColor('#f5f5f5'), borderWidth=0.5, borderColor=BD, borderPadding=4)
s_bull = ParagraphStyle('BL', fontName='CF', fontSize=10.5, leading=16, leftIndent=12*mm, bulletIndent=6*mm, spaceBefore=1*mm, spaceAfter=1*mm)
s_toc = ParagraphStyle('TOC', fontName='CF', fontSize=12, leading=20, leftIndent=8*mm, spaceBefore=2*mm, spaceAfter=2*mm, textColor=MB)
s_th = ParagraphStyle('TH', fontName='CFB', fontSize=10, leading=14, alignment=TA_CENTER, textColor=white)
s_tc = ParagraphStyle('TC', fontName='CF', fontSize=9.5, leading=14)
s_cap = ParagraphStyle('CAP', fontName='CF', fontSize=9, leading=13, alignment=TA_CENTER, textColor=HexColor('#757575'), spaceBefore=2*mm, spaceAfter=4*mm)

def T(headers, rows, cw=None):
    h = [Paragraph(x, s_th) for x in headers]
    b = [[Paragraph(str(c), s_tc) for c in r] for r in rows]
    d = [h] + b
    cw = cw or [170*mm/len(headers)]*len(headers)
    t = Table(d, colWidths=cw, repeatRows=1)
    sc = [('BACKGROUND',(0,0),(-1,0),TH_BG),('TEXTCOLOR',(0,0),(-1,0),white),
          ('FONTNAME',(0,0),(-1,-1),'CF'),('FONTSIZE',(0,0),(-1,-1),9.5),
          ('GRID',(0,0),(-1,-1),0.5,BD),('VALIGN',(0,0),(-1,-1),'MIDDLE'),
          ('BOTTOMPADDING',(0,0),(-1,0),8),('TOPPADDING',(0,0),(-1,0),8),
          ('BOTTOMPADDING',(0,1),(-1,-1),5),('TOPPADDING',(0,1),(-1,-1),5)]
    for i in range(1,len(d)):
        if i%2==0: sc.append(('BACKGROUND',(0,i),(-1,i),ALT))
    t.setStyle(TableStyle(sc))
    return t

def box(d,x,y,w,h,txt,fc,tc=black,fs=8):
    d.add(Rect(x,y,w,h,fillColor=fc,strokeColor=HexColor('#90a4ae'),strokeWidth=0.5,rx=3,ry=3))
    d.add(String(x+w/2,y+h/2-fs/3,txt,fontName='CF',fontSize=fs,fillColor=tc,textAnchor='middle'))

def on_page(canvas, doc):
    canvas.saveState()
    canvas.setFont('CF', 8)
    canvas.setFillColor(HexColor('#9e9e9e'))
    canvas.drawString(20*mm, A4[1]-12*mm, 'Hadoop NameNode 核心架构设计文档')
    canvas.drawRightString(A4[0]-20*mm, A4[1]-12*mm, f'第 {doc.page} 页')
    canvas.line(20*mm, A4[1]-14*mm, A4[0]-20*mm, A4[1]-14*mm)
    canvas.drawCentredString(A4[0]/2, 12*mm, 'release-3.3.5-RC0 | NameNode Architecture')
    canvas.restoreState()

# === 架构图 ===
def draw_layered_arch():
    d = Drawing(480, 380)
    d.add(Rect(0,0,480,380,fillColor=HexColor('#fafafa'),strokeColor=None))
    d.add(String(240,365,'NameNode 四层架构',fontName='CFB',fontSize=14,fillColor=DB,textAnchor='middle'))
    # RPC层
    d.add(Rect(15,310,450,45,fillColor=HexColor('#e8f5e9'),strokeColor=AG,strokeWidth=2,rx=8))
    d.add(String(240,340,'RPC 接入层 — NameNodeRpcServer',fontName='CFB',fontSize=11,fillColor=AG,textAnchor='middle'))
    box(d,25,315,110,20,'clientRpcServer',HexColor('#c8e6c9'),AG); box(d,145,315,120,20,'serviceRpcServer',HexColor('#c8e6c9'),AG)
    box(d,275,315,120,20,'lifelineRpcServer',HexColor('#c8e6c9'),AG); box(d,405,315,50,20,'HTTP',HexColor('#c8e6c9'),AG)
    d.add(Line(240,310,240,300,strokeColor=grey,strokeWidth=1.5))
    # 核心业务层
    d.add(Rect(15,200,450,95,fillColor=LB,strokeColor=MB,strokeWidth=2,rx=8))
    d.add(String(240,280,'核心业务逻辑层 — FSNamesystem（门面模式）',fontName='CFB',fontSize=11,fillColor=DB,textAnchor='middle'))
    comps = [('LeaseManager',25,255,80,20),('CacheManager',115,255,85,20),('SnapshotManager',210,255,95,20),
             ('EncryptionZoneMgr',315,255,100,20),('SafeMode',425,255,30,20),
             ('FSNamesystemLock',25,228,95,20),('OperationCategory',130,228,100,20),('ErasureCoding',240,228,90,20),('SPS',340,228,50,20),('AuditLog',400,228,55,20)]
    for label,x,y,w,h in comps:
        d.add(Rect(x,y,w,h,fillColor=white,strokeColor=MB,rx=3))
        d.add(String(x+w/2,y+h/2-3,label,fontName='CF',fontSize=7,fillColor=DB,textAnchor='middle'))
    d.add(Line(120,200,120,190,strokeColor=grey,strokeWidth=1.5)); d.add(Line(240,200,240,190,strokeColor=grey,strokeWidth=1.5)); d.add(Line(360,200,360,190,strokeColor=grey,strokeWidth=1.5))
    # 数据管理层
    d.add(Rect(15,95,450,90,fillColor=HexColor('#fff3e0'),strokeColor=AO,strokeWidth=2,rx=8))
    d.add(String(240,173,'数据管理层',fontName='CFB',fontSize=11,fillColor=AO,textAnchor='middle'))
    for label,x,y,w,h in [('FSDirectory',25,130,115,35),('BlockManager',155,130,130,35),('FSEditLog/FSImage',300,130,150,35)]:
        d.add(Rect(x,y,w,h,fillColor=white,strokeColor=AO,strokeWidth=1.5,rx=5))
        d.add(String(x+w/2,y+h/2-3,label,fontName='CFB',fontSize=9,fillColor=AO,textAnchor='middle'))
    for label,x,y,w,h in [('INodeFile',25,100,70,25),('INodeDirectory',105,100,85,25),('INodeReference',200,100,85,25),('INodeMap',295,100,70,25),('NNStorage',375,100,70,25)]:
        d.add(Rect(x,y,w,h,fillColor=white,strokeColor=AO,rx=3))
        d.add(String(x+w/2,y+h/2-3,label,fontName='CF',fontSize=7,fillColor=AO,textAnchor='middle'))
    # HA层
    d.add(Rect(15,15,210,70,fillColor=HexColor('#f3e5f5'),strokeColor=AP,strokeWidth=2,rx=8))
    d.add(String(120,70,'HA 高可用层',fontName='CFB',fontSize=10,fillColor=AP,textAnchor='middle'))
    for label,x,y,w,h in [('HAState',25,45,55,20),('ActiveState',90,45,65,20),('StandbyState',165,45,50,20),('EditLogTailer',25,20,80,20),('StandbyCheckpointer',115,20,100,20)]:
        d.add(Rect(x,y,w,h,fillColor=white,strokeColor=AP,rx=3))
        d.add(String(x+w/2,y+h/2-3,label,fontName='CF',fontSize=7,fillColor=AP,textAnchor='middle'))
    d.add(Rect(240,15,220,70,fillColor=HexColor('#efebe9'),strokeColor=HexColor('#795548'),strokeWidth=2,rx=8))
    d.add(String(350,70,'子模块',fontName='CFB',fontSize=10,fillColor=HexColor('#795548'),textAnchor='middle'))
    for label,x,y,w,h in [('snapshot/',250,45,60,20),('metrics/',320,45,55,20),('top/',385,45,30,20),('sps/',425,45,25,20),('startupprogress/',250,20,95,20),('fgl/',355,20,30,20),('visitor/',395,20,50,20)]:
        d.add(Rect(x,y,w,h,fillColor=white,strokeColor=HexColor('#795548'),rx=3))
        d.add(String(x+w/2,y+h/2-3,label,fontName='CF',fontSize=7,fillColor=HexColor('#795548'),textAnchor='middle'))
    return d

def draw_inode_hierarchy():
    d = Drawing(480,240)
    d.add(Rect(0,0,480,240,fillColor=HexColor('#fafafa'),strokeColor=None))
    d.add(String(240,225,'INode 类继承与内存布局',fontName='CFB',fontSize=13,fillColor=DB,textAnchor='middle'))
    d.add(Rect(175,190,130,28,fillColor=HexColor('#f3e5f5'),strokeColor=AP,strokeWidth=1.5,rx=5))
    d.add(String(240,200,'INode (抽象基类)',fontName='CFB',fontSize=10,fillColor=AP,textAnchor='middle'))
    d.add(Rect(110,145,260,35,fillColor=HexColor('#e8eaf6'),strokeColor=MB,strokeWidth=1.5,rx=5))
    d.add(String(240,168,'INodeWithAdditionalFields',fontName='CFB',fontSize=10,fillColor=DB,textAnchor='middle'))
    d.add(String(240,153,'permission(64bit): MODE(16)|GROUP(24)|USER(24)',fontName='CF',fontSize=7,fillColor=grey,textAnchor='middle'))
    d.add(Line(240,190,240,180,strokeColor=grey,strokeWidth=1))
    d.add(Rect(15,75,195,50,fillColor=HexColor('#e8f5e9'),strokeColor=AG,strokeWidth=1.5,rx=5))
    d.add(String(113,112,'INodeFile',fontName='CFB',fontSize=10,fillColor=AG,textAnchor='middle'))
    d.add(String(113,98,'header(64bit): StoragePolicy(4)',fontName='CF',fontSize=7,fillColor=grey,textAnchor='middle'))
    d.add(String(113,88,'+BlockType(2)+Replica(12)+BlockSize(48)',fontName='CF',fontSize=7,fillColor=grey,textAnchor='middle'))
    d.add(Line(170,145,113,125,strokeColor=grey,strokeWidth=1))
    d.add(Rect(270,75,195,50,fillColor=HexColor('#fff3e0'),strokeColor=AO,strokeWidth=1.5,rx=5))
    d.add(String(368,112,'INodeDirectory',fontName='CFB',fontSize=10,fillColor=AO,textAnchor='middle'))
    d.add(String(368,98,'children: ArrayList<INode>',fontName='CF',fontSize=7,fillColor=grey,textAnchor='middle'))
    d.add(String(368,88,'Feature[]: Snapshot/ACL/XAttr',fontName='CF',fontSize=7,fillColor=grey,textAnchor='middle'))
    d.add(Line(310,145,368,125,strokeColor=grey,strokeWidth=1))
    d.add(Rect(20,15,160,40,fillColor=HexColor('#fce4ec'),strokeColor=HexColor('#c62828'),strokeWidth=1.5,rx=5))
    d.add(String(100,42,'INodeReference',fontName='CFB',fontSize=9,fillColor=HexColor('#c62828'),textAnchor='middle'))
    d.add(String(100,28,'WithName→WithCount→referred',fontName='CF',fontSize=7,fillColor=grey,textAnchor='middle'))
    d.add(Line(170,145,100,55,strokeColor=grey,strokeWidth=1,strokeDashArray=[3,2]))
    d.add(Rect(320,15,140,40,fillColor=HexColor('#e8eaf6'),strokeColor=MB,rx=5))
    d.add(String(390,42,'INodeMap(LightWeightGSet)',fontName='CFB',fontSize=8,fillColor=MB,textAnchor='middle'))
    d.add(String(390,28,'ID→INode O(1)查找',fontName='CF',fontSize=7,fillColor=grey,textAnchor='middle'))
    return d

def draw_ha_state():
    d = Drawing(460,210)
    d.add(Rect(0,0,460,210,fillColor=HexColor('#fafafa'),strokeColor=None))
    d.add(String(230,195,'HA 状态模式转换图',fontName='CFB',fontSize=13,fillColor=DB,textAnchor='middle'))
    d.add(Rect(155,160,150,28,fillColor=HexColor('#f3e5f5'),strokeColor=AP,strokeWidth=1.5,rx=8))
    d.add(String(230,170,'HAState (抽象基类)',fontName='CFB',fontSize=10,fillColor=AP,textAnchor='middle'))
    d.add(Rect(20,85,130,60,fillColor=LB,strokeColor=MB,strokeWidth=2,rx=8))
    d.add(String(85,130,'StandbyState',fontName='CFB',fontSize=10,fillColor=MB,textAnchor='middle'))
    d.add(String(85,115,'拒绝WRITE操作',fontName='CF',fontSize=7,fillColor=grey,textAnchor='middle'))
    d.add(String(85,103,'EditLogTailer拉取日志',fontName='CF',fontSize=7,fillColor=grey,textAnchor='middle'))
    d.add(Rect(310,85,130,60,fillColor=HexColor('#e8f5e9'),strokeColor=AG,strokeWidth=2,rx=8))
    d.add(String(375,130,'ActiveState',fontName='CFB',fontSize=10,fillColor=AG,textAnchor='middle'))
    d.add(String(375,115,'允许所有操作',fontName='CF',fontSize=7,fillColor=grey,textAnchor='middle'))
    d.add(String(375,103,'populateReplQueues=true',fontName='CF',fontSize=7,fillColor=grey,textAnchor='middle'))
    d.add(Rect(165,15,130,50,fillColor=HexColor('#fff3e0'),strokeColor=AO,strokeWidth=2,rx=8))
    d.add(String(230,50,'Observer',fontName='CFB',fontSize=10,fillColor=AO,textAnchor='middle'))
    d.add(String(230,35,'READ允许,WRITE重定向',fontName='CF',fontSize=7,fillColor=grey,textAnchor='middle'))
    d.add(Line(230,160,85,145,strokeColor=grey,strokeWidth=1,strokeDashArray=[3,2]))
    d.add(Line(230,160,375,145,strokeColor=grey,strokeWidth=1,strokeDashArray=[3,2]))
    d.add(Line(230,160,230,65,strokeColor=grey,strokeWidth=1,strokeDashArray=[3,2]))
    d.add(Line(150,125,310,125,strokeColor=AG,strokeWidth=2))
    d.add(String(230,130,'failover',fontName='CF',fontSize=8,fillColor=AG,textAnchor='middle'))
    d.add(Line(310,100,150,100,strokeColor=MB,strokeWidth=2))
    d.add(String(230,90,'graceful failover',fontName='CF',fontSize=8,fillColor=MB,textAnchor='middle'))
    return d

# === 文档构建 ===
def build():
    e = []
    # 封面
    e.append(Spacer(1,50*mm)); e.append(Paragraph('Hadoop NameNode',s_title)); e.append(Paragraph('核心架构设计文档',s_title))
    e.append(Spacer(1,8*mm)); e.append(Paragraph('基于 release-3.3.5-RC0 版本源码深度分析',s_sub)); e.append(Spacer(1,12*mm))
    info = [['项目','Apache Hadoop HDFS'],['模块','hadoop-hdfs / server / namenode'],['版本','release-3.3.5-RC0'],
            ['范围','NameNode主目录+8个子目录(ha/snapshot/metrics/sps/fgl/top/visitor/startupprogress)'],['文件数','~150+ Java源文件'],['工具','自动化源码架构分析']]
    it = Table([[Paragraph(r[0],s_th),Paragraph(r[1],s_tc)] for r in info],colWidths=[50*mm,120*mm])
    it.setStyle(TableStyle([('BACKGROUND',(0,0),(0,-1),TH_BG),('TEXTCOLOR',(0,0),(0,-1),white),('BACKGROUND',(1,0),(1,-1),white),
                            ('GRID',(0,0),(-1,-1),0.5,BD),('VALIGN',(0,0),(-1,-1),'MIDDLE'),('TOPPADDING',(0,0),(-1,-1),6),('BOTTOMPADDING',(0,0),(-1,-1),6)]))
    e.append(it); e.append(PageBreak())
    # 目录
    e.append(Paragraph('目录',s_h1))
    for t in ['一、模块概述','二、核心分层架构','三、NameNode启动流程','四、FSNamesystem—门面模式与核心状态机','五、INode层次与内存优化',
              '六、HA高可用机制—状态模式','七、编辑日志与持久化机制','八、FSNamesystemLock读写锁','九、核心流程时序分析',
              '十、设计模式总结','十一、关键场景调用链','十二、子模块概览','十三、核心源文件清单','十四、设计亮点与总结']:
        e.append(Paragraph(t,s_toc))
    e.append(PageBreak())

    # 一、模块概述
    e.append(Paragraph('一、模块概述',s_h1))
    e.append(Paragraph('NameNode 是 HDFS 的核心元数据管理节点，负责维护整个分布式文件系统的命名空间（Namespace）。'
        '它管理文件→块映射、块→DataNode副本位置、目录树结构、权限控制、租约（Lease）、快照（Snapshot）、'
        '加密区域等所有元数据信息。模块位于 hadoop-hdfs/server/namenode 目录，包含约150+个Java源文件，代码总量超15万行。'
        '其中 FSNamesystem.java（341KB，约9200行）是HDFS中最大的单文件。',s_body))
    e.append(Paragraph('1.1 核心目标',s_h2))
    for b in ['<b>命名空间管理</b>：维护HDFS文件系统目录树（INode层次），支持文件/目录增删改查',
              '<b>块映射管理</b>：维护文件→数据块、块→DataNode的映射关系，协调副本放置',
              '<b>元数据持久化</b>：通过FSEditLog（编辑日志）和FSImage（文件系统镜像）实现可靠持久化',
              '<b>高可用（HA）</b>：支持Active/Standby/Observer三种角色，实现故障自动转移',
              '<b>快照管理</b>：支持目录级快照，实现元数据时间点回溯',
              '<b>安全与加密</b>：Kerberos认证、ACL、加密区域、安全Token管理',
              '<b>缓存管理</b>：DataNode缓存指令，加速热点数据访问']:
        e.append(Paragraph(f'• {b}',s_bull))
    e.append(PageBreak())

    # 二、核心分层架构
    e.append(Paragraph('二、核心分层架构',s_h1))
    e.append(Paragraph('NameNode模块采用清晰的四层架构设计：RPC接入层→核心业务逻辑层→数据管理层→HA+持久化层。',s_body))
    e.append(Paragraph('2.1 分层架构全景图',s_h2))
    e.append(draw_layered_arch()); e.append(Paragraph('图2-1：NameNode四层架构全景图',s_cap))
    e.append(Paragraph('2.2 RPC三通道设计',s_h2))
    e.append(T(['RPC Server','监听端口配置','职责','注册的协议'],
        [['clientRpcServer','dfs.namenode.rpc-address','处理客户端文件操作','ClientNamenodeProtocol等'],
         ['serviceRpcServer','dfs.namenode.servicerpc-address','处理DataNode和NN请求','DatanodeProtocol, NamenodeProtocol, HAServiceProtocol'],
         ['lifelineRpcServer','dfs.namenode.lifeline.rpc-address','HA健康检查专用通道','HAServiceProtocol(精简版)']],
        cw=[32*mm,42*mm,40*mm,56*mm]))
    e.append(Paragraph('<b>设计意图</b>：lifeline通道确保即使client/serviceRpc因请求积压繁忙，ZKFC仍能通过独立lifeline通道检测NameNode存活，避免不必要的HA故障转移。',s_body))
    e.append(PageBreak())

    # 三、启动流程
    e.append(Paragraph('三、NameNode 启动流程',s_h1))
    e.append(Paragraph('入口：NameNode.main() → createNameNode() → new NameNode(conf) → initialize()。',s_body))
    e.append(Paragraph('3.1 createNameNode() — 启动选项分发',s_h2))
    e.append(T(['StartupOption','操作','说明'],
        [['FORMAT','format(conf,...)','格式化文件系统，初始化存储目录'],['GENCLUSTERID','NNStorage.newClusterID()','生成新集群ID'],
         ['ROLLBACK','doRollback(conf,true)','回滚到升级前版本'],['BOOTSTRAPSTANDBY','BootstrapStandby.run()','从Active拷贝FSImage初始化Standby'],
         ['INITIALIZESHAREDEDITS','initializeSharedEdits()','初始化QJM共享编辑日志'],['default','new NameNode(conf)','正常启动，进入initialize()']],
        cw=[38*mm,52*mm,80*mm]))
    e.append(Paragraph('3.2 initialize() — 核心初始化序列',s_h2))
    e.append(T(['步骤','方法','说明'],
        [['1','loginAsNameNodeUser()','Kerberos认证登录'],['2','NameNode.initMetrics()','初始化指标监控和JVM暂停监控器'],
         ['3','startHttpServer()','启动HTTP/Web UI服务'],['4','loadNamesystem(conf)','【关键】加载FSImage+重放EditLog恢复命名空间'],
         ['5','createRpcServer()','【关键】创建三个RPC Server，注册所有协议'],['6','startCommonServices()','【关键】启动BlockManager、RPC监听、ServicePlugin']],
        cw=[12*mm,55*mm,103*mm]))
    e.append(Paragraph('<b>关键要点</b>：startCommonServices()内部启动BlockManager的ReplicationMonitor、HeartbeatManager、'
        'DecommissionManager等，然后rpcServer.start()开始接受外部请求。至此NameNode完全就绪。',s_body))
    e.append(PageBreak())

    # 四、FSNamesystem
    e.append(Paragraph('四、FSNamesystem — 门面模式与核心状态机',s_h1))
    e.append(Paragraph('FSNamesystem（341KB，~9200行）是NameNode核心类，扮演<b>门面角色（Facade Pattern）</b>，'
        '为NameNodeRpcServer提供统一业务入口，内部协调FSDirectory、BlockManager、FSEditLog、LeaseManager、'
        'SnapshotManager、CacheManager、EncryptionZoneManager等子系统。',s_body))
    e.append(Paragraph('4.1 操作分类与HA检查',s_h2))
    e.append(T(['操作类别','允许的HA状态','典型操作'],
        [['READ','Active, Observer','文件状态查询、目录列表、块位置获取'],
         ['WRITE','仅Active','文件创建/删除/重命名、权限修改、块分配'],
         ['CHECKPOINT','Active','SaveNamespace、滚动编辑日志'],
         ['UNCHECKED','所有状态','安全Token操作等']],cw=[28*mm,52*mm,90*mm]))
    e.append(Paragraph('4.2 双重锁机制',s_h2))
    e.append(Paragraph('<b>加锁模式</b>：先获取FSNamesystem锁（RwLockMode.FS）→ 再获取FSDirectory锁（dir.writeLock）→ 避免死锁。<br/>'
        '<b>典型写操作</b>：writeLock(FS) → checkOperation(WRITE) → checkSafeMode() → dir.writeLock() → 执行修改 → dir.writeUnlock() → writeUnlock(FS) → logSync()',s_body))
    e.append(Paragraph('4.3 安全模式（SafeMode）',s_h2))
    e.append(Paragraph('启动后进入安全模式，只允许读操作。退出条件：已报告块数/总块数 >= 0.999（默认），加上30秒扩展等待。安全模式下所有写操作被拒绝并抛出SafeModeException。',s_body))
    e.append(PageBreak())

    # 五、INode层次
    e.append(Paragraph('五、文件系统树 — INode层次与内存优化',s_h1))
    e.append(Paragraph('5.1 INode类继承体系',s_h2))
    e.append(draw_inode_hierarchy()); e.append(Paragraph('图5-1：INode类继承层次与内存布局',s_cap))
    e.append(Paragraph('5.2 PermissionStatusFormat — 位压缩权限',s_h2))
    e.append(T(['字段','位范围','位宽','说明'],
        [['MODE','bit 0~15','16bit','Unix文件权限(rwxrwxrwx+特殊位)'],
         ['GROUP','bit 16~39','24bit','通过SerialNumberManager映射到组名'],
         ['USER','bit 40~63','24bit','通过SerialNumberManager映射到用户名']],cw=[28*mm,28*mm,18*mm,96*mm]))
    e.append(Paragraph('<b>SerialNumberManager</b>：String→int双向映射，所有INode共享同一映射表。每个INode仅需8字节存储完整权限信息。',s_body))
    e.append(Paragraph('5.3 INodeFile — HeaderFormat位编码',s_h2))
    e.append(T(['字段','位范围','说明'],
        [['PREFERRED_BLOCK_SIZE','低48bit','首选块大小(字节)'],['REPLICATION/EC_POLICY_ID','中间12bit','副本数或EC策略ID'],
         ['BLOCK_LAYOUT','2bit','块布局(CONTIGUOUS/STRIPED)'],['STORAGE_POLICY_ID','高4bit','存储策略ID']],cw=[48*mm,30*mm,92*mm]))
    e.append(Paragraph('5.4 INodeReference — 快照引用链',s_h2))
    e.append(Paragraph('三层引用链设计解决快照+重命名场景：<b>WithName</b>（名称引用）→ <b>WithCount</b>（引用计数）→ referred（实际INode）。'
        'DstReference处理重命名目标。多路径通过WithCount共享底层INode。',s_body))
    e.append(Paragraph('5.5 INodeMap — O(1)全局查找',s_h2))
    e.append(Paragraph('使用LightWeightGSet（开放寻址哈希集合）实现INode ID到INode对象O(1)查找。匿名INode查找机制避免额外Key对象分配。',s_body))
    e.append(PageBreak())

    # 六、HA
    e.append(Paragraph('六、HA 高可用机制 — 状态模式',s_h1))
    e.append(draw_ha_state()); e.append(Paragraph('图6-1：HA状态机转换图',s_cap))
    e.append(Paragraph('6.1 setStateInternal() — 模板方法',s_h2))
    e.append(T(['步骤','方法','说明','持锁'],
        [['①','prepareToExitState()','准备退出当前状态','无锁'],['②','s.prepareToEnterState()','准备进入新状态','无锁'],
         ['③','context.writeLock()','获取全局写锁','加锁'],['④','exitState()','退出当前状态（停服务）','持锁'],
         ['⑤','context.setState(s)','原子设置新状态','持锁'],['⑥','s.enterState()','进入新状态（启服务）','持锁'],
         ['⑦','updateLastHATransitionTime()','记录转换时间戳','持锁']],cw=[12*mm,50*mm,68*mm,15*mm]))
    e.append(Paragraph('6.2 三种状态行为差异',s_h2))
    e.append(T(['特性','ActiveState','StandbyState','Observer'],
        [['允许操作','READ+WRITE','UNCHECKED+StaleREAD','READ(重定向WRITE)'],
         ['填充副本队列','是','否','否'],['enterState()','startActiveServices()','startStandbyServices()','startStandbyServices()'],
         ['EditLogTailer','不需要','运行(拉取日志)','运行(拉取日志)'],['StandbyCheckpointer','不需要','运行(定期合并)','运行']],
        cw=[32*mm,42*mm,48*mm,48*mm]))
    e.append(PageBreak())

    # 七、编辑日志
    e.append(Paragraph('七、编辑日志与持久化机制',s_h1))
    e.append(Paragraph('7.1 FSEditLog 状态机',s_h2))
    e.append(T(['状态','含义','允许操作'],
        [['UNINITIALIZED','未初始化','→BETWEEN_LOG_SEGMENTS'],['BETWEEN_LOG_SEGMENTS','日志段间隔','→IN_SEGMENT或→OPEN_FOR_READING'],
         ['IN_SEGMENT','正在写入日志段','logEdit()/logSync(); →BETWEEN_LOG_SEGMENTS'],
         ['OPEN_FOR_READING','只读(Standby)','仅读取'],['CLOSED','已关闭','终态']],cw=[42*mm,35*mm,93*mm]))
    e.append(Paragraph('7.2 logSync() — 批量同步',s_h2))
    e.append(T(['阶段','操作','持锁','说明'],
        [['等待','while(mytxid>synctxid && isSyncRunning) wait()','sync','等待其他线程同步完成'],
         ['检查','if(mytxid<=synctxid) return','sync','事务已被批量同步覆盖'],
         ['交换','editLogStream.setReadyToFlush()','sync','交换双缓冲区A↔B'],
         ['刷盘','logStream.flush()','无锁','实际I/O持久化(耗时)'],
         ['更新','synctxid=syncStart; notifyAll()','sync','更新已同步txid并唤醒']],cw=[16*mm,58*mm,14*mm,82*mm]))
    e.append(Paragraph('<b>核心设计</b>：双缓冲区让写入和刷盘并行；批量同步减少I/O；Fail-Fast策略防数据不一致；JournalSet多副本写入提供冗余。',s_body))
    e.append(Paragraph('7.3 FSImage — 检查点持久化',s_h2))
    e.append(Paragraph('启动时加载最新FSImage→重放EditLog恢复命名空间。saveNamespace()将内存序列化到fsimage_txid文件。'
        'StandbyCheckpointer定期合并FSImage+EditLog生成新Checkpoint并上传到Active NN。HA场景延迟创建VERSION文件。',s_body))
    e.append(PageBreak())

    # 八、FSNamesystemLock
    e.append(Paragraph('八、FSNamesystemLock — 读写锁机制',s_h1))
    e.append(Paragraph('FSNamesystemLock（386行）封装ReentrantReadWriteLock，增加丰富的监控诊断：',s_body))
    e.append(T(['特性','实现','说明'],
        [['读锁计时','ThreadLocal<Long> readLockHeldTimeMs','每个读锁线程的持有时间'],
         ['写锁计时','writeLockHeldTimeMs','当前写锁持有时间'],
         ['慢锁日志','readLockReportingThresholdMs','超阈值输出警告日志'],
         ['最长读锁','longestReadLockHeldInfo(AtomicReference)','CAS无锁方式记录历史最长读锁'],
         ['日志限流','LogThrottlingHelper','限制慢锁日志输出频率'],
         ['锁持有者','writeLockOwnerThread','记录写锁持有者线程名']],cw=[28*mm,62*mm,80*mm]))
    e.append(PageBreak())

    # 九、核心流程时序
    e.append(Paragraph('九、核心流程时序分析',s_h1))
    e.append(Paragraph('9.1 文件创建 startFileInt() 详细流程',s_h2))
    e.append(Paragraph('1. 路径校验：DFSUtil.isValidName(src) + 保留名检查<br/>'
        '2. 副本策略：SHOULD_REPLICATE → verifyReplication() / 否则检查EC策略<br/>'
        '3. 获取写锁：writeLock(RwLockMode.FS)<br/>'
        '4. HA+SafeMode检查：checkOperation(WRITE) + checkNameNodeSafeMode()<br/>'
        '5. 路径解析：FSDirWriteFileOp.resolvePathForStartFile()<br/>'
        '6. 加密区域：释放锁→生成EDEK→重新获取锁→重新解析路径<br/>'
        '7. 目录树修改：dir.writeLock() → FSDirWriteFileOp.startFile() → dir.writeUnlock()<br/>'
        '8. 日志同步：释放锁后 → getEditLog().logSync()',s_body))
    e.append(Paragraph('9.2 DataNode心跳处理 — BPServiceActor.offerService()',s_h2))
    e.append(Paragraph('每轮循环（~3秒）：① 发送心跳sendHeartBeat() → ② 更新HA状态 → ③ 处理NN命令（KeyUpdate优先入队）'
        '→ ④ 发送IBR增量块报告 → ⑤ 全量块报告（默认6小时，租约机制）→ ⑥ 缓存报告 → ⑦ 等待',s_body))
    e.append(Paragraph('9.3 块报告处理 — processReport()',s_h2))
    e.append(Paragraph('持有全局写锁(RwLockMode.GLOBAL)。首次报告processFirstBlockReport()高效处理加速启动。'
        '后续报告通过reportDiff()计算五个差异队列：toAdd/toRemove/toInvalidate/toCorrupt/toUC。',s_body))
    e.append(PageBreak())

    # 十、设计模式
    e.append(Paragraph('十、设计模式总结',s_h1))
    e.append(T(['设计模式','应用场景','关键类','设计意图'],
        [['门面模式','NameNode核心入口','FSNamesystem','统一入口协调FSDirectory/BlockManager/EditLog等子系统'],
         ['状态模式','HA状态管理','HAState/ActiveState/StandbyState','Active/Standby/Observer行为封装在独立状态类中'],
         ['模板方法','HA状态转换','HAState.setStateInternal()','定义prepareExit→exit→setState→enter的固定步骤'],
         ['策略模式','块副本放置','BlockPlacementPolicy','副本放置算法可插拔，配置切换'],
         ['组合模式','文件系统树','INode/INodeFile/INodeDirectory','统一文件/目录操作接口，支持递归'],
         ['装饰器模式','INode特性扩展','Feature接口(Acl/XAttr...)','Feature[]动态扩展INode功能'],
         ['观察者模式','编辑日志多副本','JournalSet+JournalManager[]','logEdit广播到所有Journal'],
         ['双缓冲区','编辑日志同步','FSEditLog','写入和刷盘并行，批量同步提升吞吐'],
         ['租约模式','文件写入控制','LeaseManager','软/硬限制控制写入并发'],
         ['写时复制','快照管理','DirectoryWithSnapshotFeature','快照存在时记录diff，不复制整树']],
        cw=[28*mm,26*mm,56*mm,60*mm]))
    e.append(Paragraph('表10-1：NameNode核心设计模式一览',s_cap))
    e.append(PageBreak())

    # 十一、调用链
    e.append(Paragraph('十一、关键场景调用链',s_h1))
    chains = [
        ('11.1 文件创建','Client.create() → DFSClient → NameNodeRpcServer.create() → FSNamesystem.startFile() → startFileInt() → FSDirWriteFileOp.startFile() → FSEditLog.logOpenFile() → logSync()'),
        ('11.2 块分配','Client.addBlock() → NameNodeRpcServer.addBlock() → FSNamesystem.getAdditionalBlock() → BlockManager.chooseTarget4NewBlock() → BlockPlacementPolicyDefault.chooseTargetInOrder()'),
        ('11.3 心跳处理','BPServiceActor.offerService() → sendHeartBeat() → DatanodeProtocol.sendHeartbeat() → NameNodeRpcServer.sendHeartbeat() → FSNamesystem.handleHeartbeat() → DatanodeManager.handleHeartbeat()'),
        ('11.4 块报告','BPServiceActor.blockReport() → NameNodeRpcServer.blockReport() → BlockManager.processReport() → processFirstBlockReport() 或 reportDiff() → addStoredBlock()/removeStoredBlock()'),
        ('11.5 HA Failover','ZKFC.becomeActive() → HAServiceProtocol.transitionToActive() → NameNode.transitionToActive() → HAState.setStateInternal(ACTIVE) → StandbyState.exitState() → ActiveState.enterState()'),
        ('11.6 快照创建','Client.createSnapshot() → NameNodeRpcServer.createSnapshot() → FSNamesystem.createSnapshot() → SnapshotManager.createSnapshot() → INodeDirectory.addSnapshot()'),
        ('11.7 租约恢复','LeaseManager.checkLeases() → 发现硬限制超时 → internalReleaseLease() → FSNamesystem.recoverLeaseInternal() → BlockManager.commitOrCompleteLastBlock()'),
    ]
    for title,chain in chains:
        e.append(Paragraph(title,s_h2))
        e.append(Paragraph(chain.replace('→',' →<br/> '),s_code))

    e.append(PageBreak())

    # 十二、子模块概览
    e.append(Paragraph('十二、子模块概览',s_h1))
    e.append(T(['子目录','文件数','核心职责','关键类'],
        [['ha/','~9','HA状态机、EditLogTailer、StandbyCheckpointer','HAState, ActiveState, StandbyState, EditLogTailer'],
         ['snapshot/','~19','目录快照管理、快照差异计算','SnapshotManager, DirectorySnapshottableFeature, SnapshotDiffReport'],
         ['metrics/','~4','NameNode指标收集和上报','NameNodeMetrics, FSNamesystemMBean'],
         ['startupprogress/','~11','启动进度追踪和报告','StartupProgress, Phase, Step, Status'],
         ['sps/','~12','存储策略满足器（数据迁移）','StoragePolicySatisfier, BlockMoveTaskHandler'],
         ['fgl/','~4','细粒度锁新方案（性能优化）','FineGrainedFSNamesystemLock'],
         ['top/','~7','操作审计和Top操作统计','TopAuditLogger, TopConf'],
         ['visitor/','~4','FSImage访问者模式','ImageVisitor, TextWriterImageVisitor']],
        cw=[25*mm,15*mm,55*mm,75*mm]))
    e.append(PageBreak())

    # 十三、核心源文件清单
    e.append(Paragraph('十三、核心源文件清单',s_h1))
    e.append(T(['源文件','大小','核心职责'],
        [['NameNode.java','110KB','HDFS主入口，HA状态管理，生命周期控制'],
         ['FSNamesystem.java','341KB','命名空间核心状态机（模块最大文件），门面模式'],
         ['FSDirectory.java','75KB','内存INode目录树管理，配额/ACL/XAttr'],
         ['FSEditLog.java','64KB','编辑日志管理（双缓冲区+批量同步+状态机）'],
         ['FSImage.java','60KB','文件系统镜像持久化（Checkpoint）'],
         ['NameNodeRpcServer.java','98KB','RPC三通道服务端，实现NamenodeProtocols'],
         ['FSNamesystemLock.java','~5KB','读写锁封装，慢锁监控和诊断'],
         ['INode.java','39KB','文件/目录抽象基类（快照感知操作）'],
         ['INodeFile.java','42KB','文件INode（64bit header位编码）'],
         ['INodeDirectory.java','35KB','目录INode（children列表，快照特性）'],
         ['INodeReference.java','25KB','快照引用链（WithName/WithCount/DstReference）'],
         ['INodeMap.java','~4KB','LightWeightGSet全局ID→INode映射'],
         ['LeaseManager.java','22KB','文件租约管理（软/硬限制，租约恢复）'],
         ['NNStorage.java','30KB','NameNode物理存储目录管理'],
         ['JournalSet.java','~15KB','多Journal管理器集合'],
         ['HAState.java','5KB','HA状态机基类（状态模式+模板方法）'],
         ['ActiveState.java','2.4KB','Active状态实现'],
         ['StandbyState.java','4KB','Standby/Observer状态实现'],
         ['SecondaryNameNode.java','35KB','辅助NameNode（CheckpointNode）'],
         ['CacheManager.java','30KB','DataNode缓存指令管理'],
         ['EncryptionZoneManager.java','12KB','加密区域管理'],
         ['SnapshotManager.java','31KB','快照全局管理器']],
        cw=[48*mm,15*mm,107*mm]))
    e.append(Paragraph('表13-1：NameNode核心源文件清单（22个关键文件）',s_cap))
    e.append(PageBreak())

    # 十四、设计亮点与总结
    e.append(Paragraph('十四、设计亮点与总结',s_h1))
    e.append(Paragraph('14.1 设计亮点',s_h2))
    for b in [
        '<b>极致内存优化</b>：PermissionStatusFormat 64bit编码权限、INodeFile header 64bit编码属性、SerialNumberManager字符串去重，每个INode节省数十字节',
        '<b>双缓冲区+批量同步</b>：FSEditLog写入和刷盘并行，多线程edit一次flush持久化，I/O吞吐量极高',
        '<b>三通道RPC隔离</b>：client/service/lifeline三个RPC Server互不阻塞，lifeline确保HA健康检查不受业务影响',
        '<b>状态模式HA</b>：Active/Standby/Observer行为封装在独立类中，setStateInternal()模板方法确保转换原子性',
        '<b>写时复制快照</b>：DiffList记录增量变更而非全量复制，内存开销与变更量成正比',
        '<b>FSNamesystemLock诊断</b>：CAS无锁最长锁记录、LogThrottlingHelper日志限流、锁持有者追踪',
        '<b>分层门面架构</b>：FSNamesystem作为统一入口，内部子系统高内聚低耦合',
    ]:
        e.append(Paragraph(f'• {b}',s_bull))
    e.append(Paragraph('14.2 质量评估',s_h2))
    e.append(T(['评估维度','评分','说明'],
        [['架构分层','★★★★★','四层架构清晰，RPC/业务/数据/HA分离彻底'],
         ['设计模式运用','★★★★★','门面/状态/模板/策略/组合/装饰/观察者等模式运用精准'],
         ['内存效率','★★★★★','位编码/序号映射/写时复制等优化极致'],
         ['可扩展性','★★★★☆','块放置策略可插拔，HA状态可扩展，但FSNamesystem过大'],
         ['并发控制','★★★★☆','读写锁+双缓冲区高效，但全局写锁是瓶颈（fgl模块正在优化）'],
         ['代码可维护性','★★★☆☆','FSNamesystem 341KB过大，需进一步拆分']],
        cw=[32*mm,28*mm,110*mm]))
    e.append(Spacer(1,10*mm))
    e.append(Paragraph('— 文档结束 —',ParagraphStyle('End',fontName='CF',fontSize=12,alignment=TA_CENTER,textColor=HexColor('#9e9e9e'))))

    return e

def main():
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)),'Hadoop_NameNode_release-3.3.5-RC0_核心架构设计文档.pdf')
    doc = SimpleDocTemplate(out,pagesize=A4,topMargin=20*mm,bottomMargin=25*mm,leftMargin=20*mm,rightMargin=20*mm)
    doc.build(build(), onFirstPage=on_page, onLaterPages=on_page)
    print(f'\n✅ PDF已生成: {out}')
    print(f'   大小: {os.path.getsize(out)/1024:.1f} KB')

if __name__ == '__main__':
    main()

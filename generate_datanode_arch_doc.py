#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hadoop DataNode 核心架构设计文档生成脚本
基于 release-3.3.5-RC0 版本源码深度分析
"""
import os
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.lib.colors import HexColor, black, white, grey
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
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
BROWN = HexColor('#795548'); TEAL = HexColor('#00695c'); PINK = HexColor('#c62828')

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
    canvas.saveState(); canvas.setFont('CF', 8); canvas.setFillColor(HexColor('#9e9e9e'))
    canvas.drawString(20*mm, A4[1]-12*mm, 'Hadoop DataNode 核心架构设计文档')
    canvas.drawRightString(A4[0]-20*mm, A4[1]-12*mm, f'第 {doc.page} 页')
    canvas.line(20*mm, A4[1]-14*mm, A4[0]-20*mm, A4[1]-14*mm)
    canvas.drawCentredString(A4[0]/2, 12*mm, 'release-3.3.5-RC0 | DataNode Architecture')
    canvas.restoreState()

def draw_layered_arch():
    d = Drawing(480, 440)
    d.add(Rect(0,0,480,440,fillColor=HexColor('#fafafa'),strokeColor=None))
    d.add(String(240,425,'DataNode 六层服务架构',fontName='CFB',fontSize=14,fillColor=DB,textAnchor='middle'))
    # 门面层
    d.add(Rect(15,375,450,38,fillColor=HexColor('#e8f5e9'),strokeColor=AG,strokeWidth=2,rx=8))
    d.add(String(240,398,'门面层 - DataNode (ReconfigurableBase)',fontName='CFB',fontSize=11,fillColor=AG,textAnchor='middle'))
    for t,x in [('DataNode.java',25),('DNConf',118),('DataStorage',206),('Metrics',304),('JMX/Web',387)]:
        box(d,x,378,80 if t!='DataNode.java' else 85,16,t,HexColor('#c8e6c9'),AG,7)
    d.add(Line(240,375,240,365,strokeColor=grey,strokeWidth=1.5))
    # 块池管理层
    d.add(Rect(15,300,450,60,fillColor=LB,strokeColor=MB,strokeWidth=2,rx=8))
    d.add(String(240,345,'块池管理层 - BlockPoolManager / BPOfferService / BPServiceActor',fontName='CFB',fontSize=10,fillColor=DB,textAnchor='middle'))
    for t,x,w in [('BlockPoolManager',25,100),('BPOfferService',135,95),('BPServiceActor',240,90),('IncrementalIBR',340,75),('LifelineSender',423,35)]:
        d.add(Rect(x,308,w,22,fillColor=white,strokeColor=MB,rx=3))
        d.add(String(x+w/2,316,t,fontName='CF',fontSize=7,fillColor=DB,textAnchor='middle'))
    d.add(Line(240,300,240,290,strokeColor=grey,strokeWidth=1.5))
    # 数据传输层
    d.add(Rect(15,225,450,60,fillColor=HexColor('#fff3e0'),strokeColor=AO,strokeWidth=2,rx=8))
    d.add(String(240,270,'数据传输层 - DataXceiverServer / DataXceiver',fontName='CFB',fontSize=10,fillColor=AO,textAnchor='middle'))
    for t,x,w in [('DataXceiverServer',25,105),('DataXceiver',140,80),('BlockReceiver',230,80),('BlockSender',320,75),('PacketResponder',403,55)]:
        d.add(Rect(x,233,w,22,fillColor=white,strokeColor=AO,rx=3))
        d.add(String(x+w/2,241,t,fontName='CF',fontSize=7,fillColor=AO,textAnchor='middle'))
    d.add(Line(240,225,240,215,strokeColor=grey,strokeWidth=1.5))
    # 块存储层
    d.add(Rect(15,145,450,65,fillColor=HexColor('#f3e5f5'),strokeColor=AP,strokeWidth=2,rx=8))
    d.add(String(240,195,'块存储层 - FsDatasetSpi / FsDatasetImpl',fontName='CFB',fontSize=10,fillColor=AP,textAnchor='middle'))
    for t,x,w in [('FsDatasetImpl',25,90),('FsVolumeImpl',125,80),('FsVolumeList',215,80),('ReplicaMap',305,70),('FileIoProvider',383,75)]:
        d.add(Rect(x,165,w,22,fillColor=white,strokeColor=AP,rx=3))
        d.add(String(x+w/2,173,t,fontName='CF',fontSize=7,fillColor=AP,textAnchor='middle'))
    for t,x,w in [('FinalizedReplica',25,85),('ReplicaBeingWritten',118,95),('ReplicaWaitingRecovery',221,100),('ReplicaUnderRecovery',329,95)]:
        d.add(Rect(x,148,w,14,fillColor=white,strokeColor=AP,rx=2))
        d.add(String(x+w/2,152,t,fontName='CF',fontSize=6,fillColor=AP,textAnchor='middle'))
    d.add(Line(240,145,240,135,strokeColor=grey,strokeWidth=1.5))
    # 维护扫描层
    d.add(Rect(15,70,450,60,fillColor=HexColor('#efebe9'),strokeColor=BROWN,strokeWidth=2,rx=8))
    d.add(String(240,115,'维护与扫描层 - Scanner / DiskBalancer / Recovery',fontName='CFB',fontSize=10,fillColor=BROWN,textAnchor='middle'))
    for t,x,w in [('BlockScanner',25,80),('VolumeScanner',113,80),('DirectoryScanner',201,90),('DiskBalancer',299,80),('BlockRecoveryWorker',387,70)]:
        d.add(Rect(x,78,w,22,fillColor=white,strokeColor=BROWN,rx=3))
        d.add(String(x+w/2,86,t,fontName='CF',fontSize=7,fillColor=BROWN,textAnchor='middle'))
    d.add(Line(240,70,240,60,strokeColor=grey,strokeWidth=1.5))
    # 高级特性层
    d.add(Rect(15,5,450,50,fillColor=HexColor('#e0f7fa'),strokeColor=TEAL,strokeWidth=2,rx=8))
    d.add(String(240,42,'高级特性层 - ErasureCoding / ShortCircuit / Checker',fontName='CFB',fontSize=10,fillColor=TEAL,textAnchor='middle'))
    for t,x,w in [('ErasureCodingWorker',25,105),('StripedReconstructor',140,100),('ShortCircuitRegistry',250,105),('StorageLocationChecker',363,95)]:
        d.add(Rect(x,12,w,22,fillColor=white,strokeColor=TEAL,rx=3))
        d.add(String(x+w/2,20,t,fontName='CF',fontSize=7,fillColor=TEAL,textAnchor='middle'))
    return d

def draw_replica_hierarchy():
    d = Drawing(480, 260)
    d.add(Rect(0,0,480,260,fillColor=HexColor('#fafafa'),strokeColor=None))
    d.add(String(240,245,'副本类型继承体系 (Replica State Machine)',fontName='CFB',fontSize=13,fillColor=DB,textAnchor='middle'))
    # Replica接口
    d.add(Rect(185,210,110,25,fillColor=HexColor('#e0f7fa'),strokeColor=TEAL,strokeWidth=1.5,rx=5))
    d.add(String(240,219,'<<interface>> Replica',fontName='CFB',fontSize=8,fillColor=TEAL,textAnchor='middle'))
    # ReplicaInfo
    d.add(Rect(160,170,160,28,fillColor=HexColor('#e8eaf6'),strokeColor=MB,strokeWidth=1.5,rx=5))
    d.add(String(240,188,'ReplicaInfo (extends Block)',fontName='CFB',fontSize=9,fillColor=DB,textAnchor='middle'))
    d.add(String(240,175,'volume / baseDir / state',fontName='CF',fontSize=7,fillColor=grey,textAnchor='middle'))
    d.add(Line(240,210,240,198,strokeColor=grey,strokeWidth=1,strokeDashArray=[3,2]))
    # LocalReplica
    d.add(Rect(170,130,140,28,fillColor=HexColor('#f3e5f5'),strokeColor=AP,strokeWidth=1.5,rx=5))
    d.add(String(240,148,'LocalReplica (abstract)',fontName='CFB',fontSize=9,fillColor=AP,textAnchor='middle'))
    d.add(String(240,135,'getBlockFile / getMetaFile',fontName='CF',fontSize=7,fillColor=grey,textAnchor='middle'))
    d.add(Line(240,170,240,158,strokeColor=grey,strokeWidth=1))
    # 四种副本
    for label,x,w,sc,fc,state,desc in [
        ('FinalizedReplica',15,100,AG,HexColor('#e8f5e9'),'FINALIZED','写入完成,不可变'),
        ('ReplicaBeingWritten',125,110,AO,HexColor('#fff3e0'),'RBW','正在写入中'),
        ('ReplicaWaitingRecovery',245,120,PINK,HexColor('#fce4ec'),'RWR','等待恢复'),
        ('ReplicaUnderRecovery',375,95,AP,HexColor('#f3e5f5'),'RUR','恢复进行中')]:
        d.add(Rect(x,65,w,45,fillColor=fc,strokeColor=sc,strokeWidth=1.5,rx=5))
        d.add(String(x+w/2,98,label,fontName='CFB',fontSize=7,fillColor=sc,textAnchor='middle'))
        d.add(String(x+w/2,86,f'State: {state}',fontName='CF',fontSize=6.5,fillColor=grey,textAnchor='middle'))
        d.add(String(x+w/2,76,desc,fontName='CF',fontSize=6.5,fillColor=grey,textAnchor='middle'))
        d.add(Line(240,130,x+w/2,110,strokeColor=grey,strokeWidth=0.8))
    # 状态转换
    d.add(Rect(15,5,450,48,fillColor=HexColor('#fffde7'),strokeColor=HexColor('#f9a825'),strokeWidth=1,rx=5))
    d.add(String(240,40,'副本状态转换流程',fontName='CFB',fontSize=9,fillColor=HexColor('#f57f17'),textAnchor='middle'))
    d.add(String(240,25,'写入: TEMPORARY -> RBW -> FINALIZED    恢复: RBW -> RWR -> RUR -> FINALIZED',fontName='CF',fontSize=8,fillColor=DB,textAnchor='middle'))
    d.add(String(240,12,'追加: FINALIZED -> RBW -> FINALIZED    快照: FINALIZED -> RUR -> FINALIZED',fontName='CF',fontSize=8,fillColor=PINK,textAnchor='middle'))
    return d

def draw_pipeline_write():
    d = Drawing(480, 185)
    d.add(Rect(0,0,480,185,fillColor=HexColor('#fafafa'),strokeColor=None))
    d.add(String(240,172,'数据写入 Pipeline (三副本)',fontName='CFB',fontSize=13,fillColor=DB,textAnchor='middle'))
    # Client
    d.add(Rect(10,110,70,40,fillColor=HexColor('#e0f7fa'),strokeColor=TEAL,strokeWidth=1.5,rx=6))
    d.add(String(45,135,'DFS Client',fontName='CFB',fontSize=8,fillColor=TEAL,textAnchor='middle'))
    d.add(String(45,118,'DFSOutputStream',fontName='CF',fontSize=6,fillColor=grey,textAnchor='middle'))
    # DNs
    for x,dn,pr in [(110,'DataNode-1','PacketResponder'),(235,'DataNode-2','PacketResponder'),(360,'DataNode-3','(末端)')]:
        d.add(Rect(x,110,110,40,fillColor=HexColor('#fff3e0'),strokeColor=AO,strokeWidth=1.5,rx=6))
        d.add(String(x+55,138,dn,fontName='CFB',fontSize=8,fillColor=AO,textAnchor='middle'))
        d.add(String(x+55,125,f'BlockReceiver',fontName='CF',fontSize=6,fillColor=grey,textAnchor='middle'))
        d.add(String(x+55,115,pr,fontName='CF',fontSize=6,fillColor=grey,textAnchor='middle'))
    for x1,x2 in [(80,110),(220,235),(345,360)]:
        d.add(Line(x1,130,x2,130,strokeColor=AG,strokeWidth=2))
        d.add(String((x1+x2)/2,135,'Packet',fontName='CF',fontSize=6,fillColor=AG,textAnchor='middle'))
    for x1,x2 in [(360,220),(235,110),(110,80)]:
        d.add(Line(x1,95,x2,95,strokeColor=MB,strokeWidth=1.5,strokeDashArray=[4,2]))
        d.add(String((x1+x2)/2,85,'ACK',fontName='CF',fontSize=6,fillColor=MB,textAnchor='middle'))
    for x in [145,270,395]:
        d.add(Line(x+20,110,x+20,65,strokeColor=grey,strokeWidth=0.8))
        d.add(Rect(x,40,40,25,fillColor=HexColor('#efebe9'),strokeColor=BROWN,rx=3))
        d.add(String(x+20,49,'Disk',fontName='CF',fontSize=7,fillColor=BROWN,textAnchor='middle'))
    d.add(Rect(10,5,460,25,fillColor=HexColor('#fffde7'),strokeColor=HexColor('#f9a825'),rx=4))
    d.add(String(240,18,'Pipeline: Client->DN1->DN2->DN3 (串行转发)  ACK: DN3->DN2->DN1->Client (逆向传播)',fontName='CF',fontSize=7.5,fillColor=HexColor('#f57f17'),textAnchor='middle'))
    return d

def build():
    e = []
    # 封面
    e.append(Spacer(1,50*mm)); e.append(Paragraph('Hadoop DataNode',s_title)); e.append(Paragraph('核心架构设计文档',s_title))
    e.append(Spacer(1,8*mm)); e.append(Paragraph('基于 release-3.3.5-RC0 版本源码深度分析',s_sub)); e.append(Spacer(1,12*mm))
    info = [['项目','Apache Hadoop HDFS'],['模块','hadoop-hdfs / server / datanode'],['版本','release-3.3.5-RC0'],
            ['范围','DataNode主目录+6个子目录(checker/erasurecode/fsdataset/impl/metrics/web)'],['文件数','131 个 Java 源文件'],['工具','自动化源码架构分析']]
    it = Table([[Paragraph(r[0],s_th),Paragraph(r[1],s_tc)] for r in info],colWidths=[50*mm,120*mm])
    it.setStyle(TableStyle([('BACKGROUND',(0,0),(0,-1),TH_BG),('TEXTCOLOR',(0,0),(0,-1),white),('BACKGROUND',(1,0),(1,-1),white),
                            ('GRID',(0,0),(-1,-1),0.5,BD),('VALIGN',(0,0),(-1,-1),'MIDDLE'),('TOPPADDING',(0,0),(-1,-1),6),('BOTTOMPADDING',(0,0),(-1,-1),6)]))
    e.append(it); e.append(PageBreak())
    # 目录
    e.append(Paragraph('目录',s_h1))
    for t in ['一、模块概述','二、核心六层服务架构','三、DataNode启动流程与生命周期','四、块池管理层—BlockPoolManager/BPOfferService/BPServiceActor',
              '五、数据传输层—DataXceiverServer/BlockReceiver/BlockSender','六、块存储层—FsDatasetSpi/FsDatasetImpl/副本类型体系',
              '七、维护与扫描层—BlockScanner/DirectoryScanner/DiskBalancer','八、块恢复机制—BlockRecoveryWorker',
              '九、高级特性—纠删码/短路读/FileIoProvider','十、核心流程时序分析','十一、设计模式总结',
              '十二、关键场景调用链','十三、子目录概览与核心源文件清单','十四、设计亮点与总结']:
        e.append(Paragraph(t,s_toc))
    e.append(PageBreak())

    # 一、模块概述
    e.append(Paragraph('一、模块概述',s_h1))
    e.append(Paragraph('DataNode 是 HDFS 分布式文件系统的<b>数据存储节点</b>，负责在本地磁盘上存储数据块（Block），'
        '响应客户端的读写请求，执行 NameNode 下发的块复制、删除、恢复等命令。模块位于 hadoop-hdfs/server/datanode 目录，'
        '包含 <b>131 个 Java 源文件</b>，覆盖根目录54个文件及 checker/、erasurecode/、fsdataset/、fsdataset/impl/、metrics/、web/ 等6个子目录。'
        'DataNode 采用<b>门面模式</b>作为统一入口，内部管理块池注册、数据传输、存储卷、块扫描、磁盘均衡、纠删码重构等子服务的完整生命周期。',s_body))
    e.append(Paragraph('1.1 核心职责',s_h2))
    for b in ['<b>数据块存储</b>：在多个存储卷（DISK/SSD/ARCHIVE/RAM_DISK）上管理数据块文件和校验文件',
              '<b>客户端数据读写</b>：响应readBlock/writeBlock请求，支持pipeline写入和零拷贝读取',
              '<b>块池注册与心跳</b>：向所有NameNode注册并定期发送心跳、块报告、增量块报告',
              '<b>NameNode命令执行</b>：执行复制(DNA_TRANSFER)、删除(DNA_INVALIDATE)、恢复(DNA_RECOVERBLOCK)等命令',
              '<b>块完整性校验</b>：BlockScanner/VolumeScanner定期扫描数据块校验和，发现坏块上报NameNode',
              '<b>目录一致性扫描</b>：DirectoryScanner定期比对磁盘文件与内存元数据的一致性',
              '<b>磁盘均衡</b>：DiskBalancer在DataNode本地不同磁盘间移动块以平衡使用率',
              '<b>纠删码支持</b>：ErasureCodingWorker/StripedReconstructor重构丢失的条带化数据块',
              '<b>短路读管理</b>：ShortCircuitRegistry管理客户端短路读使用的共享内存段']:
        e.append(Paragraph(f'• {b}',s_bull))
    e.append(Paragraph('1.2 模块目录结构',s_h2))
    e.append(T(['目录','文件数','核心职责'],
        [['datanode/ (根目录)','54','DataNode主类、块池管理、数据传输、块恢复、存储位置等'],
         ['checker/','3','存储位置检查器(StorageLocationChecker)'],['erasurecode/','10','纠删码工作器和条带化重构器'],
         ['fsdataset/','28','块存储SPI接口和数据集抽象'],['fsdataset/impl/','25','FsDatasetImpl默认实现、卷管理、副本映射'],
         ['metrics/','4','DataNode JMX指标收集'],['web/','7','DataNode Web UI资源和Servlet']],cw=[40*mm,18*mm,112*mm]))
    e.append(PageBreak())

    # 二、核心六层服务架构
    e.append(Paragraph('二、核心六层服务架构',s_h1))
    e.append(Paragraph('DataNode模块采用清晰的<b>六层服务架构</b>：门面层→块池管理层→数据传输层→块存储层→维护扫描层→高级特性层。每层职责分明，通过接口解耦。',s_body))
    e.append(Paragraph('2.1 六层架构全景图',s_h2))
    e.append(draw_layered_arch()); e.append(Paragraph('图2-1：DataNode六层服务架构全景图',s_cap))
    e.append(Paragraph('2.2 各层职责概述',s_h2))
    e.append(T(['层级','核心类','职责','设计模式'],
        [['门面层','DataNode, DNConf, DataStorage','统一入口,管理所有子服务生命周期','门面模式'],
         ['块池管理层','BlockPoolManager, BPOfferService, BPServiceActor','NN注册/心跳/块报告/HA切换','读写锁, 命令模式'],
         ['数据传输层','DataXceiverServer, DataXceiver, BlockReceiver/Sender','TCP数据传输服务,读写块','Reactor模式, 职责链'],
         ['块存储层','FsDatasetImpl, FsVolumeImpl, ReplicaMap','本地磁盘块管理,副本映射','SPI/策略/建造者模式'],
         ['维护扫描层','BlockScanner, DirectoryScanner, DiskBalancer','块校验/目录一致性/磁盘均衡','观察者模式'],
         ['高级特性层','ErasureCodingWorker, ShortCircuitRegistry','纠删码重构/短路读/IO代理','代理/装饰器模式']],cw=[25*mm,50*mm,48*mm,47*mm]))
    e.append(PageBreak())

    # 三、DataNode启动流程
    e.append(Paragraph('三、DataNode启动流程与生命周期',s_h1))
    e.append(Paragraph('入口：DataNode.main() → secureMain() → createDataNode() → instantiateDataNode() → new DataNode(conf) → startDataNode()。',s_body))
    e.append(Paragraph('3.1 startDataNode() 核心初始化序列',s_h2))
    e.append(T(['步骤','操作','说明'],
        [['1','initDataXceiver()','创建DataXceiverServer(TCP监听)，绑定流式数据传输端口'],
         ['2','startInfoServer()','启动HTTP Web UI和REST API服务'],
         ['3','initIpcServer()','创建IPC RPC Server，注册ClientDatanodeProtocol/InterDatanodeProtocol'],
         ['4','初始化BlockScanner','创建块扫描管理器，管理所有VolumeScanner'],
         ['5','初始化BlockPoolManager','创建BPOfferService(每个命名空间一个)，启动BPServiceActor线程'],
         ['6','注册MXBean','注册DataNodeMXBean用于JMX监控']],cw=[12*mm,52*mm,106*mm]))
    e.append(Paragraph('3.2 BPServiceActor完整生命周期',s_h2))
    e.append(T(['阶段','方法','说明'],
        [['握手','connectToNNAndHandshake()','与NameNode建立连接，获取NamespaceInfo和blockPoolId'],
         ['注册','register()','向NameNode注册DataNode，发送DatanodeRegistration'],
         ['服务','offerService()','主循环：心跳→处理命令→IBR→全量块报告→缓存报告→等待'],
         ['异常','handleRollingUpgradeStatus()','处理滚动升级状态同步'],
         ['关闭','stop()','停止LifelineSender和CommandProcessingThread']],cw=[14*mm,58*mm,98*mm]))
    e.append(Paragraph('3.3 DataNode关键接口实现',s_h2))
    e.append(T(['接口','用途'],
        [['ClientDatanodeProtocol','客户端到DataNode的RPC协议(刷新、块恢复触发等)'],
         ['InterDatanodeProtocol','DataNode之间的RPC协议(块恢复协调、副本信息交换)'],
         ['DataNodeMXBean','JMX监控接口(集群ID、数据目录、磁盘使用率等)'],
         ['ReconfigurableBase','支持运行时动态重配置(dfs.datanode.data.dir热添加删除存储卷)']],cw=[52*mm,118*mm]))
    e.append(PageBreak())

    # 四、块池管理层
    e.append(Paragraph('四、块池管理层',s_h1))
    e.append(Paragraph('块池管理层是DataNode与NameNode通信的核心。HDFS联邦下一个DataNode可服务多个命名空间，每个命名空间对应一个<b>BPOfferService</b>；'
        'HA下每个NameNode对应一个<b>BPServiceActor</b>线程。BlockPoolManager统一管理所有BPOfferService。',s_body))
    e.append(Paragraph('4.1 BPOfferService核心设计',s_h2))
    for b in ['<b>ReentrantReadWriteLock</b>：保护bpRegistration、bpNSInfo、activeNN等共享状态，读多写少场景最优',
              '<b>HA主备切换</b>：updateActorStatesFromHeartbeat()根据心跳返回的NNHAStatusHeartbeat更新Active/Standby角色',
              '<b>命名空间隔离</b>：每个BPOfferService管理独立的blockPoolId和注册状态，互不干扰',
              '<b>BPServiceActorAction</b>：命令模式接口，BPServiceActor通过bpServiceActorActions队列异步执行动作']:
        e.append(Paragraph(f'• {b}',s_bull))
    e.append(Paragraph('4.2 BPServiceActor内部组件',s_h2))
    e.append(T(['组件','职责','说明'],
        [['Scheduler','调度心跳/块报告/缓存报告','基于单调时间的调度器，支持抖动(jitter)防止雪崩'],
         ['LifelineSender','HA生命线心跳','独立线程发送轻量级lifeline心跳，避免主心跳阻塞导致假死'],
         ['CommandProcessingThread','异步命令处理','独立线程处理NN下发的DNA_*命令，避免阻塞主循环'],
         ['IncrementalBlockReportManager','增量块报告管理','缓存块变更(add/delete/finalize)，按调度批量上报'],
         ['RunningState','状态枚举','CONNECTING→INIT_FAILED→RUNNING→EXITED四种状态']],cw=[48*mm,38*mm,84*mm]))
    e.append(Paragraph('4.3 NameNode下发命令类型',s_h2))
    e.append(T(['命令','说明'],
        [['DNA_TRANSFER','复制块到其他DataNode(副本补充)'],['DNA_INVALIDATE','删除无效块(副本过多或过期)'],
         ['DNA_RECOVERBLOCK','块恢复(Pipeline断裂后)'],['DNA_SHUTDOWN','关闭DataNode'],
         ['DNA_REGISTER','要求DataNode重新注册'],['DNA_FINALIZE','完成升级'],
         ['DNA_BALANCERBANDWIDTHUPDATE','更新均衡带宽限制'],['DNA_CACHE / DNA_UNCACHE','缓存/取消缓存块'],
         ['DNA_ERASURE_CODING_RECOVERY','纠删码块重构']],cw=[58*mm,112*mm]))
    e.append(PageBreak())

    # 五、数据传输层
    e.append(Paragraph('五、数据传输层',s_h1))
    e.append(Paragraph('数据传输层负责处理HDFS客户端和其他DataNode的数据流请求。采用<b>Reactor模式</b>：DataXceiverServer负责accept，每个连接分派给DataXceiver线程处理。',s_body))
    e.append(Paragraph('5.1 DataXceiverServer — Reactor接受器',s_h2))
    e.append(T(['特性','实现','说明'],
        [['监听端口','dfs.datanode.address(默认9866)','TCP流式数据传输端口'],
         ['线程模型','每连接一线程(DataXceiver)','accept后创建DataXceiver,支持keepalive复用'],
         ['均衡限流','BlockBalanceThrottler(Semaphore)','控制均衡操作最大并发数,读/写/传输三维度限流'],
         ['连接管理','PeerServer + peers HashMap','跟踪所有活跃连接,支持释放过期连接']],cw=[28*mm,55*mm,87*mm]))
    e.append(Paragraph('5.2 DataXceiver — 操作分派器',s_h2))
    e.append(T(['操作','方法','说明'],
        [['读块','readBlock()','从本地磁盘读取并发送,委托BlockSender'],['写块','writeBlock()','接收上游数据写入本地,可能转发下游,委托BlockReceiver'],
         ['复制块','copyBlock()','本地块复制到请求端,用于均衡/副本迁移'],['替换块','replaceBlock()','接收远端块写入本地替换'],
         ['短路读','requestShortCircuitFds()','传递文件描述符给客户端实现短路读'],['传输块','transferBlock()','主动发起到其他DN的块传输']],cw=[20*mm,48*mm,102*mm]))
    e.append(Paragraph('5.3 Pipeline写入流程',s_h2))
    e.append(draw_pipeline_write()); e.append(Paragraph('图5-1：数据写入Pipeline(三副本)',s_cap))
    e.append(Paragraph('BlockReceiver是写入路径的核心。接收上游Packet，写入本地磁盘并转发下游。内含<b>PacketResponder</b>线程负责收集下游ACK并传回上游。',s_body))
    e.append(T(['核心机制','说明'],
        [['Packet粒度写入','每个Packet包含header+checksum+data,默认64KB'],
         ['先写磁盘再转发','receivePacket():接收→写本地→转发下游→等待ACK'],
         ['PacketResponder线程','独立线程等待下游ACK,合并本地ACK后传回上游'],
         ['同步策略','syncOnClose时fsync刷盘; syncBehindWrites可选后台同步'],
         ['校验和验证','每个chunk(默认512字节)验证CRC32/CRC32C校验和']],cw=[38*mm,132*mm]))
    e.append(Paragraph('5.4 BlockSender — 零拷贝读取',s_h2))
    e.append(T(['特性','说明'],
        [['transferTo零拷贝','利用sendfile系统调用,数据直接从内核页缓存到Socket缓冲区'],
         ['readahead预读','NativeIO.posixFadvise(WILLNEED)预读后续数据到内核页缓存'],
         ['dropCacheBehind','NativeIO.posixFadvise(DONTNEED)读取后丢弃缓存,避免缓存污染'],
         ['校验和发送','每个chunk的校验和紧跟数据发送,客户端验证完整性']],cw=[38*mm,132*mm]))
    e.append(PageBreak())

    # 六、块存储层
    e.append(Paragraph('六、块存储层 — 副本类型体系',s_h1))
    e.append(Paragraph('块存储层通过<b>SPI(Service Provider Interface)</b>设计实现可插拔的块存储管理。FsDatasetSpi和FsVolumeSpi定义契约，FsDatasetImpl和FsVolumeImpl提供本地文件系统实现。',s_body))
    e.append(Paragraph('6.1 副本类型继承体系',s_h2))
    e.append(draw_replica_hierarchy()); e.append(Paragraph('图6-1：副本类型继承体系及状态转换',s_cap))
    e.append(Paragraph('6.2 FsDatasetImpl核心组件',s_h2))
    e.append(T(['组件','类型','说明'],
        [['volumes(FsVolumeList)','ArrayList<FsVolumeImpl>','管理所有存储卷,支持热添加/移除'],
         ['volumeMap(ReplicaMap)','Map<String,Map<Long,ReplicaInfo>>','blockPoolId→(blockId→ReplicaInfo)二级映射'],
         ['datasetRWLock','AutoCloseableLock','全局读写锁保护volumeMap和副本状态'],
         ['asyncDiskService','FsDatasetAsyncDiskService','异步执行磁盘删除,避免阻塞主线程']],cw=[42*mm,50*mm,78*mm]))
    e.append(Paragraph('6.3 FsVolumeImpl — 存储卷管理',s_h2))
    e.append(T(['特性','实现'],
        [['存储介质类型','StorageType: DISK/SSD/ARCHIVE/RAM_DISK/NVDIMM'],
         ['目录结构','<volume>/current/BP-xxx/current/finalized/subdir0~N/, /rbw/, /tmp/'],
         ['VolumeChoosingPolicy','策略模式: RoundRobin(默认)/AvailableSpace可切换'],
         ['blockPoolSlice','每个blockPool对应一个BlockPoolSlice管理独立目录']],cw=[38*mm,132*mm]))
    e.append(Paragraph('6.4 ReplicaBuilder — 建造者模式',s_h2))
    e.append(Paragraph('ReplicaBuilder根据ReplicaState动态创建对应ReplicaInfo实例：FINALIZED→FinalizedReplica, RBW→ReplicaBeingWritten, '
        'RWR→ReplicaWaitingToBeRecovered, RUR→ReplicaUnderRecovery, TEMPORARY→LocalReplicaInPipeline。支持链式调用设置blockId, genStamp, volume等属性。',s_body))
    e.append(PageBreak())

    # 七、维护与扫描层
    e.append(Paragraph('七、维护与扫描层',s_h1))
    e.append(Paragraph('7.1 BlockScanner/VolumeScanner — 块完整性校验',s_h2))
    e.append(T(['特性','实现'],
        [['扫描周期','dfs.block.scanner.volume.bytes.per.second(默认1MB/s)'],
         ['优先扫描','可疑块优先: suspect块列表优先于常规扫描'],
         ['扫描结果','ScanResultHandler: BAD_METADATA/BAD_CHECKSUM/NONE→上报NameNode'],
         ['速率控制','循环缓冲区控制扫描速率,避免过度占用磁盘IO'],
         ['管理结构','BlockScanner使用TreeMap<String,VolumeScanner>按卷ID管理']],cw=[28*mm,142*mm]))
    e.append(Paragraph('7.2 DirectoryScanner — 磁盘与内存一致性',s_h2))
    e.append(T(['差异类型','含义','处理方式'],
        [['磁盘有/内存无','块文件存在但ReplicaMap没有','添加到内存(addStoredBlock)'],
         ['磁盘无/内存有','ReplicaMap有记录但文件缺失','从内存移除(removeStoredBlock)'],
         ['元文件不匹配','元文件路径/校验和不匹配','更新元数据信息'],
         ['GenStamp不一致','磁盘文件的GenerationStamp不同','以磁盘为准更新'],
         ['块长度不一致','磁盘文件大小与numBytes不同','以磁盘为准更新']],cw=[32*mm,55*mm,83*mm]))
    e.append(Paragraph('<b>设计要点</b>：分批reconcile避免长时间占锁；使用ForkJoinPool并行扫描多卷提升效率。',s_body))
    e.append(Paragraph('7.3 DiskBalancer — 本地磁盘均衡',s_h2))
    e.append(Paragraph('DiskBalancer在DataNode本地不同磁盘间移动数据块，平衡各磁盘使用率。区别于HDFS Balancer(节点间均衡)，DiskBalancer专注单节点内磁盘均衡。'
        '核心组件：DiskBalancerPlan(JSON计划)、VolumePair(源卷+目标卷)、BlockMover(执行块移动)。',s_body))
    e.append(PageBreak())

    # 八、块恢复机制
    e.append(Paragraph('八、块恢复机制 — BlockRecoveryWorker',s_h1))
    e.append(Paragraph('当NameNode检测到Pipeline异常中断时，选择一个DataNode作为<b>恢复协调者</b>，下发DNA_RECOVERBLOCK命令。BlockRecoveryWorker协调多DN将块恢复到一致状态。',s_body))
    e.append(Paragraph('8.1 块恢复流程',s_h2))
    e.append(T(['步骤','操作','说明'],
        [['1','接收恢复命令','NameNode下发RecoverBlock命令,含block+参与DN列表'],
         ['2','收集副本信息','协调者通过InterDatanodeProtocol获取每个DN的副本状态'],
         ['3','计算恢复目标','选择最小GenerationStamp和blockLength作为恢复目标'],
         ['4','同步所有副本','调用updateReplicaUnderRecovery()将所有DN副本截断/同步到目标'],
         ['5','上报恢复结果','通过commitBlockSynchronization()向NameNode报告完成']],cw=[12*mm,35*mm,123*mm]))
    e.append(Paragraph('8.2 连续块 vs 条带化块恢复',s_h2))
    e.append(T(['类型','实现类','特点'],
        [['连续块恢复','RecoveryTaskContiguous','标准三副本恢复,所有副本截断到相同长度和GS'],
         ['条带化块恢复','RecoveryTaskStriped','纠删码条带化块恢复,需考虑条带对齐和校验块']],cw=[28*mm,55*mm,87*mm]))
    e.append(PageBreak())

    # 九、高级特性
    e.append(Paragraph('九、高级特性',s_h1))
    e.append(Paragraph('9.1 纠删码 — ErasureCodingWorker',s_h2))
    e.append(Paragraph('ErasureCodingWorker处理NameNode下发的纠删码重构任务。当数据/校验块丢失时，利用Reed-Solomon算法从剩余块重构丢失数据。',s_body))
    e.append(T(['组件','说明'],
        [['ErasureCodingWorker','纠删码工作器入口,管理重构任务队列和线程池'],
         ['StripedReconstructor','抽象基类,定义条带化重构通用流程'],
         ['StripedBlockReader/Writer','从远端DN读取/写入条带化数据块'],
         ['StripedBlockChecksumReconstructor','重构丢失块的校验和'],
         ['RawErasureEncoder/Decoder','JNI调用ISA-L或Java实现的编解码器']],cw=[55*mm,115*mm]))
    e.append(Paragraph('9.2 短路读 — ShortCircuitRegistry',s_h2))
    e.append(Paragraph('短路读允许同节点HDFS客户端绕过DataNode直接读取本地文件。ShortCircuitRegistry管理客户端和DataNode间的<b>共享内存段</b>和<b>槽位(Slot)</b>。'
        '通过Unix域套接字(DomainSocket)传递文件描述符(FD)给客户端。',s_body))
    e.append(Paragraph('9.3 FileIoProvider — IO代理/装饰器模式',s_h2))
    e.append(Paragraph('FileIoProvider是所有文件IO操作的<b>代理/装饰器</b>。所有FsDatasetImpl/FsVolumeImpl的IO操作都通过FileIoProvider间接执行，'
        '在实际IO前后插入两类钩子：<b>ProfilingFileIoEvents</b>(性能统计)和<b>FaultInjectorFileIoEvents</b>(故障注入)。',s_body))
    e.append(PageBreak())

    # 十、核心流程时序
    e.append(Paragraph('十、核心流程时序分析',s_h1))
    e.append(Paragraph('10.1 客户端写块完整流程',s_h2))
    e.append(Paragraph('<b>阶段一 Pipeline建立</b>：Client→NN.addBlock()获取[DN1,DN2,DN3]→Client连接DN1发送OpWriteBlock(PIPELINE_SETUP_CREATE)→DN1连接DN2转发→DN2连接DN3转发→DN3返回SUCCESS逆向传播。<br/>'
        '<b>阶段二 数据传输</b>：Client逐Packet发送→DN1(BlockReceiver)写本地+转发DN2→DN2写本地+转发DN3→DN3写本地+发ACK回DN2→DN2合并ACK回DN1→DN1回Client。<br/>'
        '<b>阶段三 完成</b>：Client发送lastPacketInBlock→各DN finalizeBlock(RBW→FINALIZED)→各DN通过IBR上报FINALIZED给NameNode。',s_body))
    e.append(Paragraph('10.2 客户端读块流程',s_h2))
    e.append(Paragraph('Client→NN.getBlockLocations()→选择最近DN发送OpReadBlock→DN(DataXceiver.readBlock)创建BlockSender→'
        'transferTo零拷贝发送→Client验证CRC→如果校验失败尝试下一个DN并报告坏块。',s_body))
    e.append(Paragraph('10.3 块恢复协调流程',s_h2))
    e.append(Paragraph('NN检测Pipeline断裂→选择协调者DN下发DNA_RECOVERBLOCK→协调者(BlockRecoveryWorker)联系所有参与DN→'
        '各DN返回ReplicaRecoveryInfo(state,genStamp,numBytes)→协调者计算minLength→各DN.updateReplicaUnderRecovery()截断→'
        '协调者NN.commitBlockSynchronization()提交。',s_body))
    e.append(Paragraph('10.4 增量块报告(IBR)流程',s_h2))
    e.append(Paragraph('FsDatasetImpl中块状态变更→调用notifyNamenodeReceived/Deleted/Finalized→IncrementalBlockReportManager缓存到PerStorageIBR→'
        'BPServiceActor主循环调用reportReceivedDeletedBlocks()→发送到NameNode(BlockManager.processIncrementalBlockReport)→NN更新块→DN映射。',s_body))
    e.append(PageBreak())

    # 十一、设计模式总结
    e.append(Paragraph('十一、设计模式总结',s_h1))
    e.append(T(['设计模式','应用场景','关键类','设计意图'],
        [['门面模式','DataNode主入口','DataNode','统一入口管理所有子服务,隐藏内部复杂性'],
         ['Reactor模式','数据传输服务','DataXceiverServer+DataXceiver','accept+dispatch,每连接一线程处理读写'],
         ['职责链/Pipeline','写入数据流','BlockReceiver+PacketResponder','DN串行转发,ACK逆向传播'],
         ['SPI接口模式','块存储抽象','FsDatasetSpi/FsVolumeSpi','存储实现可插拔,通过配置选择'],
         ['策略模式','卷选择策略','VolumeChoosingPolicy','RoundRobin/AvailableSpace可切换'],
         ['建造者模式','副本创建','ReplicaBuilder','根据State动态创建不同ReplicaInfo'],
         ['命令模式','NN命令执行','BPServiceActorAction/ErrorReportAction','封装DNA_*为Action,异步队列执行'],
         ['代理/装饰器','文件IO代理','FileIoProvider','透明插入性能统计和故障注入'],
         ['读写锁','共享状态保护','BPOfferService/FsDatasetImpl','读多写少的并发控制'],
         ['信号量','均衡限流','BlockBalanceThrottler(Semaphore)','控制均衡最大并发,保护磁盘IO'],
         ['观察者模式','块变更通知','FsDatasetImpl→BPOfferService→IBR','块变更自动触发增量报告'],
         ['模板方法','块恢复流程','RecoveryTaskContiguous/Striped','定义恢复骨架,子类实现差异']],cw=[26*mm,25*mm,53*mm,66*mm]))
    e.append(Paragraph('表11-1：DataNode核心设计模式一览(12种)',s_cap))
    e.append(PageBreak())

    # 十二、关键场景调用链
    e.append(Paragraph('十二、关键场景调用链',s_h1))
    chains = [
        ('12.1 DataNode启动','DataNode.main() -> secureMain() -> createDataNode() -> instantiateDataNode() -><br/>'
         'new DataNode(conf) -> startDataNode() -> initDataXceiver() -> startInfoServer() -><br/>'
         'initIpcServer() -> BlockPoolManager.refreshNamenodes() -> BPOfferService.start() -><br/>'
         'BPServiceActor.run() -> connectToNNAndHandshake() -> register() -> offerService()'),
        ('12.2 客户端写块','Client -> DataXceiverServer.accept() -> new DataXceiver(peer).start() -><br/>'
         'DataXceiver.writeBlock() -> FsDatasetImpl.createRbw() -> new BlockReceiver() -><br/>'
         'BlockReceiver.receiveBlock() -> receivePacket() -> 写磁盘 + 转发下游 -><br/>'
         'PacketResponder.run() -> 收集ACK + 传回上游'),
        ('12.3 客户端读块','Client -> DataXceiverServer.accept() -> new DataXceiver(peer).start() -><br/>'
         'DataXceiver.readBlock() -> FsDatasetImpl.getBlockInputStream() -><br/>'
         'new BlockSender() -> BlockSender.sendBlock() -> transferToFully() (零拷贝)'),
        ('12.4 心跳与块报告','BPServiceActor.offerService() -> scheduler.isHeartbeatDue() -><br/>'
         'sendHeartBeat() -> NN处理返回命令列表 -> processCommand() -><br/>'
         'CommandProcessingThread -> reportReceivedDeletedBlocks() -> blockReport()(每6h)'),
        ('12.5 块恢复','NN下发DNA_RECOVERBLOCK -> BPServiceActor.processCommand() -><br/>'
         'DataNode.recoverBlocks() -> BlockRecoveryWorker.recoverBlocks() -><br/>'
         'RecoveryTaskContiguous.recover() -> initReplicaRecovery() -><br/>'
         'updateReplicaUnderRecovery() -> commitBlockSynchronization()'),
        ('12.6 磁盘均衡','hdfs diskbalancer -> DataNode.submitDiskBalancerPlan() -><br/>'
         'DiskBalancer.submitPlan() -> 解析JSON -> VolumePair列表 -><br/>'
         'BlockMover.run() -> FsDatasetImpl.moveBlockAcrossVolumes()'),
        ('12.7 纠删码重构','NN下发DNA_ERASURE_CODING_RECOVERY -><br/>'
         'ErasureCodingWorker.processErasureCodingTasks() -><br/>'
         'StripedReconstructor.reconstruct() -> StripedBlockReader.read() -><br/>'
         'RawErasureDecoder.decode() -> StripedBlockWriter.write()'),
    ]
    for title,chain in chains:
        e.append(Paragraph(title,s_h2)); e.append(Paragraph(chain,s_code))
    e.append(PageBreak())

    # 十三、子目录与源文件
    e.append(Paragraph('十三、子目录概览与核心源文件清单',s_h1))
    e.append(Paragraph('13.1 子目录功能概览',s_h2))
    e.append(T(['子目录','文件数','核心职责','关键类'],
        [['checker/','3','存储位置校验','StorageLocationChecker, ThrottledAsyncChecker'],
         ['erasurecode/','10','纠删码重构','ErasureCodingWorker, StripedReconstructor'],
         ['fsdataset/','28','块存储SPI接口','FsDatasetSpi, FsVolumeSpi, Replica, ReplicaInfo'],
         ['fsdataset/impl/','25','存储默认实现','FsDatasetImpl, FsVolumeImpl, ReplicaMap'],
         ['metrics/','4','JMX指标','DataNodeMetrics, DataNodePeerMetrics'],
         ['web/','7','Web UI','DatanodeHttpServer, DatanodeWebHdfsMethods']],cw=[28*mm,14*mm,42*mm,86*mm]))
    e.append(Paragraph('13.2 根目录核心源文件清单(Top 25)',s_h2))
    e.append(T(['源文件','核心职责','关键特点'],
        [['DataNode.java','DataNode主类(门面)','继承ReconfigurableBase,管理所有子服务'],
         ['BPOfferService.java','块池/命名空间服务','RWLock保护,管理BPServiceActor,HA切换'],
         ['BPServiceActor.java','NN通信线程','握手→注册→心跳→块报告完整生命周期'],
         ['BlockReceiver.java','Pipeline写入核心','接收Packet+写磁盘+转发+PacketResponder'],
         ['BlockSender.java','块读取发送','transferTo零拷贝+readahead+dropCacheBehind'],
         ['DataXceiverServer.java','TCP监听(Reactor)','BlockBalanceThrottler信号量,三维度限流'],
         ['DataXceiver.java','操作分派器','读/写/复制/替换/短路读,长连接复用'],
         ['BlockRecoveryWorker.java','块恢复工作器','Contiguous+Striped两种恢复任务'],
         ['BlockScanner.java','块扫描管理器','TreeMap管理VolumeScanner'],
         ['VolumeScanner.java','单卷扫描线程','优先可疑块,循环缓冲区速率控制'],
         ['DirectoryScanner.java','目录一致性扫描','ForkJoinPool并行,分批reconcile'],
         ['DiskBalancer.java','磁盘均衡器','JSON计划,BlockMover,VolumePair'],
         ['BlockPoolManager.java','BPOfferService管理器','联邦下命名服务动态增删改'],
         ['DNConf.java','配置封装','集中管理所有DataNode配置参数'],
         ['DataStorage.java','存储目录管理','格式化/版本升级/回滚/热插拔'],
         ['FileIoProvider.java','IO代理(装饰器)','ProfilingEvents+FaultInjectorEvents'],
         ['IncrementalBlockReportManager.java','增量块报告','PerStorageIBR缓存块变更'],
         ['ShortCircuitRegistry.java','短路读管理','共享内存段+Slot管理'],
         ['StorageLocation.java','存储位置封装','URI+StorageType(DISK/SSD/ARCHIVE)'],
         ['Replica.java','副本顶层接口','blockId/genStamp/state/numBytes'],
         ['ReplicaInfo.java','副本信息基类','extends Block implements Replica'],
         ['LocalReplica.java','本地副本基类','封装本地磁盘文件操作'],
         ['FinalizedReplica.java','已完成副本','FINALIZED状态,不可变'],
         ['ReplicaBuilder.java','副本建造者','根据State创建不同ReplicaInfo'],
         ['DataNodeMXBean.java','JMX监控接口','集群ID/磁盘使用率/存活DN']],cw=[52*mm,38*mm,80*mm]))
    e.append(Paragraph('表13-2：DataNode根目录核心源文件清单(Top 25)',s_cap))
    e.append(PageBreak())

    # 十四、设计亮点与总结
    e.append(Paragraph('十四、设计亮点与总结',s_h1))
    e.append(Paragraph('14.1 设计亮点',s_h2))
    for b in [
        '<b>Pipeline写入架构</b>：串行转发+ACK逆向传播，Packet粒度流式传输，最大化吞吐量',
        '<b>零拷贝读取</b>：BlockSender使用transferTo(sendfile)系统调用，避免用户态拷贝',
        '<b>Reactor模式数据传输</b>：DataXceiverServer accept+DataXceiver dispatch，支持keepalive复用',
        '<b>SPI可插拔存储</b>：FsDatasetSpi/FsVolumeSpi接口设计，存储实现完全可替换',
        '<b>联邦+HA架构</b>：BlockPoolManager→BPOfferService→BPServiceActor三层管理',
        '<b>建造者模式副本创建</b>：ReplicaBuilder根据状态动态创建不同ReplicaInfo',
        '<b>FileIoProvider装饰器</b>：所有IO操作统一代理，透明插入性能统计和故障注入',
        '<b>增量块报告优化</b>：IncrementalBlockReportManager缓存变更按批上报',
        '<b>三维度限流</b>：BlockBalanceThrottler使用Semaphore控制均衡读/写/传输并发',
        '<b>分批reconcile</b>：DirectoryScanner分批处理避免长时间占锁',
    ]:
        e.append(Paragraph(f'• {b}',s_bull))
    e.append(Paragraph('14.2 质量评估',s_h2))
    e.append(T(['评估维度','评分','说明'],
        [['架构分层','★★★★★','六层架构清晰,门面/管理/传输/存储/扫描/特性分离彻底'],
         ['设计模式运用','★★★★★','12种设计模式精准运用,门面/Reactor/Pipeline/SPI/建造者等'],
         ['可扩展性','★★★★★','SPI接口可插拔存储,策略模式可切换卷选择,命令模式可扩展'],
         ['性能优化','★★★★★','零拷贝读/Pipeline写/增量报告/异步删除/三维度限流'],
         ['并发控制','★★★★☆','读写锁+信号量+异步命令处理,部分场景仍有锁竞争'],
         ['容错设计','★★★★☆','块恢复/目录扫描/坏块检测,但大文件恢复可能较慢']],cw=[30*mm,28*mm,112*mm]))
    e.append(Spacer(1,10*mm))
    e.append(Paragraph('— 文档结束 —',ParagraphStyle('End',fontName='CF',fontSize=12,alignment=TA_CENTER,textColor=HexColor('#9e9e9e'))))
    return e

def main():
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)),'Hadoop_DataNode_release-3.3.5-RC0_核心架构设计文档.pdf')
    doc = SimpleDocTemplate(out,pagesize=A4,topMargin=20*mm,bottomMargin=25*mm,leftMargin=20*mm,rightMargin=20*mm)
    doc.build(build(), onFirstPage=on_page, onLaterPages=on_page)
    print(f'\n✅ PDF已生成: {out}')
    print(f'   大小: {os.path.getsize(out)/1024:.1f} KB')

if __name__ == '__main__':
    main()

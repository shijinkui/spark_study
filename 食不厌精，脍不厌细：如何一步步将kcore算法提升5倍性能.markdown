##		食不厌精，脍不厌细：如何一步步将KCore算法提升5倍性能

@玄畅    
2014.12.27

##		1.		关于

KCore是图算法中的一个经典算法，其计算结果是判断节点重要性最常用的参考值之一。本算法实现原理基于论文Distributed k-Core Decomposition，底层实现基于Spark GraphX。经过不断的优化，充分利用图计算的特点，在亿级别节点上的运行时间，从7.8小时，缩短为1.2小时，并达到稳定的生产级别。本文还原了整个优化过程，希望对做图算法的同学，有所帮助。

**概要描述：**  
开始时，图中每个点的coreness初始值设置为它的度值。然后，所有点把它当前的coreness值发给它的邻居。
接着，在获得邻居的消息后，每个节点计算更新自身的coreness，并且会把变化情况通知它的所有邻居。
如此迭代计算，直到消息数目为零，达到稳定状态，最终得到图中每个点的coreness值。

**具体过程：**    

1.	读`边`的文本文件生成普通的RDD。一行一条边信息，形如：    
	`10,11`    
	`10,12`    
	`11,12`    
	`13,14`   
	`13,15`
2.	构建图。根据1中生成`EdgeRDD`, 滤掉重复的点，构建出`VertextRDD`, 组合成`Graph` (这块内容较为复杂，后续有源码分析)
3.	顶点`VertextRDD`中的attr为默认值`1`, 初始化为对象`KCoreVertex`替换默认值`1`
4.	迭代前的初始化。每个点都向邻居节点发消息。通过`aggregateMessages`函数执行，得到`VertexRDD[Map[Int, Int]]`，即：每个顶点收到的消息
5.	迭代过程
	1.	innerJoin(msg)(vertexProgram)计算每个点K值变化，更新vertextRDD的attr对象`VertexRDD`，得到变化的`changedVerts = VertexRDD[KCoreVertex]`		
	2. outerJoinVertices(changedVerts)。 把1变化的点更新到`Graph`中，其中通过VertexRDD中`isChange=true`表示点有变化
	3. aggregateMessages(sendMsg, mergeMsg) 让Graph中有变化的点向外发消息(执行sendMsg)；reduce时，`mergeMsg`合并每个分区上点收到的消息。得到`VertexRDD[Map[Int, Int]]`
	4. joinVertices。在2中标记了变化的节点，这里需要重置这些标记，`isChange=false`，否则下一轮迭代时直接让这个点发消息(可能这个点在下一轮并没有收到消息，k值也没有变化)

**优化的地方主要是迭代过程，这里耗时最大**

**KCore算法概念图：**        
![k_cores](img/k-cores.png)

![k](img/k.png)
(来自网络)

##		2.		测试数据

亿级节点

点：181640208    
边：890041895

##		3.		优化之前
KCore算法在spark上能跑起来   
问题：    

1.	不能处理到消息数为0    
2.	150迭代会任务失败，内存不够用

```
   运行参数
--driver-memory 60g 
--num-executors 50 
--executor-memory 60g 
--executor-cores 8 
```

**耗时：**
迭代到150轮时耗时: 500秒左右(历史纪录看不到了，大概是这个)

##		4.1		优化过程
问题：

1. 耗内存
2. 代码可读性

###	4.1.1		第1次优化：KCoreVertex对象瘦身

每次迭代发送的消息数为：当前节点到相邻节点。节点数不变，消息一直在骤减直至消息数为0退出迭代。所以优化的重点考虑节点attr属性对象，即：`KCoreVertex`

之前成员变量数14个：

```

class KCoreVertex(val vId: Long, val vDegree: Int, val max: Int, val offset: Int) {

  val coreOffset = offset
  val MAX_K_CORE = max
  val id = vId
  val degree = if (vDegree < MAX_K_CORE) vDegree else MAX_K_CORE
  var preKCore = -1
  var curKCore = degree
  var isChanged = false
  var isInit = false
  val arrayLength = degree - coreOffset
  val kCoreStack = new Array[Int](arrayLength + 1)
  for (i <- 0 to arrayLength) {
    kCoreStack(i) = 0
  }

  def this(other: KCoreVertex) {
    this(other.id, other.degree, other.MAX_K_CORE, other.coreOffset)
    preKCore = other.preKCore
    curKCore = other.curKCore
    isChanged = other.isChanged
    isInit = other.isInit
    for (i <- 0 to arrayLength) {
      kCoreStack(i) = other.kCoreStack(i)
    }
  }

```

优化后变为7个成员变量：

```
class KCoreVertex(val vId: Long, val vDegree: Int, val max: Int) {

  var preKCore = -1
  var curKCore = Math.min(vDegree, max)
  var flag = 0
  //  发生变化在第几次迭代
  var kCoreStack = Array.fill(Math.min(vDegree, max) + 1)(0)

```


**小结:**

1. 每减少一个变量，都需要调整相应的逻辑，本地测试数据的正确性，集群上测性能
2. 减少非必需的变量，节约总是好的习惯
3. 成员变量的减少意味着复杂度的降低，需要更精炼的表达


###	4.1.2		第2次优化：数组的拷贝   
  
数组拷贝可以分为慢拷贝(for)和快拷贝(System.arraycopy)
foreach的慢拷贝多用于源数组和目标数组类型不一致，那就迭代对每个元素转换；快拷贝是native方法，用于数组类型一致的情况.

在KCoreVertex对象中，类型一致，适用快拷贝， 代码中有三处慢拷贝，改为`arraycopy`

本地测试了下，对比如下：

拷贝 | 数组大小 | 耗时(毫秒)
------------ | ------------- | ------------
for | 千万  | 11
arraycopy | 千万  | 8
foreach | 百万  | 5
arraycopy | 百万  | 1


**小结：**    
1.	数组类型一致使用快拷贝`System.arraycopy`

###		4.1.3			第3次优化：scala范型和特化系统的类型约束

特化这里指的具体的实现，只用于KCore算法实现这种情形，不适用其他场景。

泛化一般适用于通用平台，像spark、HSF、metaq、kafka这些系统，他们不关心具体的数据类型，在这些系统中一般用范型或者不关心类型直接传输`byte[]`数据


之前：

```
  def KCorePregel[ED: ClassTag, A: ClassTag]
  (graph: Graph[KCoreVertex, ED],

```

之后：

```
  private def pregel(
    originGraph: Graph[KCoreVertex, Int],

```

**小结:** 
   
具体业务代码中使用确定的类型，代码可读性好一些。

###	4.1.4		第4次优化：函数参数定义

之前：

```

  /**
   * Add time message
   */
  def KCorePregel[ED: ClassTag, A: ClassTag]
  (graph: Graph[KCoreVertex, ED],
   initialMsg: A,
   checkpointPath: String,
   maxIterations: Int = Int.MaxValue,
   activeDirection: EdgeDirection = EdgeDirection.Either)
  (vprog: (VertexId, KCoreVertex, A) => KCoreVertex,
   sendMsg: EdgeTriplet[KCoreVertex, ED] => Iterator[(VertexId, A)],
   mergeMsg: (A, A) => A)
  : Graph[KCoreVertex, ED] = {

```

之后：

```
  /**
   * 核心计算函数, 迭代计算kcore
   * 0. 初始化: mapReduceTriplets生成消息列表
   * 1. innerjoin: 得出所有消息中涉及变更的点newVertexs, vertexProgram计算节点的kcore
   * 2. outerJoinVertices: 更新1中得到的vertex到当前的graph中
   * 4. aggregateMessages: 变更的点向邻居发消息
   *
   */
  private def pregel(
    originGraph: Graph[KCoreVertex, Int],
    checkpointPath: String,
    maxIterations: Int): Graph[KCoreVertex, Int] = {

```

*这个函数根pregel很相似，但是pregel问题在于不能满足部分发消息的需求*

**小结：**      
1.	函数参数尽可能少。 尽量让代码高内聚低耦合

----------------

**效果：**  
 
通过上述几轮scala代码层面上的修改，算法可以运行到第150轮迭代，内存问题暂时解决，整体耗时6小时，提升了将近2小时。

----------------

###		4.2	算法实现的改进   
  	
改进之前的流程图(蓝色标记表示变化的节点)：    
![kcore_runtime_before](img/kcore_runtime_before.jpg)

1. 原先的思路。当前实现为常规思路，对变化的节点通过标记节点属性`ischanged=true/false`，在当前迭代结束后，擦除标记。

2. 迭代过程：       
	1. innerJoin		上一个迭代产生的消息RDD，找到这些消息所在的点及其属性，计算corenness, 并返回coreness变化的节点
	2. outerJoinVertices	用1中变化的节点更新当前graph相对应点的属性对象`KCoreVertex`
	3.	mapReduceTriplets		标记为变化的节点向邻边发消息
	4.	joinVertices		重置标记为初始状态，即`ischanged=false`。不重置的话，下一轮迭代时，当前标记为true的节点会再发消息，但这个点可能在下一轮中并没有变化。

3.	当前主要问题
	*	3次join操作在每次迭代时，**整体耗时每次单调递增1秒**，一秒不大，累计60轮单次迭代增加了1分钟，600轮就是1小时。
	*	在测试过程中，迭代到100轮以后，`outerJoinVertices`和`joinVertices`耗时为140秒左右，为主要耗时的地方
	*	这种问题本地debug发现不了，节点少了不明显，线上无法debug对比每次增量在哪里发生的。走读了graphx的代码也没发现有明显的问题。

-----------

尝试的解决办法：   

1.	mini graph 	
	*	思路：每轮迭代中，在整个graph中标记出变化的节点，向邻边发消息。考虑到单调递增的1秒和join比较耗时，打算在一次迭代结束后，把当前变化的点从graph取出，找到包含这些点的边，以及这些边向外10次延伸的边路经。这样10次迭代就可以用mini graph了。   
	*	放弃原因：要得到这个mini graph，需要做十次消息聚合(mapReduceTriplet), `collectNeighborIds`同样也是用到`mapreduceTriplet`      
2.	去掉ischange变量
	*	放弃原因：不做标记，所有节点全向邻居发消息，节省最后一次join操作，结果迭代次数过多，消息过多，无法跑完整个数据  
3.	染色	见下  

####		4.2.1		最终办法：染色

当前迭代中，在`KCoreVertex`的变量`flag`赋值为最新的迭代号`i=0,1,2...n`, 简称为染色，见下图：

![kcore_runtime_after](img/kcore-runtime-after.jpg)

1.	染色。一个节点在第3次迭代时变化了，标记为:`flag=3`，下一轮没变化`flag=3`保持不变。最终结果是：全图节点的`flag`各个不同，分别等于运行时的`i=0,1,2...n`
2. 去掉第四个join`joinVertices`。不需要重置，每个点都可以保留`i=0,1,2...n`值
3. 使用偏函数，同一迭代过程中，传递第一个参数`i=n`值
	*	`vertexProgram(cur_iter, _: VertexId, _: KCoreVertex, _: Map[Int, Int])`
	*	`sendMsg(cur_iter, _: EdgeContext[KCoreVertex, Int, Map[Int, Int]])`   
	*	cur_iter为当前迭代号

和以往不同的是: **不需要重置标记，只让对当前迭代过程中标记为最新`i=n`的节点发消息**


**测试结果**    

-	边: 890041895, 点:181640208
-	迭代平均耗时：24秒
-	迭代150轮，耗时：1.2h
-	迭代420轮，耗时：3.1h
-	能跑到消息数为0，但是消息数从4到3需要迭代80次左右，建议在消息数为10就退出迭代(不影响最终结果)

**部分日志**

```

14-12-27 23:16:57 [Driver] : {"iter" : 1, "active_msg" : 31147359, "cost" : 456437}
14-12-27 23:21:35 [Driver] : {"iter" : 2, "active_msg" : 19993211, "cost" : 278075}
14-12-27 23:24:28 [Driver] : {"iter" : 3, "active_msg" : 12935166, "cost" : 172991}
14-12-27 23:26:25 [Driver] : {"iter" : 4, "active_msg" : 8568323, "cost" : 116654}
14-12-27 23:27:40 [Driver] : {"iter" : 5, "active_msg" : 5911480, "cost" : 75346}
14-12-27 23:28:31 [Driver] : {"iter" : 6, "active_msg" : 4262917, "cost" : 50365}

14-12-28 01:55:44 [Driver] : {"iter" : 412, "active_msg" : 11, "cost" : 33277}
14-12-28 01:56:15 [Driver] : {"iter" : 413, "active_msg" : 11, "cost" : 30841}
14-12-28 01:56:39 [Driver] : {"iter" : 414, "active_msg" : 11, "cost" : 23936}
14-12-28 01:57:02 [Driver] : {"iter" : 415, "active_msg" : 11, "cost" : 23546}
14-12-28 01:57:27 [Driver] : {"iter" : 416, "active_msg" : 11, "cost" : 24452}
14-12-28 01:57:50 [Driver] : {"iter" : 417, "active_msg" : 11, "cost" : 23491}
14-12-28 01:58:15 [Driver] : {"iter" : 418, "active_msg" : 11, "cost" : 24407}
14-12-28 01:58:46 [Driver] : {"iter" : 419, "active_msg" : 11, "cost" : 31229}
14-12-28 01:59:09 [Driver] : {"iter" : 420, "active_msg" : 10, "cost" : 23603}


```

##		5.		总结

1. 效果对比   

	|  | 平均迭代耗时 | 150轮耗时 | 420轮耗时 | 
	| ------------ | ------------- | ------------ | ------------ |
	|  优化之前 | 188秒 | 7.8h | 跑不完 |
	|  优化之后 | 24秒 | 1.2h   | 3.1h | 
	|	对比	|	提升7倍 | 提升6.5倍 | 由不能跑全程到能3.1h跑完 |

2. scala代码方面         
	1. 减少非必需的变量，养成节约的好习惯
	2. 类型相同的数组复制用arraycopy
	3. 特化系统中尽量使用确定的类型
	4. 函数参数尽可能少，高内聚低耦合
	5. 恰当使用mutable和immutable对象
		测试过程中，`KCoreVertext`对象改用case类和immutable.Map后，耗时大大增加，gc代价高
		在大量生成新对象的地方不建议使用immutable, GC是个大负担, 这也是jvm的痛点。性能要求超高的程序，很多时候选择使用c、c++、golang，大都因为GC     

2. 其他方面

	1. 建立标准数据集。用于验证算法的正确性，小数据集方便本地debug
	2. 每次小改动都应该本地验证正确性，集群上验证效果。
		有时候，很小的改动，在集群中结果大不同
	3. 不断尝试
		念念不忘，必有回响。
	   算法第一个版本并非标准版本，根据当前的主要问题如内存、gc等，不断调整。

##		6.	 感谢

感谢@明风和@刀剑在本文的逻辑和语言表达上给予的指正
	
----EOF----
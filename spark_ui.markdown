#	spark UI源码阅读
-----------
@玄畅   
2015.6.11

>	spark master ui中worker ui的链接使用hostName:8081, 今天上午想改成自定义proxy地址的方式, 索性读一下spark UI的代码，整理如下。

spark集群启动后，用户可以通过Spark提供UI界面实时查看spark任务的执行情况，spark standalone模式下支持离线的UI。

master上使用8080端口提供UI服务，worker上使用8081提供UI服务。

Spark Master上的UI如下:

图1: Spark Master UI    
![](img/spark_ui_master.png)

##		Spark Master start UI

###	执行脚本, 启动Master

spark的启动脚本在`sbin`目录中, 启动时脚本调用路径:     
`start-all.sh` => `stop-master.sh` => `spark-daemon.sh org.apache.spark.deploy.master.Master` => `../bin/spark-submit` => `spark-class org.apache.spark.deploy.SparkSubmit` 

启动最终的入口为`SparkSubmit`, 在这里解析args参数, 执行`org.apache.spark.deploy.master.Master`的main函数, 怎么执行的？`mainMethod.invoke(null, childArgs.toArray)`[SparkSubmit.scala 620行]

###	Master启动UI: 初始化`WebUIPage`和`ServletContextHandler`

Master继承自`Actor`, 成员变量初始化`private val webUi = new MasterWebUI(this, webUiPort)`, 在Actor的`preStart`中, 真正绑定UI端口。

再看一下`MasterWebUI`, 它的父类是`WebUI`, 实现抽象类`WebUI`的子类有:`MasterWebUI`,`HistoryServer`, `MesosClusterUI`, `SparkUI`, `WorkerWebUI`。这些都是真正提供web ui服务的。

MasterWebUI在初始化的时候(initialize), 初始化Master UI的web组件:  `ContextHandler`，具体的为: 

Component | path | Description
------------ | ------------- | -------------
MasterPage | / | 渲染workers, 活跃的appliction, 活跃的drivers
ApplicationPage | /app | 根据request中的appId参数, 展示这个活跃的application
HistoryNotFoundPage | /history/not-found | 历史信息找不到404
static handler | 加载静态资源, 见这里:`resources/org/apache/spark/ui/static/`
ApiRootResource handler| -
redirect handler| /app/kill, /driver/kill | 把这俩kill请求转发给master page处理, Master异步kill

`WebUIPage`继承自`WebUI`, `WebUI`有两个抽象函数`render(request: HttpServletRequest): Seq[Node]`和`renderJson(request: HttpServletRequest): JValue`。需要注意的是成员参数`prefix`, 这个就是这个页面对应的http path了。

看参数可以猜出, 接收servlet request, 处理请求, 返回Node或者Json结果。

看页面`WebUIPage`中的`content`变量, 一大堆的html代码, 补上所需的变量, 加上公共部分页头`UIUtils.basicSparkPage`, 就是一个完整的渲染后的页面了。

每个页面都需要有一个对应page handler和json handler。比如：`http://localhost:8080`返回的是渲染的html，`http://localhost:8080/json/`返回的是渲染的json

###	Master启动UI: startJettyServer

`Master.scala`在prestart调用`webUi.bind()`, 生成server实例, 监听web端口, jetty server绑定handler。然后就成了。

前面page对应handler以及添加的其他handler, 生成一个`ContextHandlerCollection`, 其他就是标准的jetty接口了。

图2: Jetty Server接口     
![](img/spark_ui_jetty_server.png)

```

    // Bind to the given port, or throw a java.net.BindException if the port is occupied
    def connect(currentPort: Int): (Server, Int) = {
      val server = new Server(new InetSocketAddress(hostName, currentPort))
      val pool = new QueuedThreadPool
      pool.setDaemon(true)
      server.setThreadPool(pool)
      val errorHandler = new ErrorHandler()
      errorHandler.setShowStacks(true)
      server.addBean(errorHandler)
      server.setHandler(collection)
      try {
        server.start()
        (server, server.getConnectors.head.getLocalPort)
      } catch {
        case e: Exception =>
          server.stop()
          pool.stop()
          throw e
      }
    }

```

绑定端口, 添加pool, 设置handler, start, 齐活。


##		Spark Worker start UI
sbin目录中, `start-all.sh` => `start-slaves.sh` => `start-slave.sh` => `spark-daemon.sh start org.apache.spark.deploy.worker.Worker` => `../bin/spark-submit` => `spark-class org.apache.spark.deploy.SparkSubmit` 

入口`SparkSubmit`调用Worker的main函数. Worker同样是一个Actor, prestart里初始化webUi. 

```
webUi = new WorkerWebUI(this, workDir, webUiPort)
webUi.bind()

```

`WorkerWebUI`同样实现了抽象类`WebUI`, 注册的web组件有:

Component | path | Description
-----|-----|----
LogPage | /logPage | 根据appId/executorId/driverId定位日志
WorkerPage | / | 展示worker中的executor以及driver信息
static handler | /static | 静态资源加载
log handler | /log | 请求log，由LogPage渲染


##		读后感

###	embedded jetty
内嵌的jetty非常好用, 之前改过一个[jetty-embedded-spring-mvc](https://github.com/shijinkui/jetty-embedded-spring-mvc)和[jetty-embedded-spring-mvc-noxml](https://github.com/shijinkui/jetty-embedded-spring-mvc-noxml), 当时主要做从web容器resin到非容器的过度(finagle/rest.li/dropwizard)

embedded jetty, 轻, QPS比web容器高多了。去重量级web容器是大势所趋。

###	page, handler, render
收到用户request, 匹配到hander, 渲染页面, 返回。


###	耦合
UI的代码也不少, 一大堆的资源文件(css/js/images)都放在core里。而web界面的进化必然是越来越重, UI越来越炫, 功能越来越丰富, 比如: 提供notebook功能。

UI放在core里主要是方便使用Master这个actor查询worker/application/driver的状态。

一个更好的方式是：

1. Master提供标准的metric接口和rest操作
	可以通过actor message沟通, 也可以开端口用RestFul方式, 也可以用netty保持tcp长链接, decode/encode Message的交互方式。
2. Spark UI独立模块
	天高任鸟飞, 让UI越来越丰富。

为什么要让UI更丰富？降低使用门槛, 方便监控。





 
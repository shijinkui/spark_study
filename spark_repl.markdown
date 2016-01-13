##		spark repl 图解(v0.1)
> author: 玄畅  时金魁  2016.1.12

本文基于[spark master代码](https://github.com/apache/spark)分析, 当前最新的spark版本为:2.0.0-SNAPSHOT, 最新commit id: [8cfa218](https://github.com/apache/spark/commit/8cfa218f4f1b05f4d076ec15dd0a033ad3e4500d), Commits on Jan 12, 2016

###	overview
----------
repl: `Read! Eval! Print! Loop..`, 顾名思义就是, 读取输入, 求值, 打印.

[jline2](https://github.com/jline/jline2)是一个java实现的repl, 有个[exaple](https://github.com/jline/jline2/blob/master/src/test/java/jline/example/Example.java)，可以感受下是repl怎么回事。


spark repl鲜有人说，大概因为repl是非必需品，在生产和调试spark时几乎用不到repl。在刚接触spark时，跑一下 [Spark Examples
](http://spark.apache.org/examples.html)时, 一般会直接在`spark-shell`里跑一下样例。

下文就是从`spark-shell`入口剖析下spark repl的运行路径。

###	used by
----------
用到repl的应用:

1.	spark shell
2.	hue livy
3.	spark-notebook

适用于spark交互式场景, 操作界面一般是notebook, 在web上写spark代码, 直接在web上运行，输出结果。

为什么notebook比较受欢迎？   
配置好集群后，直接通过web界面(notebook)运行spark作业，**渲染**输出结果，特别适用于作业调试，方便、快速。算法工程师焦点于job作业，而不需要关心底下的spark集群。

###	full graph
----------

![](img/spark_repl_shijinkui_20160112/spark_repl.jpg)

###	entrance
----------
从`bin/spark-shell`文件作为研究spark repl的入口。

```
export SPARK_SUBMIT_OPTS
"$FWDIR"/bin/spark-submit --class org.apache.spark.repl.Main --name "Spark shell" "$@"
```

`bin/spark-shell`向`bin/spark-submit`提交main为`org.apache.spark.repl.Main`的scala object。Main是spark-repl包里的类，所以不需要添加jar，spark自带的。

`bin/spark-submit`

```
exec "${SPARK_HOME}"/bin/spark-class org.apache.spark.deploy.SparkSubmit "$@"
```

`/bin/spark-class`负责读取环境配置，所需的jar, 然后执行org.apache.spark.deploy.SparkSubmit.main(), SparkSubmit会:

1. 生成指定的类加载器
2. 把jar添加到classpath
3. 设置系统属性 System.setProperty
4. `--class`指定的类, invoke it


经过前面的spark运行环境准备工作，后面进入到`org.apache.spark.repl.Main.main()`, 这个Main对象就是repl的入口了。

![](img/spark_repl_shijinkui_20160112/1_entrance.jpg)


###	repl
----------
org.apache.spark.repl.Main.main()函数很简单, new一个`SparkILoop`对象, 调用它的`process()`函数。

SparkILoop是repl处理输入-求值-打印的主要地方. 

一个repl的过程大致有以下4个步骤:

1. 读取控制台输入
2. 编译输入的代码, 生成AST
3. apply, 执行编译后的字节码
3. 输出结果

SparkILoop对象有两个关键的成员变量:

1.	`intp: SparkIMain` 解释器
2.	`in: InteractiveReader` 控制台输入reader

一个读取用户输入, 一个解释执行输入的代码，打印结果。

####	step 1:

第一个进入的是`process()`函数, 这个主要是把输入参数转化成`SparkCommandLine`对象, 如果输入参数不是帮助说明参数, 进入process(command.settings)函数。

```
  /** process command-line arguments and do as they request */
  def process(args: Array[String]): Boolean = {
    val command = new SparkCommandLine(args.toList, msg => echo(msg))
    def neededHelp(): String =
      (if (command.settings.help.value) command.usageMsg + "\n" else "") +
      (if (command.settings.Xhelp.value) command.xusageMsg + "\n" else "")

    // if they asked for no help and command is valid, we call the real main
    neededHelp() match {
      case ""     => command.ok && process(command.settings)
      case help   => echoNoNL(help) ; true
    }
  }
```

####	step 2: 

1. 创建解释器
	
	process(command.settings)函数，首先会检查master是否是`yarn-client`，如果是则设置系统变量`SPARK_YARN_MODE=true`, 标示当前是yarn模式。

	接着会执行函数`createInterpreter()`创建一个解释器对象, 这个解释器会准备基本的运行环境。后续用户输入的代码，都是通过这个解释器执行。

	查询是否设置了系统变量`spark.jars`，如果不存在则读取另外一个系统变量`ADD_JARS`是否存在。由此得到一个逗号`,`分割的jar列表, 解析每个jar的URL, 添加到classpath中。

	构建一个解释器对象`SparkILoopInterpreter`, 赋值给`SparkILoop`的变量`intp`

	解释器继承自`InteractiveReader`, 固定的生命周期为:
	
	```
	val interactive: Boolean
	def init(): Unit
	def reset(): Unit
	def history: History
	def completion: Completion
	def eraseLine(): Unit
	def redrawLine(): Unit
	def currentLine: String
	def readYesOrNo(prompt: String, alt: => Boolean): Boolean
	def readAssumingNo(prompt: String)
	def readAssumingYes(prompt: String) 
	def readLine(prompt: String): String
	```
	
	规范见[这里](http://cnswww.cns.cwru.edu/php/chet/readline/readline.html#SEC9)
	
	*2.1 创建解释器:*    
	![](img/spark_repl_shijinkui_20160112/2_1_create_intp.jpg)
	
2. 创建控制台输入reader

	有两类reader：
	
	*	用户指定readerBuffer了, new一个`SimpleReader`对象
	*	根据settings配置选择一个reader对象，new一个`SparkJLineReader`或`SimpleReader`对象 

	reader对象生成后, 赋值给`SparkILoop`的变量`in`, 后续读取输入。
	
	*2.2 选择reader:*    
	![](img/spark_repl_shijinkui_20160112/2_2_chose_reader.jpg)

3. 添加bind绑定到执行列表
	
	3,4,5,6都是把要执行的函数放到`pendingThunks:List[() => Unit]`中, 这些函数都是需要在解释器初始化后被执行。
	
	名为绑定，要绑定个啥？    
	把一个key-value设置到intp解释器中，在后面的用户输入的表达式中可以直接引用这个key-value。在后续代码片段生成的class中引入intp作为成员变量，这样就可以直接用了。
	
	先new一个`val bindRep = new ReadEvalPrint()`对象, ReadEvalPrint表达一个repl的过程。`bindRep`首先编译object类，再调用`set`函数, 把SparkIMain对象set进去。
	
	//	todo CodeAssembler, ObjectSourceCode, ResultObjectSour	ceCode
	
	```
	object ${bindRep.evalName} {
	  var value: ${boundType} = _
	  def set(x: Any) = value = x.asInstanceOf[${boundType}]
	}
	```
	
	上面这个静态类就是要编译的代码, 查询系统属性`scala.repl.name.eval`, 如果不存在就以`$eval`作为类名。成员变量`boundType`就是`SparkIMain`了。
	
	`bindRep.callEither()`调用上面生成的object的set函数，把SparkIMain对象set进去。
	
	这样，直接调用生成object对象`${bindRep.evalName}`的value属性就可以用`SparkIMain`了。
	
	*2.3 绑定*    
	![](img/spark_repl_shijinkui_20160112/2_3_bind.jpg)

4. 添加repl自动执行代码到执行列表
	
	在`scala.tools.nsc.ReplProps`中定义的变量`replAutorunCode`, 会引用系统变量`scala.repl.autoruncode`, 如果用户设置了这个属性, 则会读取对应的value, value一般是指向一个代码文件, 如果确实存在这个源代码文件, 则调用编译执行函数。
	
	上述整个过程添加到待执行列表。	
	
5. 添加欢迎信息函数到执行列表
	
	控制台输出欢迎字符串:
	
	```
	      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version %s
      /_/
	```
	
6. 添加初始化spark环境函数到执行列表
	
	编译执行以下代码块:
	
	```
	//	1. command
	@transient val sc = {
	  val _sc = org.apache.spark.repl.Main.interp.createSparkContext()
	  println("Spark context available as sc " +
	    s"(master = ${_sc.master}, app id = ${_sc.applicationId}).")
	  _sc
	}

	//	2. command
	@transient val sqlContext = {
	  val _sqlContext = org.apache.spark.repl.Main.interp.createSQLContext()
	  println("SQL context available as sqlContext.")
	  _sqlContext
	}

	//	3. command
	import org.apache.spark.SparkContext._
	//	4. command
	import sqlContext.implicits._
	//	5. command
	import sqlContext.sql
	//	6. command
	import org.apache.spark.sql.functions._
	
	```
	
	导入必需的类, 声明sc和sqlContext变量, 直接暴露这俩变量给用户使用。
	**notebook也主要以此两个变量作为调试job代码的入口。**

	上述整个过程添加到待执行列表。	
7. 解释器初始化
	
	解释器`intp`根据用户的设定是否为异步执行同步初始化或异步初始化。目前固定为同步初始化。
	
	> 异步初始化的设置位置为: `scala.tools.ScalaSettings`    
	> val Yreplsync       = BooleanSetting    ("-Yrepl-sync", "Do not use asynchronous code for repl startup")
	
	初始化的过程就是编译执行名为`<init>`代码:`class $repl_$init { }`，一个空类。
	
	如果这个空类编译执行报错，那么整个repl就会hang死翘翘了。更像是个探针，测试下scala编译执行环境是否可用。
	
8. 执行3,4,5,6添加到执行列表中的函数
	
	初始化完毕后, 遍历之前添加到`pendingThunks`列表中的待执行函数，apply执行之。
	
9. loop开始干活
		
	解释器初始化正常，执行系统定义的代码:绑定、自动执行代码、welcome、初始化spark变量，一切正常则开始无尽循环的正事: read-eval-print, readLine => processLine。
	
	loop退出条件为：    
	*	读到的行为null
	*	行命令执行结果`Result`的`keepRunning==false`     
		`case class Result(val keepRunning: Boolean, val lineToRecord: Option[String])`
	
	读到行代码, 执行代码内容, 执行函数路径为: 
	
	1. 解析		command(line) 
	2. 开始调用解释器		`interpretStartingWith(code: String)`
	3. 调用解释器	`intp.interpret(code) `
	4. 生成语法树	`requestFromLine(line, synthetic)`
	5. 加载上下文环境, 执行语法树。`loadAndRunReq(req: Request)`
	6. 调用用户输入的代码		`call()`      
		`m.invoke(evalClass, args.map(_.asInstanceOf[AnyRef]): _*)` 调用
		
		这里的Method是把line source code放到一个函数中, 生成一个class, 然后调用执行。
		
	*2.9 repl*    
	![](img/spark_repl_shijinkui_20160112/2_8_loop_rep.jpg)

--------------

*转载请注明原作者*

--------EOF---------
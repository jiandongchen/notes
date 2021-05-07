# 记一次YARN - ResourceManager进程意外退出的解决过程

## 背景

1月6日凌晨，YARN的ResourceManager进程突然退出，导致集群无法正常使用。先尝试重启了ResourceManager进程，暂时修复了问题，保证了主调度的正常运行。之后对此次事件的发生原因进行了深入调查，最后基本了解了起因及做了修复。这边在这里做一个梳理（以下用RM指代ResourceManager）。

## 调查过程

1. 查看cm的警报日志，发现进程退出原因为oom

   ![cm警报日志](https://github.com/jiandongchen/notes/blob/main/summary/yarn/images/cm警报日志.jpg)

2. RM发生oom是比较奇怪的，因为理论上它不像是Hadoop的NameNode，需要在内存中保存一份完整的元数据，内存不应该有过大的压力。确定了是oom之后，自然我就去查看了RM堆栈的运行历史和gc情况

   RM堆栈：

   ![rm堆栈历史](https://github.com/jiandongchen/notes/blob/main/summary/yarn/images/rm堆栈历史.png)

   gc：

   ![gc历史](https://github.com/jiandongchen/notes/blob/main/summary/yarn/images/gc历史.png)

   看到这里oom的结论已经很清楚了，从图上能很明显的看出，随着时间推移堆内存不断在变大，而gc时间也在不断增加，直到达到内存设置上限（默认为1GB）发生oom进程退出。

   那么问题来了，为什么堆内存随着时间在不断变大，而gc并不能完成这部分的垃圾回收？

   为了解决这个疑问，很明显的需要对当时的堆内存快照进行分析。但当时的内存快照并没有保存下来，只能使用重启之后目前的内存快照看看能不能找到问题产生的原因。

   步骤：

   1. 使用yarn账号登陆yarn部署节点

   2. 输入jps命令

      ![jps](https://github.com/jiandongchen/notes/blob/main/summary/yarn/images/jps.jpg)

      可以看到RM的PID为16371

   3. 执行命令 jmap -dump:format=b,file=resourcemanager.hprof 16371 生成.hprof文件

   4. 使用MemoryAnalyzer打开该文件

      ![mat1](https://github.com/jiandongchen/notes/blob/main/summary/yarn/images/mat1.jpg)

      可以观察到有一个实例**org.apache.hadoop.yarn.server.resourcemanager.RMActiveServiceContext**

      中的一个**java.util.concurrent.ConcurrentHashMap**占据了绝大部分的内存

      ![hashmapEntry](https://github.com/jiandongchen/notes/blob/main/summary/yarn/images/.jpg)

      观察这个hashmap，可以看到其中的node对象为10001（截图为将yarn.resourcemanager.max-completed-applications参数的默认值10000修改为2000后的，所以截图为2001）个。

      那这个hashmap是用来做什么的？为什么不会被垃圾回收？

   5. 查看该类的源码

      ![RMActiveServiceContext](https://github.com/jiandongchen/notes/blob/main/summary/yarn/images/.png)

      有一个hashmap吸引了我的注意，applications，key为appid，而value是app对象。难道yarn将application的信息存储在内存中？

   6. 在yarn的官方文档上果然找到了一个参数

      yarn.resourcemanager.max-completed-applications解释为The maximum number of completed applications RM keeps.并且默认值正好是10000

      查看yarn-applications界面：

      ![yarn-applications](https://github.com/jiandongchen/notes/blob/main/summary/yarn/images/.png)

      可以看到entries值为10001（此为修改后的截图，所以为2001）

## 结论

那么至此，oom的原因已经很明显了 - RM会在内存中初始化一个hashmap，并在其中保存执行的application的信息。这个hashmap始终存在，无法被回收，并且默认大小为10000。但是默认给RM分配的内存大小为1GB，当RM中的application数增长到一定数目时，oom发生。

## 解决过程

### 第一次尝试解决

第一次我们将RM的内存大小修改为2GB，修改完成后，发现oom果然不再发生，但RM的gc时间却在不断增加，并且很奇怪的一点是full gc次数远大于young gc次数，并且几乎是一直在full gc，很明显还是有问题，但老年代内存只是使用到了89%，并没有用满。

![resourcemanager-gc](https://github.com/jiandongchen/notes/blob/main/summary/yarn/images/resourcemanager-gc.png)

查看RM启动参数

![rm启动参数](https://github.com/jiandongchen/notes/blob/main/summary/yarn/images/rm启动参数.jpg)

发现RM的jvm启动参数有-XX:+UseConcMarkSweepGC，通过查阅资料得知这个参数表示对于老年代的回收采用CMS。CMS采用的基础算法是：标记—清除，而-XX:CMSInitiatingOccupancyFraction=70这个参数表示设定CMS在对内存占用率达到70%的时候开始GC。

这样问题点就很明显了，2GB内存还是小了，老年代占用内存的百分比超过了70%，而full gc又不能回收出足够资源。

### 第二次尝试解决

在第二次尝试解决时，突然想到，比起调大RM分配内存，是不是可以通过降低RM中的hashmap存储app信息的数量来同样解决问题。因为我们集群的情况是一般不需要回看到之前10000的app，并且因为绝大部分app都为spark任务，历史情况也可以至spark history server查看。

修改yarn.resourcemanager.max-completed-applications参数为2000

经过一段时间的观察，RM内存使用情况平稳，问题得到解决。

![rm内存情况-新](https://github.com/jiandongchen/notes/blob/main/summary/yarn/images/rm内存情况-新.jpg)


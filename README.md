# 哈利波特的魔法世界————大数据实验综合项目
# 一、小组介绍

# 二、项目环境
+ Ubuntu 20.04
+ Hadoop 3.3.5
+ Maven 3.9.1
+ Java 1.8
+ Python 3.8.10
+ 
# 三、构建手册
## 1. 本地单机伪分布式
搭建好上述环境后，进入项目目录下，运行
```shell
mvn clean install
```
下载所需依赖。

然后修改`pom.xml`中的主类名，分别编译`src/java/main/java`下的六个软件包。（没有做多模块编译）

软件包的名称设置分别为`run.sh`下的软件包路径，例如`WizardingWorldOfHPTask1-1.0-SNAPSHOT.jar`。

```shell
mvn package
```

在target目录下生成jar包。

启动本机Hadoop环境，上传输入文件，在`run.sh`下设置输入文件路径、输出文件路径、jar包位置等信息，执行命令：

```shell
./run.sh
```

运行tasks。

等待运行结束后，将relationshipsFilter和task5的输出复制到本项目对应output文件夹下，可运行 `runPython.sh`导出excel表格、本地neo4j数据库，和生成markdown文件。

具体的路径、信息设置请查看 `src/main/python`下的脚本文件。

# 四、任务分工

## 1. 基本任务
|    任务     | 负责人 |
|:---------:| :----: |
| 实验框架及文档编写 | 闫慧渊 |
|   task1   | 闫慧渊 |
|   task2   | 李尚旺 |
|   task3   | 李尚旺 |
|   task4   | 乔冠霖 |
|   task5   | 闫慧渊 |
 
## 2. 可视化任务

+ neo4j、gephi可视化图形展示： 闫慧渊
+ obsidian+digital garden 网页展示： 李尚旺

# 五、结果展示

//TODO
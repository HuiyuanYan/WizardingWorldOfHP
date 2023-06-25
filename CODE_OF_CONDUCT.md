# 一、代码风格

## 1.文件/文件夹/包的命名

文件夹和包命名：

文件夹和包的命名与项目名称相关，使用小写字母和单词之间用下划线分隔。
示例：word_count_analysis

## 2.命名规范
- 类名使用驼峰式命名法，首字母大写。
    ```java
     public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
         // Class implementation...
     }
     ```
- 方法名和变量名使用驼峰式命名法，首字母小写。
  ```java
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      // Method implementation...
  }
  ```
- 常量名使用全大写，单词之间用下划线分隔。
  ```java
  private static final int MAX_RETRIES = 3;
  ```
- 包名使用全小写。
  ```java
  package com.example.hadoop;
 
  // Class implementation...
  ```


## 3.注释规范
+ 在每个类、方法和重要的代码块前添加注释，解释其功能和作用。
     ```java
         /**
     * @description 从缓存文件中读取名字字典
     * @param uri: 缓存文件uri
     * @return void
     * @author Yan
     * @date 2023/6/23 上午5:58
     */
    private void readNameDict(URI uri)
    {
        ...
    }
     ```
  
## 4.代码格式化
- 使用适当的缩进和空格对代码进行格式化，使其更易于阅读。
     ```java
     public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
         // Method implementation...
         if (condition) {
             // Code block...
         } else {
             // Code block...
         }
     }
     ```
- 使用大括号对代码块进行明确的定界。
  ```java
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      // Method implementation...
      if (condition) {
          // Code block...
      } else {
          // Code block...
      }
  }
  ```
- 在适当的位置添加空行，以提高代码的可读性。
  ```java
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      // Method implementation...
 
      // Some code...
 
      // More code...
 
      // Final code...
  }
  ```

## 5.异常处理

使用合适的异常处理机制来捕获和处理异常。
```java
public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    try {
        // Method implementation...
    } catch (IOException e) {
        // Handle IO exception...
    } catch (InterruptedException e) {
        // Handle interrupted exception...
    }
}
```

# 二、文件结构
```
├── CODE_OF_CONDUCT.md
├── dependency-reduced-pom.xml 
├── input #各个任务的输入文件夹
│   ├── task1
│   └── task2
├── output #各个任务的输出文件夹
│   └── task1
│       └── task1_output
├── pom.xml
├── README.md
├── src #源代码
│   ├── main
│   │   ├── java
│   │   │   ├── task1
│   │   │   │   ├── task1Driver.java
│   │   │   │   ├── task1Mapper.java
│   │   │   │   └── task1Reducer.java
│   │   │   ├── task2
│   │   │   ├── task3
│   │   │   ├── task4
│   │   │   ├── task5
│   │   │   └── tasks_driver
│   │   └── resources #资源文件，有需要可以添加在这里，打包时会一起打包进取
│   │       ├── config.properties
│   │       ├── log4j.properties
│   │       └── nameList.txt
│   └── test
│       └── java
└── target
```

完成task时（以task1）举例，你需要：

1. 在`src/main/java`下对应的task编写相应的类。且必须包含：
   + `task1Driver`：task的启动类
   + `task1Mapper`：mapper
   + `task1Reducer`：reducer
  
其他的类可根据具体情况选择添加。

2. 从上级任务输出/课程网站上获取你需要的输入文件，并添加到`input/task1`中，作为你的输入文件。

3. 将输入文件上传到你本地的hdfs系统上，准备测试。

4. 修改`pom.xml`第43行的主类，修改为你编写的`driver`类所在位置（例如`task1.task1Driver`），之后进行编译，上传至本地hdfs系统运行。

5. 确保输出无误后，将输出文件复制到`output/task1/task1_output.txt`中。

6. 在`PROJECT_IDEA.md`中写下你实现的思路和细节。

7. 将代码上传到github，等待其他组员PR。
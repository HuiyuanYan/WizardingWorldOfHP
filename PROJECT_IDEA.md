# 项目实现思路及细节

## task1
### 1.nameList处理

//TODO: 添加思路

名字的归一化处理：

+ 对于小说中出现的主要人物，例如`Harry Potter`，小说中多次出现的`Harry`或`Potter`都指代`Harry Potter`，因此把`Harry`，`Potter`都归一化为`Harry Potter`。
+ 对于小说中出现的次要人物，例如`Neville Longbottom`，对其名`Neville`归一化为`Neville Longbottom`，其姓氏则特指`Longbottom`一家，不进行归一。
再如`Godric Griffindor`，其姓氏`Griffindor`一般特指`Griffindor`学院，不进行归一。

+ 按照上述规则处理好，以`Lexeme:Name`的形式存入文件`nameList.txt`中。

### 2.mapReduce设计

+ Driver：将处理好的`nameList.txt`读入缓存文件，配置任务。
+ Mapper：读取小说的每一行（段），尽可能长的匹配每一个名字，如果最终一段中出现名字数大于等于2，则写入：`<Names,NullWritable>`，`Names`为该段出现的人名，以逗号分割；`NullWritable`为一可写入的空对象。
+ Reducer：将`<Names,NullWritable>`直接写入输出即可。

## task2
//TODO
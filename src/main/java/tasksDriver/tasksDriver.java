package tasksDriver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import task1.task1Driver;
import task2.task2Driver;
import task3.task3Driver;
import task4.task4Driver;
import relationshipFilter.FilterStarter;
import task5.task5Driver;

public class tasksDriver {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if ((remainingArgs.length != 2)) {
            System.err.println("Usage: HPTasks <nameList_path> <dataSet_path>");
            System.exit(2);
        }

        //在这里设置你的输出路径
        String task1_output_path="/user/yhy/output1";
        String task2_output_path="/user/yhy/output2";
        String task3_output_path="/user/yhy/output3";
        String task4_output_path="/user/yhy/output4";
        String filter_output_path="/user/yhy/filterOutput";
        String task5_output_path="/user/yhy/output5";

        task1Driver driver1 = new task1Driver();
        task2Driver driver2 = new task2Driver();
        task3Driver driver3 = new task3Driver();
        task4Driver driver4 = new task4Driver();
        FilterStarter filterStarter = new FilterStarter();
        task5Driver driver5 = new task5Driver();


        String infoStr = "\033[0;32m[INFO]\033[0m ";


        String[] param1 = {remainingArgs[0],remainingArgs[1],task1_output_path};
        System.out.println(infoStr + "running task1...");
        driver1.main(param1);
        System.out.println("task1 finished");

        String[] param2 = {task1_output_path,task2_output_path};
        System.out.println(infoStr + "running task2...");
        driver2.main(param2);
        System.out.println("task2 finished");

        String[] param3 = {task2_output_path,task3_output_path};
        System.out.println(infoStr + "running task3...");
        driver3.main(param3);
        System.out.println("task3 finished");

        String[] param4 = {task3_output_path,task4_output_path};
        System.out.println(infoStr + "running task4...");
        driver4.main(param4);
        System.out.println("task4 finished");

        String[] paramFilter = {task3_output_path,filter_output_path};
        System.out.println(infoStr + "running filter...");
        filterStarter.main(param4);
        System.out.println("filter finished");

        String[] param5 = {filter_output_path,task5_output_path};
        System.out.println(infoStr + "running task5...");
        driver5.main(param4);
        System.out.println("task5 finished");

        System.out.println(infoStr + "all task finished");


    }
}

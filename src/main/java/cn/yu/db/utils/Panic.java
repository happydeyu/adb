package cn.yu.db.utils;

/**
 * @author Yu
 * @description TODO
 * @date 2023-08-06
 */

public class Panic {
    public static void panic(Exception e){
        e.printStackTrace();
        System.exit(1);
    }
}

package com.hhjt.hadoop;

import java.util.ArrayList;

/**
 * Created by root on 2015-8-10.
 */
public class Demo {
    public static ArrayList<Integer> getNumbers(int m, int n){
        ArrayList<Integer> res = new ArrayList<Integer>();

        for (int i = m; i <= n; i++){
            int h = i/100;
            int hs = i%100;
            int t = hs/10;
            int g = hs%10;
            if (h*h*h+t*t*t+g*g*g == i)
                res.add(i);
        }

        return res;
    }
    public static void main(String[] args){
//        int m = Integer.valueOf(args[0]);
//        int n = Integer.valueOf(args[1]);
        ArrayList<Integer> data = getNumbers(300,380);
        if (data.size()==0){
            System.out.println("no");
        }else {
            System.out.println(data);
        }
    }
}

package com.spark.sql.app;

public class View {
    private int number ;
    public void setNumber(int n){
        number = n;
    }
    public void addToNumber(int n){
        number += n;
    }
    public int getNumber(){
        return number;
    }
}

package com.bigdata.spark.sql.rdd;

import java.io.Serializable;

public class Student implements Serializable {
    private static final long serialVersionUID = 5176154204172119134L;
    private String name;
    private String gender;
    private int age;
    private int id;

    public Student() {
    }

    public Student(String name, String gender, int age, int id) {
        this.name = name;
        this.gender = gender;

        this.age = age;
        this.id = id;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    @Override
    public String toString() {
        return "name: " + this.name + "    gender: " + this.gender;

    }
}

package com.singularity.kafkaconsumer.model;

import org.springframework.cloud.gcp.data.datastore.core.mapping.Entity;
import org.springframework.data.annotation.Id;

@Entity(name = "Users")
public class User {

    @Id
    private Long id;
    private String name;
    private String surname;
    private Integer age;
    private String gender;
    private String nickName;

    public User(String name, String surname, Integer age, String gender, String nickName) {
        this.name = name;
        this.surname = surname;
        this.age = age;
        this.gender = gender;
        this.nickName = nickName;
    }

    public User(){}

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSurname() {
        return surname;
    }

    public void setSurname(String surname) {
        this.surname = surname;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getNickName() {
        return nickName;
    }

    public void setNickName(String nickName) {
        this.nickName = nickName;
    }

    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", surname='" + surname + '\'' +
                ", age=" + age +
                ", gender='" + gender + '\'' +
                ", nickName='" + nickName + '\'' +
                '}';
    }
}

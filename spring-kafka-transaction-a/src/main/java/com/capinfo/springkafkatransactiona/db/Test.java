package com.capinfo.springkafkatransactiona.db;

import lombok.Data;

import javax.persistence.*;

/**
 * @author zhanghaonan
 * @date 2022/3/8
 */
@Entity
@Table(name = "test")
@Data
public class Test {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "content", nullable = false)
    private String content;
}

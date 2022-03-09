package com.capinfo.springkafkatransactionb.db;

import org.springframework.data.jpa.repository.JpaRepository;

/**
 * @author zhanghaonan
 * @date 2022/3/8
 */
public interface TestRepository  extends JpaRepository<Test, Long> {
}

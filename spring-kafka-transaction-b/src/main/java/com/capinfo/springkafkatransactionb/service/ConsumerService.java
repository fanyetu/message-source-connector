package com.capinfo.springkafkatransactionb.service;

import cn.hutool.core.util.IdUtil;
import com.capinfo.springkafkatransactionb.db.Test;
import com.capinfo.springkafkatransactionb.db.TestRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

/**
 * @author zhanghaonan
 * @date 2022/3/9
 */
@Service
public class ConsumerService {

    @Resource
    private TestRepository testRepository;

    @Transactional
    public String consume(String data) {
        Test test = new Test();
        test.setContent(IdUtil.fastSimpleUUID());
        testRepository.save(test);

        return data.toUpperCase();
    }
}

package com.capinfo.appa.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;

/**
 * @author zhanghaonan
 * @date 2022/3/9
 */
@FeignClient(name = "test", url = "http://localhost:8081/app-b")
public interface TestClient {

    @PostMapping("/test")
    String test(String data);
}

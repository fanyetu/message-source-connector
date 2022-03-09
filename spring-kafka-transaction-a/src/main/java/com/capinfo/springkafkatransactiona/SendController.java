package com.capinfo.springkafkatransactiona;

import com.capinfo.springkafkatransactiona.service.SendService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * @author zhanghaonan
 * @date 2022/3/1
 */
@RestController
@RequestMapping("/app-a")
public class SendController {

    @Resource
    private SendService sendService;

    @PostMapping("/sendWithTransaction")
    public String sendWithTransaction(@RequestBody String data) {
//        return sendService.sendWithTransaction(data);
        return sendService.sendWithTransactionAsync(data);
    }

    @PostMapping("/send")
    public String send(@RequestBody String data) throws InterruptedException, ExecutionException, TimeoutException {
        return sendService.send(data);
    }
}

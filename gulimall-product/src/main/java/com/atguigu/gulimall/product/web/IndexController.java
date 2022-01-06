package com.atguigu.gulimall.product.web;

import com.atguigu.gulimall.product.entity.CategoryEntity;
import com.atguigu.gulimall.product.service.CategoryService;
import com.atguigu.gulimall.product.vo.Catelog2Vo;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Controller
public class IndexController {

    @Autowired
    private CategoryService categoryService;

    @Autowired
    private RedissonClient redisson;

    @GetMapping({"/", "/index.html"})
    public String indexPage(Model model) {
        //TODO 查出所有的一级分类
        List<CategoryEntity> categoryEntities = categoryService.getLevel1Categorys();
        model.addAttribute("categorys", categoryEntities);
        return "index";
    }

    @ResponseBody
    @GetMapping("/index/catalog.json")
    public Map<String, List<Catelog2Vo>> getCatalogJson() {

        return categoryService.getCatalogJson();
    }

    @ResponseBody
    @GetMapping("/hello")
    public String hello() {
        //1、获取一把锁，只要锁的名字一样，就是同一把锁
        RLock lock = redisson.getLock("my-lock");
        //2、加锁
//        lock.lock();    //阻塞式等待，默认加的锁都是30s时间
        //1）、锁的自动续期，如果业务超长，运行期间自动给锁续上新的30s。不用担心业务时间长锁自动过期被删掉
        //2）、加锁的业务只要运行完成，就不会给当前锁续期，即使不手动解锁，锁默认在30s以后自动删除

        lock.lock(10, TimeUnit.SECONDS);    //10秒钟自动解锁，自动解锁时间一定要大于业务的执行时间
        //问题：在锁时间到了以后，锁不会自动续期
        //1、如果我们传递了超时时间，就发送Lua脚本给redis，进行占锁，默认超时时间就是我们指定的时间
        //2、如果我们未指定超时时间，就使用30 * 1000【LockWatchdogTimeout看门狗的默认时间】
        //      只要占锁成功，就会启动一个定时任务【重新给锁设置一个过期时间，新的过期时间就是看门狗的默认时间】，每隔10s就会自动续期，续期为看门狗时间
        //      internalLockLeaseTime【看门狗时间】 / 3，10s

        //最佳实战
        //1）、lock.lock(10, TimeUnit.SECONDS);   省掉了整个续期操作。手动解锁
        try {
            System.out.println("加锁成功，执行业务" + Thread.currentThread().getId());
            Thread.sleep(30000);
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            //3、解锁   假设解锁代码没有运行，redisson会不会出现死锁
            lock.unlock();
        }

        return "Hello World!";
    }

}

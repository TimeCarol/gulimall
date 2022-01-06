package com.atguigu.gulimall.product.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.atguigu.gulimall.product.service.CategoryBrandRelationService;
import com.atguigu.gulimall.product.vo.Catelog2Vo;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.atguigu.common.utils.PageUtils;
import com.atguigu.common.utils.Query;

import com.atguigu.gulimall.product.dao.CategoryDao;
import com.atguigu.gulimall.product.entity.CategoryEntity;
import com.atguigu.gulimall.product.service.CategoryService;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;


@Service("categoryService")
@Slf4j
public class CategoryServiceImpl extends ServiceImpl<CategoryDao, CategoryEntity> implements CategoryService {

//    @Autowired
//    CategoryDao categoryDao;

    @Autowired
    private StringRedisTemplate redis;

    @Autowired
    private CategoryBrandRelationService categoryBrandRelationService;

    @Autowired
    private RedissonClient redisson;
    @Override
    public PageUtils queryPage(Map<String, Object> params) {
        IPage<CategoryEntity> page = this.page(
                new Query<CategoryEntity>().getPage(params),
                new QueryWrapper<CategoryEntity>()
        );

        return new PageUtils(page);
    }

    @Override
    public List<CategoryEntity> listWithTree() {
        //1、查出所有分类
        List<CategoryEntity> entities = baseMapper.selectList(null);

        //2、组装成父子的树形结构

        //2.1）、找到所有的一级分类
        List<CategoryEntity> level1Menus = entities.stream().filter(categoryEntity ->
             categoryEntity.getParentCid() == 0
        ).map((menu)->{
            menu.setChildren(getChildrens(menu,entities));
            return menu;
        }).sorted((menu1,menu2)->{
            return (menu1.getSort()==null?0:menu1.getSort()) - (menu2.getSort()==null?0:menu2.getSort());
        }).collect(Collectors.toList());




        return level1Menus;
    }

    @Override
    public void removeMenuByIds(List<Long> asList) {
        //TODO  1、检查当前删除的菜单，是否被别的地方引用

        //逻辑删除
        baseMapper.deleteBatchIds(asList);
    }

    //[2,25,225]
    @Override
    public Long[] findCatelogPath(Long catelogId) {
        List<Long> paths = new ArrayList<>();
        List<Long> parentPath = findParentPath(catelogId, paths);

        Collections.reverse(parentPath);


        return parentPath.toArray(new Long[parentPath.size()]);
    }

    /**
     * 级联更新所有关联的数据
     * @param category
     */
    @Transactional
    @Override
    public void updateCascade(CategoryEntity category) {
        this.updateById(category);
        categoryBrandRelationService.updateCategory(category.getCatId(),category.getName());
    }

    //每一个需要缓存的数据我们都来指定要放到哪个名字的缓存。【缓存的分区（按照业务的类型划分）】
    //代表当前方法的结果需要缓存，如果缓存中有，方法不用调用。如果缓存中没有，会调用方法，最后将方法的结果放入缓存

    /*
     * 默认行为
     *      如果缓存中有，方法不用调用
     *      key默认自动生成，缓存的名字::SimpleKey []（自动生成的key值）
     *      缓存的value的值，默认使用jdk序列化机制，将序列化后的数据存到redis
     * 自定义：
     *      指定生成的缓存使用的key，key属性指定
     *      指定缓存存活的时间，配置文件中指定ttl
     *      将数据保存为JSON格式
     */
    @Cacheable(value = {"category"}, key = "'level1Categorys'")
    @Override
    public List<CategoryEntity> getLevel1Categorys() {
        return baseMapper.selectList(new QueryWrapper<CategoryEntity>().eq("parent_cid", 0));
    }


    //TODO 产生堆外内存溢出：OutOfDirectMemoryError
    //1）、SpringBoot2.0以后默认使用lettuce作为操作redis的客户端，它使用netty进行网络通信。
    //2）、Lettuce的bug导致netty堆外内存溢出  -Xmx300m，netty如果没有指定堆外内存，默认使用 -Xmx300m
    //3）、可以通过-Dio.netty.maxDirectMemory   进行设置
    //解决方案：不能使用-Dio.netty.maxDirectMemory只去调大堆外内存。
    //1)、升级Lettuce客户端。     2）、切换使用Jedis
    @Override
    public Map<String, List<Catelog2Vo>> getCatalogJson() {
        //给缓存中放JSON字符串，拿出的JSON字符串还要逆转成能用的对象类型  【序列与发序列化】
        /**
         * 1、空结果缓存：解决缓存穿透
         * 2、设置过期时间（加随机值）：解决缓存雪崩
         * 3、加锁：解决缓存击穿
         */
        //1、加入缓存逻辑，缓存中存的数据是JSON字符串
        //JSON跨语言，跨平台兼容
        String catalogJson = redis.opsForValue().get("catalogJson");
        if (StringUtils.isEmpty(catalogJson)) {
            //2、缓存中没有数据，查询数据库
            Map<String, List<Catelog2Vo>> catalogJsonFromDB = getCatalogJsonFromDBWithRedisLock();

            return catalogJsonFromDB;
        }
        return JSON.parseObject(catalogJson, new TypeReference<Map<String, List<Catelog2Vo>>>(){});
    }

    /**
     * 从数据库查询并封装分类数据
     * @return
     */
    private Map<String, List<Catelog2Vo>> getCatalogJsonFromDBWithRedissonLock() {
        //1、锁的名字。锁的粒度，越细越快
        RLock lock = redisson.getLock("catalogJson-lock");
        lock.lock();
        //加锁成功，执行业务
        Map<String, List<Catelog2Vo>> catalogJsonFromDB = null;
        try {
            catalogJsonFromDB = getCatalogJsonFromDB();
        } finally {
            //删除分布式锁，需要是原子操作
            lock.unlock();
        }
        return catalogJsonFromDB;
    }

    /**
     * 从数据库查询并封装分类数据
     * @return
     */
    private Map<String, List<Catelog2Vo>> getCatalogJsonFromDBWithRedisLock() {
        //使用UUID绑定为自己的锁
        UUID uuid = UUID.randomUUID();
        //占分布式锁并设置30秒后过期
        Boolean isLocked = redis.opsForValue().setIfAbsent("lock", uuid.toString(), 30, TimeUnit.SECONDS);
        if (isLocked == null || !isLocked) {
            //加锁失败，重试
//            log.info("获取分布式锁失败，等待重试");
            //休眠200ms后重试
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                log.error("线程休眠出错! {}", e);
            }
            return getCatalogJsonFromDBWithRedisLock(); //自旋的方式
        }
        //加锁成功，执行业务
//        log.info("获取分布式锁成功");
        Map<String, List<Catelog2Vo>> catalogJsonFromDB = getCatalogJsonFromDB();

        String script = "if redis.call('get',KEYS[1]) == ARGV[1]" +
                " then" +
                "     return redis.call('del',KEYS[1])" +
                " else" +
                "     return 0" +
                " end";

        //删除分布式锁，需要是原子操作
//        if (uuid.toString().equals(redis.opsForValue().get("lock"))) redis.delete("lock");  //如果是自己的锁才删除
        Long lock = redis.execute(new DefaultRedisScript<Long>(script, Long.class), Collections.singletonList("lock"), uuid.toString());
        return catalogJsonFromDB;
    }

    private Map<String, List<Catelog2Vo>> getCatalogJsonFromDB() {
        //得到锁之后，先在缓存中确认一次，如果没有才继续执行
        String catalogJson = redis.opsForValue().get("catalogJson");
        if (!StringUtils.isEmpty(catalogJson)) {
            return JSON.parseObject(catalogJson, new TypeReference<Map<String, List<Catelog2Vo>>>(){});
        }

        log.info("查询了数据库，线程名：" + Thread.currentThread().getName() + "，线程ID：" + Thread.currentThread().getId());
        /**
         * 1、将数据库的多次查询变为一次
         */
        List<CategoryEntity> selectList = baseMapper.selectList(null);


        //查出所有分类
        Map<String, List<Catelog2Vo>> catalogJsonFromDB = getParentCidList(selectList, 0L).stream().collect(Collectors.toMap(k -> k.getCatId().toString(), v -> {
            //查询这个一级分类的所有二级分类
            List<CategoryEntity> categoryEntities = getParentCidList(selectList, v.getCatId());
            List<Catelog2Vo> catelog2Vos = new ArrayList<>();
            if (categoryEntities != null) {
                catelog2Vos = categoryEntities.stream().map(l2 -> {
                    //查找当前二级分类的三级分类封装成vo
                    List<CategoryEntity> level3Catelog = getParentCidList(selectList, l2.getCatId());
                    List<Catelog2Vo.Catalog3Vo> catalog3Vos = new ArrayList<>();
                    if (level3Catelog != null) {
                        //封装成指定格式
                        catalog3Vos = level3Catelog.stream().map(l3 -> {
                            return new Catelog2Vo.Catalog3Vo(l2.getCatId().toString(), l3.getCatId().toString(), l3.getName());
                        }).collect(Collectors.toList());
                    }
                    return new Catelog2Vo(v.getCatId().toString(), catalog3Vos, l2.getCatId().toString(), l2.getName());
                }).collect(Collectors.toList());
            }

            return catelog2Vos;
        }));
        //3、将查到的数据放入缓存，将对象转为JSON放在内存中
        catalogJson = JSON.toJSONString(catalogJsonFromDB);
        redis.opsForValue().set("catalogJson", catalogJson, 1, TimeUnit.DAYS);
        return catalogJsonFromDB;
    }

    /**
     * 从数据库查询并封装分类数据
     * @return
     */
    private Map<String, List<Catelog2Vo>> getCatalogJsonFromDBWithLocalLock() {
        //SpringBoot中所有组件都是单例的
        //TODO 本地锁：synchronized，JUC（Lock），在分布式情况下想要使用所有必须使用分布式锁
        synchronized (CategoryServiceImpl.class) {
            //得到锁之后，先在缓存中确认一次，如果没有才继续执行
            String catalogJson = redis.opsForValue().get("catalogJson");
            if (!StringUtils.isEmpty(catalogJson)) return JSON.parseObject(catalogJson, new TypeReference<Map<String, List<Catelog2Vo>>>(){});

            log.info("查询了数据库，线程名：" + Thread.currentThread().getName() + "，线程ID：" + Thread.currentThread().getId());
            /**
             * 1、将数据库的多次查询变为一次
             */
            List<CategoryEntity> selectList = baseMapper.selectList(null);


            //查出所有分类
            Map<String, List<Catelog2Vo>> catalogJsonFromDB = getParentCidList(selectList, 0L).stream().collect(Collectors.toMap(k -> k.getCatId().toString(), v -> {
                //查询这个一级分类的所有二级分类
                List<CategoryEntity> categoryEntities = getParentCidList(selectList, v.getCatId());
                List<Catelog2Vo> catelog2Vos = new ArrayList<>();
                if (categoryEntities != null) {
                    catelog2Vos = categoryEntities.stream().map(l2 -> {
                        //查找当前二级分类的三级分类封装成vo
                        List<CategoryEntity> level3Catelog = getParentCidList(selectList, l2.getCatId());
                        List<Catelog2Vo.Catalog3Vo> catalog3Vos = new ArrayList<>();
                        if (level3Catelog != null) {
                            //封装成指定格式
                            catalog3Vos = level3Catelog.stream().map(l3 -> {
                                return new Catelog2Vo.Catalog3Vo(l2.getCatId().toString(), l3.getCatId().toString(), l3.getName());
                            }).collect(Collectors.toList());
                        }
                        return new Catelog2Vo(v.getCatId().toString(), catalog3Vos, l2.getCatId().toString(), l2.getName());
                    }).collect(Collectors.toList());
                }

                return catelog2Vos;
            }));
            //3、将查到的数据放入缓存，将对象转为JSON放在内存中
            catalogJson = JSON.toJSONString(catalogJsonFromDB);
            redis.opsForValue().set("catalogJson", catalogJson, 1, TimeUnit.DAYS);
            return catalogJsonFromDB;
        }
    }

    /**
     * 从集合中挑出指定的parentCid
     * @param categoryEntities
     * @param parentCid
     * @return
     */
    private List<CategoryEntity> getParentCidList(List<CategoryEntity> categoryEntities, Long parentCid) {
        return categoryEntities.stream().filter(item -> parentCid == item.getParentCid()).collect(Collectors.toList());
    }

    //225,25,2
    private List<Long> findParentPath(Long catelogId,List<Long> paths){
        //1、收集当前节点id
        paths.add(catelogId);
        CategoryEntity byId = this.getById(catelogId);
        if(byId.getParentCid()!=0){
            findParentPath(byId.getParentCid(),paths);
        }
        return paths;

    }


    //递归查找所有菜单的子菜单
    private List<CategoryEntity> getChildrens(CategoryEntity root,List<CategoryEntity> all){

        List<CategoryEntity> children = all.stream().filter(categoryEntity -> {
            return categoryEntity.getParentCid() == root.getCatId();
        }).map(categoryEntity -> {
            //1、找到子菜单
            categoryEntity.setChildren(getChildrens(categoryEntity,all));
            return categoryEntity;
        }).sorted((menu1,menu2)->{
            //2、菜单的排序
            return (menu1.getSort()==null?0:menu1.getSort()) - (menu2.getSort()==null?0:menu2.getSort());
        }).collect(Collectors.toList());

        return children;
    }



}
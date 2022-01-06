package com.atguigu.gulimall.product.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Catelog2Vo {
    /* 一级父分类id */
    private String catale1Id;
    /* 三级子分类 */
    private List<Catalog3Vo> catalog3List;
    private String id;
    private String name;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    /* 三级分类VO */
    public static class Catalog3Vo {
        /* 二级父分类id */
        private String catalog2Id;
        private String id;
        private String name;
    }
}

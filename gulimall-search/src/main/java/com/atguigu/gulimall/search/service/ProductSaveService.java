package com.atguigu.gulimall.search.service;

import com.atguigu.common.to.SkuEsModel;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

public interface ProductSaveService {
    Boolean productStatusUp(List<SkuEsModel> skuEsModels) throws IOException;
}

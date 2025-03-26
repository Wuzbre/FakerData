package com.bigdata.api.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.bigdata.api.entity.SysPermission;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface SysPermissionMapper extends BaseMapper<SysPermission> {
    
    /**
     * 查询用户权限列表
     *
     * @param userId 用户ID
     * @return 权限列表
     */
    List<SysPermission> selectUserPermissions(@Param("userId") Long userId);
} 
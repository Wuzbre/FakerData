package com.bigdata.api.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.bigdata.api.entity.SysRole;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface SysRoleMapper extends BaseMapper<SysRole> {
    
    /**
     * 查询用户角色列表
     *
     * @param userId 用户ID
     * @return 角色列表
     */
    List<SysRole> selectUserRoles(@Param("userId") Long userId);
}
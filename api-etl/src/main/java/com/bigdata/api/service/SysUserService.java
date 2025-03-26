package com.bigdata.api.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.bigdata.api.entity.SysUser;
import com.bigdata.api.dto.LoginRequest;
import com.bigdata.api.dto.LoginResponse;

public interface SysUserService extends IService<SysUser> {
    
    /**
     * 用户登录
     *
     * @param loginRequest 登录请求
     * @return 登录响应
     */
    LoginResponse login(LoginRequest loginRequest);
    
    /**
     * 根据用户名查询用户
     *
     * @param username 用户名
     * @return 用户信息
     */
    SysUser getByUsername(String username);
    
    /**
     * 获取用户角色编码列表
     *
     * @param userId 用户ID
     * @return 角色编码列表
     */
    String[] getUserRoles(Long userId);
    
    /**
     * 获取用户权限编码列表
     *
     * @param userId 用户ID
     * @return 权限编码列表
     */
    String[] getUserPermissions(Long userId);
} 
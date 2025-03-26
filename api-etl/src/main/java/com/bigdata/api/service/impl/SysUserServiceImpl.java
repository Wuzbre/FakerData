package com.bigdata.api.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.bigdata.api.entity.SysUser;
import com.bigdata.api.entity.SysRole;
import com.bigdata.api.entity.SysPermission;
import com.bigdata.api.mapper.SysUserMapper;
import com.bigdata.api.mapper.SysRoleMapper;
import com.bigdata.api.mapper.SysPermissionMapper;
import com.bigdata.api.service.SysUserService;
import com.bigdata.api.dto.LoginRequest;
import com.bigdata.api.dto.LoginResponse;
import com.bigdata.api.util.JwtUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class SysUserServiceImpl extends ServiceImpl<SysUserMapper, SysUser> implements SysUserService {

    @Autowired
    private AuthenticationManager authenticationManager;
    
    @Autowired
    private JwtUtil jwtUtil;
    
    @Autowired
    private SysRoleMapper roleMapper;
    
    @Autowired
    private SysPermissionMapper permissionMapper;

    @Override
    public LoginResponse login(LoginRequest loginRequest) {
        // 进行身份验证
        Authentication authentication = authenticationManager.authenticate(
            new UsernamePasswordAuthenticationToken(loginRequest.getUsername(), loginRequest.getPassword())
        );
        
        SecurityContextHolder.getContext().setAuthentication(authentication);
        
        // 生成JWT token
        UserDetails userDetails = (UserDetails) authentication.getPrincipal();
        String token = jwtUtil.generateToken(userDetails);
        
        // 获取用户信息
        SysUser user = getByUsername(loginRequest.getUsername());
        
        // 获取用户角色和权限
        String[] roles = getUserRoles(user.getId());
        String[] permissions = getUserPermissions(user.getId());
        
        // 构建登录响应
        return LoginResponse.builder()
                .userId(user.getId())
                .username(user.getUsername())
                .nickname(user.getNickname())
                .token(token)
                .roles(roles)
                .permissions(permissions)
                .build();
    }

    @Override
    public SysUser getByUsername(String username) {
        return getOne(new LambdaQueryWrapper<SysUser>()
                .eq(SysUser::getUsername, username)
                .eq(SysUser::getStatus, 1)
                .eq(SysUser::getDeleted, 0));
    }

    @Override
    public String[] getUserRoles(Long userId) {
        List<SysRole> roles = roleMapper.selectUserRoles(userId);
        return roles.stream()
                .map(SysRole::getRoleCode)
                .toArray(String[]::new);
    }

    @Override
    public String[] getUserPermissions(Long userId) {
        List<SysPermission> permissions = permissionMapper.selectUserPermissions(userId);
        return permissions.stream()
                .map(SysPermission::getPermissionCode)
                .toArray(String[]::new);
    }
} 
package com.bigdata.api.security;

import com.bigdata.api.entity.SysUser;
import com.bigdata.api.service.SysUserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class UserDetailsServiceImpl implements UserDetailsService {

    @Autowired
    private SysUserService userService;

    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        // 查询用户信息
        SysUser user = userService.getByUsername(username);
        if (user == null) {
            throw new UsernameNotFoundException("用户不存在");
        }

        // 查询用户角色和权限
        String[] roles = userService.getUserRoles(user.getId());
        String[] permissions = userService.getUserPermissions(user.getId());

        // 构建权限列表
        List<SimpleGrantedAuthority> authorities = Arrays.stream(roles)
                .map(role -> new SimpleGrantedAuthority("ROLE_" + role))
                .collect(Collectors.toList());
        
        authorities.addAll(Arrays.stream(permissions)
                .map(SimpleGrantedAuthority::new)
                .collect(Collectors.toList()));

        return new User(user.getUsername(), user.getPassword(), authorities);
    }
} 
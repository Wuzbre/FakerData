package com.bigdata.api.dto;

import lombok.Data;
import lombok.Builder;

@Data
@Builder
public class LoginResponse {
    
    private Long userId;
    private String username;
    private String nickname;
    private String token;
    private String[] roles;
    private String[] permissions;
} 
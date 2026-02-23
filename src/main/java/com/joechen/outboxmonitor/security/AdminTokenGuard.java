package com.joechen.outboxmonitor.security;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class AdminTokenGuard {

    private final String adminToken;

    public AdminTokenGuard(@Value("${app.admin.token:}") String adminToken) {
        this.adminToken = adminToken;
    }

    public boolean isAuthorized(String providedToken) {
        if (adminToken == null || adminToken.isBlank()) {
            return false;
        }
        return adminToken.equals(providedToken);
    }
}

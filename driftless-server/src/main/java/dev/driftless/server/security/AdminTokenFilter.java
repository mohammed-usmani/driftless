package dev.driftless.server.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import org.springframework.web.filter.OncePerRequestFilter;

/** Every API call needs {@code Authorization: Bearer <admin token>}. With no token configured, nothing gets in. */
public class AdminTokenFilter extends OncePerRequestFilter {

    private final byte[] expected;

    public AdminTokenFilter(String adminToken) {
        this.expected = adminToken == null || adminToken.isBlank() ? null : ("Bearer " + adminToken).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain) throws ServletException, IOException {
        String header = req.getHeader("Authorization");
        if (expected != null && header != null && MessageDigest.isEqual(expected, header.getBytes(StandardCharsets.UTF_8))) {
            chain.doFilter(req, res);
            return;
        }
        res.setStatus(401);
        res.setHeader("WWW-Authenticate", "Bearer");
        res.setContentType("application/problem+json");
        res.getWriter().write("""
                {"type":"about:blank","title":"Unauthorized","status":401,"detail":"missing or invalid bearer token"}""");
    }
}

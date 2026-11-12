package dev.driftless.server.security;

import dev.driftless.server.config.DriftlessProperties;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** One shared admin token, no users: a servlet filter is all the security this needs. */
@Configuration(proxyBeanMethods = false)
class SecurityConfig {

    @Bean
    FilterRegistrationBean<AdminTokenFilter> adminTokenFilter(DriftlessProperties props) {
        FilterRegistrationBean<AdminTokenFilter> r = new FilterRegistrationBean<>(new AdminTokenFilter(props.adminToken()));
        r.addUrlPatterns("/v1/*");
        return r;
    }
}

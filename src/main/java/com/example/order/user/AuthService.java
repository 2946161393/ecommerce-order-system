package com.example.order.user;

import com.example.order.security.JwtTokenProvider;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.http.HttpStatus;

@Service
public class AuthService {

    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    private final AuthenticationManager authenticationManager;
    private final JwtTokenProvider tokenProvider;

    public AuthService(UserRepository userRepository,
                       PasswordEncoder passwordEncoder,
                       AuthenticationManager authenticationManager,
                       JwtTokenProvider tokenProvider) {
        this.userRepository = userRepository;
        this.passwordEncoder = passwordEncoder;
        this.authenticationManager = authenticationManager;
        this.tokenProvider = tokenProvider;
    }

    public void register(String username, String rawPassword) {
        if (userRepository.existsByUsername(username)) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Username already taken");
        }
        // store only the BCrypt hash
        String hash = passwordEncoder.encode(rawPassword);
        userRepository.save(new UserEntity(username, hash, "ROLE_USER"));
    }

    public String login(String username, String rawPassword) {
        // delegates to AuthenticationManager -> AppUserDetailsService + PasswordEncoder.
        // throws if credentials are bad.
        authenticationManager.authenticate(
                new UsernamePasswordAuthenticationToken(username, rawPassword));
        return tokenProvider.generateToken(username);
    }
}

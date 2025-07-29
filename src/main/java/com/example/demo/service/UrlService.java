package com.example.demo.service;

import com.example.demo.dto.UrlRequestDTO;
import com.example.demo.entity.UrlMapping;
import com.example.demo.repository.UrlRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
public class UrlService implements ICUrlService {
    @Autowired
    private UrlRepository urlRepository;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    private final String URL_CACHE = "shortUrl:";
    private final String HIT_CACHE = "hits:";

    @Override
    public String createShortUrl(UrlRequestDTO dto) {
        UrlMapping mapping = new UrlMapping();
        mapping.setLongUrl(dto.getLongUrl());

        mapping = urlRepository.save(mapping);

        String shortCode = encode(mapping.getId());
        mapping.setShortCode(shortCode);

        return urlRepository.save(mapping).getShortCode();
    }

    @Override
    public String getLongUrl(String shortCode) {

        String cacheKey = URL_CACHE + shortCode;

        String longUrl = redisTemplate.opsForValue().get(cacheKey);

        if(longUrl != null)
        {
            System.out.println("Redis Call");
            return longUrl;
        }

        UrlMapping urlMapping = urlRepository.findByShortCode(shortCode).orElseThrow(() -> new RuntimeException("URL not Found"));

        longUrl = urlMapping.getLongUrl();

        redisTemplate.opsForValue().set(cacheKey, longUrl, Duration.ofHours(24));

        return longUrl;

    }

    String encode(Long id)
    {
        String chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        StringBuilder builder = new StringBuilder();

        while(id > 0)
        {
            builder.append(chars.charAt((int) (id % 62)));
            id /= 62;
        }

        return builder.reverse().toString();
    }
}

package com.example.demo.service;

import com.example.demo.dto.UrlRequestDTO;

public interface ICUrlService {

    public String createShortUrl(UrlRequestDTO dto);

    public String getLongUrl(String shortCode);
}

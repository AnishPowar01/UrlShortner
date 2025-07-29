package com.example.demo.controller;

import com.example.demo.dto.UrlRequestDTO;
import com.example.demo.service.ICUrlService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;

@RequestMapping("/api/v1")
@RestController
public class UrlController {

    private ICUrlService icUrlService;

    public UrlController(ICUrlService icUrlService)
    {
        this.icUrlService = icUrlService;
    }

    @PostMapping("/shorten")
    public ResponseEntity<String> createShortUrl(@RequestBody UrlRequestDTO dto)
    {
        String shortCode = icUrlService.createShortUrl(dto);

        return ResponseEntity.ok(shortCode);
    }

    @GetMapping("/{shortCode}")
    public ResponseEntity<?> redirect(@PathVariable String shortCode)
    {
        String longUrl = icUrlService.getLongUrl(shortCode);
        return ResponseEntity.status(HttpStatus.FOUND).
                location(URI.create(longUrl)).build();
    }
}

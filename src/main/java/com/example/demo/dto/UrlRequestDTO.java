package com.example.demo.dto;

import jakarta.persistence.Entity;
import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@Builder
@AllArgsConstructor
public class UrlRequestDTO {

    public String longUrl;
}

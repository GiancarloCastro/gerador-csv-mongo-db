package com.mongodb.model;


import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ProdutoComImagemInvalidaDto {
    private Long sku;

    private Long skuExterno;

    private String nome;

    private String departamento;

    private Integer quantidadePadrao;

    private Integer quantidadeCD;

    private String emLinhaForaDeLinha;

    private String motivo;
}

package com.cj.hive.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author CJ
 * @date: 2021/11/2 13:46
 */
@Data
@AllArgsConstructor
public class Tuple<A, B> {
    public final A first;
    public final B second;
}
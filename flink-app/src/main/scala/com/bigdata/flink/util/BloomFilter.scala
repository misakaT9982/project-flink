package com.bigdata.flink.util

/**
 * Ciel
 * project-flink: com.bigdata.flink.util
 * 2020-07-02 23:03:29
 */
object BloomFilter {
    // 位图容量
    val cap: Long = 1 << 29

    def offset(s: String, seed: Int): Long = {
        var hashResult = 0L
        for(c <- s){
            hashResult = hashResult * seed + c
        }
        hashResult & (cap - 1)
    }

}

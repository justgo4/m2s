#ifndef J4_NATIVE_H
#define J4_NATIVE_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#if defined(_WIN32)
#define J4_NATIVE_API __declspec(dllexport)
#elif defined(__GNUC__) || defined(__clang__)
#define J4_NATIVE_API __attribute__((visibility("default")))
#else
#define J4_NATIVE_API
#endif

#define J4_NATIVE_ABI_VERSION 3u
#define J4_NATIVE_FEATURE_STABLE_PARTITION (1ULL << 0)
#define J4_NATIVE_FEATURE_NO_LIBC (1ULL << 1)
#define J4_NATIVE_FEATURE_JSON_ENCODER (1ULL << 2)

#define J4_JSON_I8 1u
#define J4_JSON_U8 2u
#define J4_JSON_I16 3u
#define J4_JSON_U16 4u
#define J4_JSON_I32 5u
#define J4_JSON_U32 6u
#define J4_JSON_I64 7u
#define J4_JSON_U64 8u
#define J4_JSON_STRING 11u
#define J4_JSON_BINARY 12u

#define J4_JSON_FLAG_LARGE_OFFSETS (1u << 0)
#define J4_JSON_FLAG_BASE64 (1u << 1)

typedef struct j4_json_column {
    const uint8_t* validity;
    const void* values;
    const uint8_t* data;
    const uint8_t* key;
    uint64_t offset;
    uint32_t key_len;
    uint32_t kind;
    uint32_t flags;
    uint32_t reserved;
} j4_json_column;

J4_NATIVE_API uint32_t j4_native_abi_version(void);
J4_NATIVE_API uint64_t j4_native_feature_bits(void);

J4_NATIVE_API int j4_stable_partition_u16(
    const uint16_t* lanes,
    uint64_t nrows,
    uint32_t partitions,
    uint64_t* order,
    uint64_t* counts,
    uint64_t* cursor);

J4_NATIVE_API int j4_json_measure(
    const j4_json_column* columns,
    uint32_t ncolumns,
    uint64_t nrows,
    uint32_t include_sequence,
    uint64_t sequence,
    uint64_t* out_bytes);

J4_NATIVE_API int j4_json_encode(
    const j4_json_column* columns,
    uint32_t ncolumns,
    uint64_t nrows,
    uint32_t include_sequence,
    uint64_t sequence,
    uint64_t* offsets,
    uint8_t* output,
    uint64_t capacity,
    uint64_t* used_bytes);

#ifdef __cplusplus
}
#endif

#endif

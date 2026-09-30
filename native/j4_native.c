#include <stdint.h>
#include "j4_native.h"

_Static_assert(sizeof(uint8_t) == 1, "j4 native ABI requires 8-bit uint8_t");
_Static_assert(sizeof(uint16_t) == 2, "j4 native ABI requires 16-bit uint16_t");
_Static_assert(sizeof(uint32_t) == 4, "j4 native ABI requires 32-bit uint32_t");
_Static_assert(sizeof(uint64_t) == 8, "j4 native ABI requires 64-bit uint64_t");
_Static_assert(sizeof(j4_json_column) == 56, "unexpected j4_json_column ABI layout");

uint32_t j4_native_abi_version(void) {
    return J4_NATIVE_ABI_VERSION;
}

uint64_t j4_native_feature_bits(void) {
    return J4_NATIVE_FEATURE_STABLE_PARTITION |
           J4_NATIVE_FEATURE_NO_LIBC |
           J4_NATIVE_FEATURE_JSON_ENCODER;
}

int j4_stable_partition_u16(
        const uint16_t* lanes,
        uint64_t nrows,
        uint32_t partitions,
        uint64_t* order,
        uint64_t* counts,
        uint64_t* cursor) {
    if (!lanes || !order || !counts || !cursor ||
            partitions == 0 || partitions > 65536) {
        return 1;
    }
    for (uint32_t lane = 0; lane < partitions; lane++) counts[lane] = 0;
    for (uint64_t i = 0; i < nrows; i++) {
        uint32_t lane = lanes[i];
        if (lane >= partitions) return 2;
        counts[lane]++;
    }
    uint64_t offset = 0;
    for (uint32_t lane = 0; lane < partitions; lane++) {
        cursor[lane] = offset;
        offset += counts[lane];
    }
    for (uint64_t i = 0; i < nrows; i++) {
        uint32_t lane = lanes[i];
        order[cursor[lane]++] = i;
    }
    return 0;
}

static int j4_valid(const j4_json_column* column, uint64_t row) {
    uint64_t index = column->offset + row;
    if (!column->validity) return 1;
    return (column->validity[index >> 3] >> (index & 7)) & 1u;
}

static uint64_t j4_u64_digits(uint64_t value) {
    uint64_t n = 1;
    while (value >= 10) { value /= 10; n++; }
    return n;
}

static uint64_t j4_i64_digits(int64_t value) {
    if (value >= 0) return j4_u64_digits((uint64_t)value);
    return 1 + j4_u64_digits((uint64_t)(-(value + 1)) + 1u);
}

static uint8_t* j4_write_u64(uint8_t* out, uint64_t value) {
    uint8_t tmp[20];
    uint32_t n = 0;
    do { tmp[n++] = (uint8_t)('0' + (value % 10)); value /= 10; } while (value);
    while (n) *out++ = tmp[--n];
    return out;
}

static uint8_t* j4_write_i64(uint8_t* out, int64_t value) {
    uint64_t magnitude;
    if (value < 0) {
        *out++ = '-';
        magnitude = (uint64_t)(-(value + 1)) + 1u;
    } else {
        magnitude = (uint64_t)value;
    }
    return j4_write_u64(out, magnitude);
}

static uint64_t j4_escape_measure(const uint8_t* data, uint64_t length) {
    uint64_t out = 0;
    for (uint64_t i = 0; i < length; i++) {
        uint8_t c = data[i];
        if (c == '"' || c == '\\' || c == '\b' || c == '\f' ||
                c == '\n' || c == '\r' || c == '\t') out += 2;
        else if (c < 0x20) out += 6;
        else out += 1;
    }
    return out;
}

static uint8_t* j4_escape_write(uint8_t* out, const uint8_t* data, uint64_t length) {
    static const uint8_t hex[] = "0123456789abcdef";
    for (uint64_t i = 0; i < length; i++) {
        uint8_t c = data[i];
        if (c == '"') { *out++='\\'; *out++='"'; }
        else if (c == '\\') { *out++='\\'; *out++='\\'; }
        else if (c == '\b') { *out++='\\'; *out++='b'; }
        else if (c == '\f') { *out++='\\'; *out++='f'; }
        else if (c == '\n') { *out++='\\'; *out++='n'; }
        else if (c == '\r') { *out++='\\'; *out++='r'; }
        else if (c == '\t') { *out++='\\'; *out++='t'; }
        else if (c < 0x20) {
            *out++='\\'; *out++='u'; *out++='0'; *out++='0';
            *out++=hex[c >> 4]; *out++=hex[c & 15];
        } else *out++=c;
    }
    return out;
}

static void j4_span(const j4_json_column* column, uint64_t row,
                    const uint8_t** data, uint64_t* length) {
    uint64_t index = column->offset + row;
    uint64_t start, stop;
    if (column->flags & J4_JSON_FLAG_LARGE_OFFSETS) {
        const int64_t* offsets = (const int64_t*)column->values;
        start = (uint64_t)offsets[index];
        stop = (uint64_t)offsets[index + 1];
    } else {
        const int32_t* offsets = (const int32_t*)column->values;
        start = (uint64_t)(uint32_t)offsets[index];
        stop = (uint64_t)(uint32_t)offsets[index + 1];
    }
    *data = column->data ? column->data + start : column->data;
    *length = stop - start;
}

static uint64_t j4_base64_length(uint64_t length) {
    return ((length + 2u) / 3u) * 4u;
}

static uint8_t* j4_base64_write(uint8_t* out, const uint8_t* data, uint64_t length) {
    static const uint8_t alphabet[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    uint64_t i = 0;
    while (i + 3 <= length) {
        uint32_t v = ((uint32_t)data[i] << 16) |
                     ((uint32_t)data[i+1] << 8) | data[i+2];
        *out++=alphabet[(v >> 18) & 63];
        *out++=alphabet[(v >> 12) & 63];
        *out++=alphabet[(v >> 6) & 63];
        *out++=alphabet[v & 63];
        i += 3;
    }
    if (i < length) {
        uint32_t v = (uint32_t)data[i] << 16;
        *out++=alphabet[(v >> 18) & 63];
        if (i + 1 < length) {
            v |= (uint32_t)data[i+1] << 8;
            *out++=alphabet[(v >> 12) & 63];
            *out++=alphabet[(v >> 6) & 63];
            *out++='=';
        } else {
            *out++=alphabet[(v >> 12) & 63];
            *out++='='; *out++='=';
        }
    }
    return out;
}

static uint64_t j4_column_index(const j4_json_column* column, uint64_t row) {
    return column->offset + row;
}

static int j4_value_measure(const j4_json_column* column, uint64_t row, uint64_t* bytes) {
    if (!j4_valid(column,row)) { *bytes = 4; return 0; }
    uint64_t index = j4_column_index(column,row);
    switch (column->kind) {
        case J4_JSON_I8: *bytes=j4_i64_digits(((const int8_t*)column->values)[index]); return 0;
        case J4_JSON_U8: *bytes=j4_u64_digits(((const uint8_t*)column->values)[index]); return 0;
        case J4_JSON_I16: *bytes=j4_i64_digits(((const int16_t*)column->values)[index]); return 0;
        case J4_JSON_U16: *bytes=j4_u64_digits(((const uint16_t*)column->values)[index]); return 0;
        case J4_JSON_I32: *bytes=j4_i64_digits(((const int32_t*)column->values)[index]); return 0;
        case J4_JSON_U32: *bytes=j4_u64_digits(((const uint32_t*)column->values)[index]); return 0;
        case J4_JSON_I64: *bytes=j4_i64_digits(((const int64_t*)column->values)[index]); return 0;
        case J4_JSON_U64: *bytes=j4_u64_digits(((const uint64_t*)column->values)[index]); return 0;
        case J4_JSON_STRING: {
            const uint8_t* data; uint64_t length; j4_span(column,row,&data,&length);
            *bytes = 2 + j4_escape_measure(data,length); return 0;
        }
        case J4_JSON_BINARY: {
            if (!(column->flags & J4_JSON_FLAG_BASE64)) return 4;
            const uint8_t* data; uint64_t length; j4_span(column,row,&data,&length);
            *bytes = 2 + j4_base64_length(length); return 0;
        }
        default: return 3;
    }
}

static int j4_value_write(const j4_json_column* column, uint64_t row, uint8_t** outp) {
    uint8_t* out = *outp;
    if (!j4_valid(column,row)) {
        *out++='n'; *out++='u'; *out++='l'; *out++='l'; *outp=out; return 0;
    }
    uint64_t index = j4_column_index(column,row);
    switch (column->kind) {
        case J4_JSON_I8: out=j4_write_i64(out,((const int8_t*)column->values)[index]); break;
        case J4_JSON_U8: out=j4_write_u64(out,((const uint8_t*)column->values)[index]); break;
        case J4_JSON_I16: out=j4_write_i64(out,((const int16_t*)column->values)[index]); break;
        case J4_JSON_U16: out=j4_write_u64(out,((const uint16_t*)column->values)[index]); break;
        case J4_JSON_I32: out=j4_write_i64(out,((const int32_t*)column->values)[index]); break;
        case J4_JSON_U32: out=j4_write_u64(out,((const uint32_t*)column->values)[index]); break;
        case J4_JSON_I64: out=j4_write_i64(out,((const int64_t*)column->values)[index]); break;
        case J4_JSON_U64: out=j4_write_u64(out,((const uint64_t*)column->values)[index]); break;
        case J4_JSON_STRING: {
            const uint8_t* data; uint64_t length; j4_span(column,row,&data,&length);
            *out++='"'; out=j4_escape_write(out,data,length); *out++='"'; break;
        }
        case J4_JSON_BINARY: {
            if (!(column->flags & J4_JSON_FLAG_BASE64)) return 4;
            const uint8_t* data; uint64_t length; j4_span(column,row,&data,&length);
            *out++='"'; out=j4_base64_write(out,data,length); *out++='"'; break;
        }
        default: return 3;
    }
    *outp = out;
    return 0;
}

int j4_json_measure(
        const j4_json_column* columns,
        uint32_t ncolumns,
        uint64_t nrows,
        uint32_t include_sequence,
        uint64_t sequence,
        uint64_t* out_bytes) {
    if (!columns || !out_bytes || ncolumns == 0) return 1;
    uint64_t total = 0;
    for (uint64_t row = 0; row < nrows; row++) {
        uint64_t row_bytes = 2;
        for (uint32_t col = 0; col < ncolumns; col++) {
            uint64_t value_bytes = 0;
            int rc = j4_value_measure(&columns[col],row,&value_bytes);
            if (rc) return rc;
            row_bytes += (col ? 1u : 0u) + columns[col].key_len + 1u + value_bytes;
        }
        if (include_sequence) row_bytes += 12u + j4_u64_digits(sequence);
        row_bytes += 1u;
        if (UINT64_MAX - total < row_bytes) return 5;
        total += row_bytes;
    }
    *out_bytes = total;
    return 0;
}

int j4_json_encode(
        const j4_json_column* columns,
        uint32_t ncolumns,
        uint64_t nrows,
        uint32_t include_sequence,
        uint64_t sequence,
        uint64_t* offsets,
        uint8_t* output,
        uint64_t capacity,
        uint64_t* used_bytes) {
    if (!columns || !offsets || !output || !used_bytes || ncolumns == 0) return 1;
    uint8_t* out = output;
    uint8_t* end = output + capacity;
    offsets[0] = 0;
    for (uint64_t row = 0; row < nrows; row++) {
        if (out >= end) return 6;
        *out++='{';
        for (uint32_t col = 0; col < ncolumns; col++) {
            uint64_t value_bytes = 0;
            int rc = j4_value_measure(&columns[col],row,&value_bytes);
            if (rc) return rc;
            uint64_t needed = (col ? 1u : 0u) + columns[col].key_len + 1u + value_bytes;
            if ((uint64_t)(end-out) < needed) return 6;
            if (col) *out++=',';
            for (uint32_t i=0;i<columns[col].key_len;i++) *out++=columns[col].key[i];
            *out++=':';
            rc = j4_value_write(&columns[col],row,&out);
            if (rc) return rc;
        }
        if (include_sequence) {
            static const uint8_t key[] = ",\"_cdc_seq\":";
            uint64_t needed = 12u + j4_u64_digits(sequence);
            if ((uint64_t)(end-out) < needed + 2u) return 6;
            for (uint32_t i=0;i<12;i++) *out++=key[i];
            out=j4_write_u64(out,sequence);
        }
        if ((uint64_t)(end-out) < 2u) return 6;
        *out++='}'; *out++='\n';
        offsets[row+1]=(uint64_t)(out-output);
    }
    *used_bytes=(uint64_t)(out-output);
    return 0;
}

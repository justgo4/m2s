#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "nanoarrow/nanoarrow.h"
#include "nanoarrow/nanoarrow_ipc.h"

#define MAX_TABLES 128
#define MAX_COLUMNS 256
#define MAX_TABLE_MAPS 256
#define MAX_FRAME_BYTES (256u * 1024u * 1024u)
#define DECODER_IGNORED 10001
#define DECODER_UNKNOWN_MAP 10002

#define FRAME_CONFIG 'C'
#define FRAME_EVENT 'E'
#define FRAME_SNAPSHOT 'S'
#define FRAME_QUIT 'Q'
#define FRAME_ACK 'A'
#define FRAME_BATCH 'B'
#define FRAME_ERROR 'X'

#define MYSQL_TYPE_DECIMAL 0
#define MYSQL_TYPE_TINY 1
#define MYSQL_TYPE_SHORT 2
#define MYSQL_TYPE_LONG 3
#define MYSQL_TYPE_FLOAT 4
#define MYSQL_TYPE_DOUBLE 5
#define MYSQL_TYPE_NULL 6
#define MYSQL_TYPE_TIMESTAMP 7
#define MYSQL_TYPE_LONGLONG 8
#define MYSQL_TYPE_INT24 9
#define MYSQL_TYPE_DATE 10
#define MYSQL_TYPE_TIME 11
#define MYSQL_TYPE_DATETIME 12
#define MYSQL_TYPE_YEAR 13
#define MYSQL_TYPE_NEWDATE 14
#define MYSQL_TYPE_VARCHAR 15
#define MYSQL_TYPE_BIT 16
#define MYSQL_TYPE_TIMESTAMP2 17
#define MYSQL_TYPE_DATETIME2 18
#define MYSQL_TYPE_TIME2 19
#define MYSQL_TYPE_JSON 245
#define MYSQL_TYPE_NEWDECIMAL 246
#define MYSQL_TYPE_ENUM 247
#define MYSQL_TYPE_SET 248
#define MYSQL_TYPE_TINY_BLOB 249
#define MYSQL_TYPE_MEDIUM_BLOB 250
#define MYSQL_TYPE_LONG_BLOB 251
#define MYSQL_TYPE_BLOB 252
#define MYSQL_TYPE_VAR_STRING 253
#define MYSQL_TYPE_STRING 254
#define MYSQL_TYPE_GEOMETRY 255

#define TABLE_MAP_EVENT 19
#define WRITE_ROWS_EVENT_V1 23
#define UPDATE_ROWS_EVENT_V1 24
#define DELETE_ROWS_EVENT_V1 25
#define WRITE_ROWS_EVENT_V2 30
#define UPDATE_ROWS_EVENT_V2 31
#define DELETE_ROWS_EVENT_V2 32
#define PARTIAL_UPDATE_ROWS_EVENT 39

enum ArrowKind {
    AK_I8=1, AK_U8, AK_I16, AK_U16, AK_I32, AK_U32, AK_I64, AK_U64,
    AK_F32, AK_F64, AK_STRING, AK_BINARY, AK_DATE32, AK_TIMESTAMP_US, AK_DECIMAL128
};

struct Cursor {
    const uint8_t* data;
    size_t size;
    size_t pos;
};

struct ColumnConfig {
    char* name;
    uint8_t kind;
    uint8_t is_unsigned;
    uint8_t precision;
    uint8_t scale;
};

struct TableConfig {
    char* db;
    char* table;
    uint16_t ncol;
    struct ColumnConfig* col;
};

struct ColumnMeta {
    uint8_t type;
    uint16_t meta;
    uint16_t max_length;
    uint8_t length_size;
    uint8_t precision;
    uint8_t scale;
    uint8_t fsp;
};

enum TableMapState {
    MAP_UNKNOWN = 0,
    MAP_IGNORED = 1,
    MAP_SUBSCRIBED = 2
};

struct TableMap {
    uint64_t table_id;
    uint8_t state;
    struct TableConfig* config;
    uint16_t ncol;
    struct ColumnMeta* col;
};

struct Decoder {
    uint16_t ntables;
    struct TableConfig tables[MAX_TABLES];
    uint16_t nmaps;
    struct TableMap maps[MAX_TABLE_MAPS];
};

static struct ArrowBufferView bytes_view(const uint8_t* p, int64_t n) {
    struct ArrowBufferView out;
    out.data.as_uint8 = p;
    out.size_bytes = n;
    return out;
}

static struct ArrowStringView string_view(const uint8_t* p, int64_t n) {
    struct ArrowStringView out;
    out.data = (const char*)p;
    out.size_bytes = n;
    return out;
}

static uint16_t u16le(const uint8_t* p) {
    return (uint16_t)p[0] | ((uint16_t)p[1] << 8);
}

static uint32_t u24le(const uint8_t* p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16);
}

static uint32_t u32le(const uint8_t* p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) |
           ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

static uint64_t u64le(const uint8_t* p) {
    return (uint64_t)u32le(p) | ((uint64_t)u32le(p + 4) << 32);
}

static uint64_t uint_le_n(const uint8_t* p, int n) {
    uint64_t v = 0;
    for (int i = 0; i < n; i++) v |= ((uint64_t)p[i]) << (8 * i);
    return v;
}

static uint64_t uint_be_n(const uint8_t* p, int n) {
    uint64_t v = 0;
    for (int i = 0; i < n; i++) v = (v << 8) | p[i];
    return v;
}

static int64_t int_be_n(const uint8_t* p, int n) {
    uint64_t v = uint_be_n(p,n);
    if (n > 0 && n < 8 && (p[0] & 0x80)) v -= 1ULL << (n * 8);
    return (int64_t)v;
}

static int cur_take(struct Cursor* c, size_t n, const uint8_t** out) {
    if (n > c->size - c->pos) return EINVAL;
    *out = c->data + c->pos;
    c->pos += n;
    return 0;
}

static int cur_u8(struct Cursor* c, uint8_t* out) {
    const uint8_t* p;
    if (cur_take(c, 1, &p)) return EINVAL;
    *out = p[0];
    return 0;
}

static int cur_u16(struct Cursor* c, uint16_t* out) {
    const uint8_t* p;
    if (cur_take(c, 2, &p)) return EINVAL;
    *out = u16le(p);
    return 0;
}

static int cur_u32(struct Cursor* c, uint32_t* out) {
    const uint8_t* p;
    if (cur_take(c, 4, &p)) return EINVAL;
    *out = u32le(p);
    return 0;
}

static int cur_u64(struct Cursor* c, uint64_t* out) {
    const uint8_t* p;
    if (cur_take(c, 8, &p)) return EINVAL;
    *out = u64le(p);
    return 0;
}

static int cur_lenenc(struct Cursor* c, uint64_t* out) {
    uint8_t first;
    if (cur_u8(c, &first)) return EINVAL;
    if (first < 0xfb) { *out = first; return 0; }
    const uint8_t* p;
    if (first == 0xfc) {
        if (cur_take(c, 2, &p)) return EINVAL;
        *out = u16le(p); return 0;
    }
    if (first == 0xfd) {
        if (cur_take(c, 3, &p)) return EINVAL;
        *out = u24le(p); return 0;
    }
    if (first == 0xfe) {
        if (cur_take(c, 8, &p)) return EINVAL;
        *out = u64le(p); return 0;
    }
    return EINVAL;
}

static int bit_get(const uint8_t* bitmap, int i) {
    return (bitmap[i >> 3] >> (i & 7)) & 1;
}

static int bit_count(const uint8_t* bitmap, int nbits) {
    int count = 0;
    for (int i = 0; i < nbits; i++) count += bit_get(bitmap, i);
    return count;
}

static int64_t days_from_civil(int y, unsigned m, unsigned d) {
    y -= m <= 2;
    const int era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = (unsigned)(y - era * 400);
    const unsigned mp = m > 2 ? m - 3 : m + 9;
    const unsigned doy = (153 * mp + 2) / 5 + d - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return (int64_t)era * 146097 + (int64_t)doe - 719468;
}

static int64_t timestamp_us(int y, int mon, int day, int hour, int min, int sec, int us) {
    return ((days_from_civil(y, (unsigned)mon, (unsigned)day) * 86400 +
             hour * 3600 + min * 60 + sec) * 1000000) + us;
}

static int read_fsp(struct Cursor* c, uint8_t fsp, int* us) {
    int n = (fsp + 1) / 2;
    if (n == 0) { *us = 0; return 0; }
    const uint8_t* p;
    if (cur_take(c, (size_t)n, &p)) return EINVAL;
    int v = (int)uint_be_n(p, n);
    if (fsp & 1) v /= 10;
    for (int i = fsp; i < 6; i++) v *= 10;
    *us = v;
    return 0;
}

static void free_map(struct TableMap* map) {
    free(map->col);
    memset(map, 0, sizeof(*map));
}

static void decoder_reset(struct Decoder* d) {
    for (uint16_t i = 0; i < d->ntables; i++) {
        free(d->tables[i].db);
        free(d->tables[i].table);
        for (uint16_t j = 0; j < d->tables[i].ncol; j++) free(d->tables[i].col[j].name);
        free(d->tables[i].col);
    }
    for (uint16_t i = 0; i < d->nmaps; i++) free_map(&d->maps[i]);
    memset(d, 0, sizeof(*d));
}

static struct TableConfig* find_config(struct Decoder* d, const char* db, const char* table) {
    for (uint16_t i = 0; i < d->ntables; i++) {
        if (!strcmp(d->tables[i].db, db) && !strcmp(d->tables[i].table, table)) return &d->tables[i];
    }
    return NULL;
}

static struct TableConfig* find_config_view(
        struct Decoder* d, const uint8_t* db, size_t db_len,
        const uint8_t* table, size_t table_len) {
    for (uint16_t i = 0; i < d->ntables; i++) {
        struct TableConfig* cfg = &d->tables[i];
        if (strlen(cfg->db) == db_len && strlen(cfg->table) == table_len &&
            !memcmp(cfg->db, db, db_len) && !memcmp(cfg->table, table, table_len))
            return cfg;
    }
    return NULL;
}

static struct TableMap* find_map(struct Decoder* d, uint64_t table_id) {
    for (uint16_t i = 0; i < d->nmaps; i++) if (d->maps[i].table_id == table_id) return &d->maps[i];
    return NULL;
}

static struct TableMap* put_map(struct Decoder* d, uint64_t table_id) {
    struct TableMap* map = find_map(d, table_id);
    if (map) { free_map(map); map->table_id = table_id; return map; }
    if (d->nmaps >= MAX_TABLE_MAPS) return NULL;
    map = &d->maps[d->nmaps++];
    memset(map, 0, sizeof(*map));
    map->table_id = table_id;
    return map;
}

static enum ArrowType kind_type(uint8_t kind) {
    switch (kind) {
        case AK_I8: return NANOARROW_TYPE_INT8;
        case AK_U8: return NANOARROW_TYPE_UINT8;
        case AK_I16: return NANOARROW_TYPE_INT16;
        case AK_U16: return NANOARROW_TYPE_UINT16;
        case AK_I32: return NANOARROW_TYPE_INT32;
        case AK_U32: return NANOARROW_TYPE_UINT32;
        case AK_I64: return NANOARROW_TYPE_INT64;
        case AK_U64: return NANOARROW_TYPE_UINT64;
        case AK_F32: return NANOARROW_TYPE_FLOAT;
        case AK_F64: return NANOARROW_TYPE_DOUBLE;
        case AK_STRING: return NANOARROW_TYPE_LARGE_STRING;
        case AK_BINARY: return NANOARROW_TYPE_LARGE_BINARY;
        case AK_DATE32: return NANOARROW_TYPE_DATE32;
        case AK_TIMESTAMP_US: return NANOARROW_TYPE_TIMESTAMP;
        case AK_DECIMAL128: return NANOARROW_TYPE_DECIMAL128;
        default: return NANOARROW_TYPE_UNINITIALIZED;
    }
}

static int build_schema(struct TableConfig* cfg, struct ArrowSchema* schema) {
    ArrowSchemaInit(schema);
    int rc = ArrowSchemaSetTypeStruct(schema, cfg->ncol + 2);
    if (rc) return rc;
    for (uint16_t i = 0; i < cfg->ncol; i++) {
        struct ColumnConfig* col = &cfg->col[i];
        if (col->kind == AK_TIMESTAMP_US) {
            rc = ArrowSchemaSetTypeDateTime(schema->children[i], NANOARROW_TYPE_TIMESTAMP,
                                            NANOARROW_TIME_UNIT_MICRO, NULL);
        } else if (col->kind == AK_DECIMAL128) {
            rc = ArrowSchemaSetTypeDecimal(schema->children[i], NANOARROW_TYPE_DECIMAL128,
                                           col->precision, col->scale);
        } else {
            rc = ArrowSchemaSetType(schema->children[i], kind_type(col->kind));
        }
        if (rc) return rc;
        rc = ArrowSchemaSetName(schema->children[i], col->name);
        if (rc) return rc;
    }
    rc = ArrowSchemaSetType(schema->children[cfg->ncol], NANOARROW_TYPE_INT8);
    if (rc) return rc;
    rc = ArrowSchemaSetName(schema->children[cfg->ncol], "_sync_op");
    if (rc) return rc;
    rc = ArrowSchemaSetType(schema->children[cfg->ncol + 1], NANOARROW_TYPE_INT64);
    if (rc) return rc;
    return ArrowSchemaSetName(schema->children[cfg->ncol + 1], "_sync_order");
}


static int parse_uint_text(const uint8_t* p, size_t n, uint64_t* out) {
    if (!n) return EINVAL;
    uint64_t v = 0;
    for (size_t i = 0; i < n; i++) {
        if (p[i] < '0' || p[i] > '9') return EINVAL;
        uint64_t digit = (uint64_t)(p[i] - '0');
        if (v > (UINT64_MAX - digit) / 10) return ERANGE;
        v = v * 10 + digit;
    }
    *out = v;
    return 0;
}

static int parse_int_text(const uint8_t* p, size_t n, int64_t* out) {
    if (!n) return EINVAL;
    int negative = p[0] == '-';
    int positive = p[0] == '+';
    size_t start = (negative || positive) ? 1 : 0;
    if (start == n) return EINVAL;
    uint64_t v;
    int rc = parse_uint_text(p + start, n - start, &v);
    if (rc) return rc;
    if (negative) {
        uint64_t limit = (uint64_t)INT64_MAX + 1ULL;
        if (v > limit) return ERANGE;
        *out = v == limit ? INT64_MIN : -(int64_t)v;
    } else {
        if (v > (uint64_t)INT64_MAX) return ERANGE;
        *out = (int64_t)v;
    }
    return 0;
}

static int fixed_digits(const uint8_t* p, size_t n, int* out) {
    if (!n) return EINVAL;
    int v = 0;
    for (size_t i = 0; i < n; i++) {
        if (p[i] < '0' || p[i] > '9') return EINVAL;
        v = v * 10 + (int)(p[i] - '0');
    }
    *out = v;
    return 0;
}

static int parse_date_text(const uint8_t* p, size_t n, int* y, int* mon, int* day) {
    if (n != 10 || p[4] != '-' || p[7] != '-') return EINVAL;
    if (fixed_digits(p,4,y) || fixed_digits(p+5,2,mon) || fixed_digits(p+8,2,day))
        return EINVAL;
    if (*y == 0 && *mon == 0 && *day == 0) return 1;
    if (*y < 1 || *mon < 1 || *mon > 12 || *day < 1 || *day > 31) return EINVAL;
    return 0;
}

static int parse_datetime_text(
        const uint8_t* p, size_t n, int* y, int* mon, int* day,
        int* hour, int* minute, int* second, int* micros) {
    if (n < 19 || p[4] != '-' || p[7] != '-' || p[10] != ' ' ||
        p[13] != ':' || p[16] != ':') return EINVAL;
    if (fixed_digits(p,4,y) || fixed_digits(p+5,2,mon) || fixed_digits(p+8,2,day) ||
        fixed_digits(p+11,2,hour) || fixed_digits(p+14,2,minute) ||
        fixed_digits(p+17,2,second)) return EINVAL;
    if (*y == 0 && *mon == 0 && *day == 0) return 1;
    if (*y < 1 || *mon < 1 || *mon > 12 || *day < 1 || *day > 31 ||
        *hour > 23 || *minute > 59 || *second > 59) return EINVAL;
    *micros = 0;
    if (n == 19) return 0;
    if (p[19] != '.' || n < 21 || n > 26) return EINVAL;
    int frac = 0;
    if (fixed_digits(p+20,n-20,&frac)) return EINVAL;
    for (size_t i = n-20; i < 6; i++) frac *= 10;
    *micros = frac;
    return 0;
}

static int append_decimal_text(
        struct ArrowArray* out, const struct ColumnConfig* cfg,
        const uint8_t* p, size_t n) {
    if (!n || cfg->precision > 38) return EINVAL;
    size_t in = 0, out_pos = 0;
    char digits[96];
    if (p[in] == '-' || p[in] == '+') {
        digits[out_pos++] = (char)p[in++];
        if (in == n) return EINVAL;
    }
    size_t dot = n;
    for (size_t i = in; i < n; i++) {
        if (p[i] == '.') {
            if (dot != n) return EINVAL;
            dot = i;
        } else if (p[i] < '0' || p[i] > '9') {
            return EINVAL;
        }
    }
    size_t integral_end = dot == n ? n : dot;
    if (integral_end == in) return EINVAL;
    size_t frac_start = dot == n ? n : dot + 1;
    size_t frac_len = n - frac_start;
    if (frac_len > cfg->scale) return EINVAL;
    size_t digit_count = integral_end - in + cfg->scale;
    if (digit_count > cfg->precision || out_pos + digit_count >= sizeof(digits))
        return ERANGE;
    memcpy(digits + out_pos,p + in,integral_end - in);
    out_pos += integral_end - in;
    if (frac_len) {
        memcpy(digits + out_pos,p + frac_start,frac_len);
        out_pos += frac_len;
    }
    while (frac_len++ < cfg->scale) digits[out_pos++] = '0';

    struct ArrowDecimal dec;
    ArrowDecimalInit(&dec,128,cfg->precision,cfg->scale);
    int rc = ArrowDecimalSetDigits(&dec,string_view((const uint8_t*)digits,(int64_t)out_pos));
    if (rc) return rc;
    return ArrowArrayAppendDecimal(out,&dec);
}

static int append_text_value(
        struct ArrowArray* out, const struct ColumnConfig* cfg,
        const uint8_t* p, size_t n) {
    uint64_t u;
    int64_t s;
    switch (cfg->kind) {
        case AK_I8:
        case AK_I16:
        case AK_I32:
        case AK_I64:
            if (parse_int_text(p,n,&s)) return EINVAL;
            return ArrowArrayAppendInt(out,s);
        case AK_U8:
        case AK_U16:
        case AK_U32:
        case AK_U64:
            if (parse_uint_text(p,n,&u)) return EINVAL;
            return ArrowArrayAppendUInt(out,u);
        case AK_F32:
        case AK_F64: {
            if (!n || n >= 128) return EINVAL;
            char buf[128];
            memcpy(buf,p,n);
            buf[n] = 0;
            errno = 0;
            char* end = NULL;
            double value = strtod(buf,&end);
            if (errno == ERANGE || end != buf + n) return EINVAL;
            return ArrowArrayAppendDouble(out,value);
        }
        case AK_STRING:
            return ArrowArrayAppendString(out,string_view(p,(int64_t)n));
        case AK_BINARY:
            return ArrowArrayAppendBytes(out,bytes_view(p,(int64_t)n));
        case AK_DATE32: {
            int y,mon,day;
            int rc = parse_date_text(p,n,&y,&mon,&day);
            if (rc == 1) return ArrowArrayAppendNull(out,1);
            if (rc) return rc;
            return ArrowArrayAppendInt(out,days_from_civil(y,(unsigned)mon,(unsigned)day));
        }
        case AK_TIMESTAMP_US: {
            int y,mon,day,hour,minute,second,micros;
            int rc = parse_datetime_text(
                p,n,&y,&mon,&day,&hour,&minute,&second,&micros);
            if (rc == 1) return ArrowArrayAppendNull(out,1);
            if (rc) return rc;
            return ArrowArrayAppendInt(
                out,timestamp_us(y,mon,day,hour,minute,second,micros));
        }
        case AK_DECIMAL128:
            return append_decimal_text(out,cfg,p,n);
        default:
            return ENOTSUP;
    }
}

static int decode_snapshot(
        struct Decoder* d, const uint8_t* payload, size_t size,
        struct ArrowSchema* schema, struct ArrowArray* array,
        struct TableConfig** out_cfg) {
    struct Cursor c = {payload,size,0};
    uint16_t db_len,table_len;
    const uint8_t *db,*table,*row_data;
    uint64_t order_base;
    uint32_t nrows;
    if (cur_u16(&c,&db_len) || cur_take(&c,db_len,&db) ||
        cur_u16(&c,&table_len) || cur_take(&c,table_len,&table) ||
        cur_u64(&c,&order_base) || cur_u32(&c,&nrows))
        return EINVAL;

    struct TableConfig* cfg = find_config_view(d,db,db_len,table,table_len);
    if (!cfg) return ENOENT;
    int rc = build_schema(cfg,schema);
    if (rc) return rc;
    struct ArrowError error;
    rc = ArrowArrayInitFromSchema(array,schema,&error);
    if (rc) return rc;
    rc = ArrowArrayStartAppending(array);
    if (rc) return rc;

    for (uint32_t row = 0; row < nrows; row++) {
        uint32_t row_len;
        if (cur_u32(&c,&row_len) || cur_take(&c,row_len,&row_data)) return EINVAL;
        struct Cursor r = {row_data,row_len,0};
        for (uint16_t i = 0; i < cfg->ncol; i++) {
            if (r.pos >= r.size) return EINVAL;
            if (r.data[r.pos] == 0xfb) {
                r.pos++;
                rc = ArrowArrayAppendNull(array->children[i],1);
            } else {
                uint64_t value_len;
                const uint8_t* value;
                if (cur_lenenc(&r,&value_len) || value_len > r.size-r.pos ||
                    cur_take(&r,(size_t)value_len,&value))
                    return EINVAL;
                rc = append_text_value(array->children[i],&cfg->col[i],value,(size_t)value_len);
            }
            if (rc) return rc;
        }
        if (r.pos != r.size) return EINVAL;
        rc = ArrowArrayAppendInt(array->children[cfg->ncol],0);
        if (rc) return rc;
        if (order_base > (uint64_t)INT64_MAX - row) return EOVERFLOW;
        rc = ArrowArrayAppendInt(
            array->children[cfg->ncol+1],(int64_t)(order_base + row));
        if (rc) return rc;
        rc = ArrowArrayFinishElement(array);
        if (rc) return rc;
    }
    if (c.pos != c.size) return EINVAL;
    rc = ArrowArrayFinishBuildingDefault(array,&error);
    if (rc) return rc;
    *out_cfg = cfg;
    return 0;
}

static int parse_table_map(struct Decoder* d, const uint8_t* event, size_t size) {
    if (size < 19 + 8) return EINVAL;
    struct Cursor c = {event + 19, size - 19, 0};
    const uint8_t* p;
    if (cur_take(&c, 6, &p)) return EINVAL;
    uint64_t table_id = uint_le_n(p, 6);
    if (cur_take(&c, 2, &p)) return EINVAL;

    uint8_t db_len, table_len;
    if (cur_u8(&c, &db_len)) return EINVAL;
    if (cur_take(&c, db_len, &p)) return EINVAL;
    char* db = (char*)malloc((size_t)db_len + 1);
    if (!db) return ENOMEM;
    memcpy(db, p, db_len); db[db_len] = 0;
    if (cur_take(&c, 1, &p)) { free(db); return EINVAL; }

    if (cur_u8(&c, &table_len)) { free(db); return EINVAL; }
    if (cur_take(&c, table_len, &p)) { free(db); return EINVAL; }
    char* table = (char*)malloc((size_t)table_len + 1);
    if (!table) { free(db); return ENOMEM; }
    memcpy(table, p, table_len); table[table_len] = 0;
    if (cur_take(&c, 1, &p)) { free(db); free(table); return EINVAL; }

    uint64_t ncol64;
    if (cur_lenenc(&c, &ncol64) || ncol64 > MAX_COLUMNS) { free(db); free(table); return EINVAL; }
    uint16_t ncol = (uint16_t)ncol64;
    const uint8_t* types;
    if (cur_take(&c, ncol, &types)) { free(db); free(table); return EINVAL; }
    uint64_t meta_len;
    if (cur_lenenc(&c, &meta_len) || meta_len > c.size - c.pos) { free(db); free(table); return EINVAL; }
    struct Cursor mc = {c.data + c.pos, (size_t)meta_len, 0};
    c.pos += (size_t)meta_len;

    struct TableConfig* cfg = find_config(d, db, table);
    free(db); free(table);

    struct TableMap* map = put_map(d, table_id);
    if (!map) return ENOSPC;
    if (!cfg) {
        map->state = MAP_IGNORED;
        map->ncol = ncol;
        return 0;
    }
    if (cfg->ncol != ncol) return EINVAL;

    map->state = MAP_SUBSCRIBED;
    map->config = cfg;
    map->ncol = ncol;
    map->col = (struct ColumnMeta*)calloc(ncol, sizeof(struct ColumnMeta));
    if (!map->col) return ENOMEM;

    for (uint16_t i = 0; i < ncol; i++) {
        struct ColumnMeta* m = &map->col[i];
        m->type = types[i];
        uint8_t a, b;
        switch (m->type) {
            case MYSQL_TYPE_VARCHAR: {
                uint16_t v;
                if (cur_u16(&mc, &v)) return EINVAL;
                m->max_length = v;
                break;
            }
            case MYSQL_TYPE_FLOAT:
            case MYSQL_TYPE_DOUBLE:
                if (cur_u8(&mc, &a)) return EINVAL;
                m->meta = a;
                break;
            case MYSQL_TYPE_TIMESTAMP2:
            case MYSQL_TYPE_DATETIME2:
            case MYSQL_TYPE_TIME2:
                if (cur_u8(&mc, &m->fsp)) return EINVAL;
                break;
            case MYSQL_TYPE_VAR_STRING:
            case MYSQL_TYPE_STRING:
                if (cur_u8(&mc, &a) || cur_u8(&mc, &b)) return EINVAL;
                m->meta = ((uint16_t)a << 8) | b;
                if (a == MYSQL_TYPE_SET || a == MYSQL_TYPE_ENUM) return ENOTSUP;
                m->max_length = (uint16_t)((((m->meta >> 4) & 0x300) ^ 0x300) + (m->meta & 0xff));
                break;
            case MYSQL_TYPE_TINY_BLOB:
            case MYSQL_TYPE_MEDIUM_BLOB:
            case MYSQL_TYPE_LONG_BLOB:
            case MYSQL_TYPE_BLOB:
            case MYSQL_TYPE_JSON:
            case MYSQL_TYPE_GEOMETRY:
                if (cur_u8(&mc, &m->length_size)) return EINVAL;
                break;
            case MYSQL_TYPE_NEWDECIMAL:
                if (cur_u8(&mc, &m->precision) || cur_u8(&mc, &m->scale)) return EINVAL;
                break;
            case MYSQL_TYPE_BIT:
                if (cur_u8(&mc, &a) || cur_u8(&mc, &b)) return EINVAL;
                m->meta = ((uint16_t)a << 8) | b;
                break;
            default:
                break;
        }
    }
    return 0;
}

static int append_decimal(struct ArrowArray* out, struct Cursor* c, uint8_t precision, uint8_t scale) {
    static const int compressed_bytes[10] = {0,1,1,2,2,3,3,4,4,4};
    int integral = precision - scale;
    int uncomp_i = integral / 9, uncomp_f = scale / 9;
    int comp_i = integral - uncomp_i * 9, comp_f = scale - uncomp_f * 9;
    int size = compressed_bytes[comp_i] + uncomp_i * 4 + uncomp_f * 4 + compressed_bytes[comp_f];
    const uint8_t* raw;
    if (cur_take(c, (size_t)size, &raw)) return EINVAL;

    uint8_t* tmp = (uint8_t*)malloc((size_t)size);
    if (!tmp) return ENOMEM;
    memcpy(tmp, raw, (size_t)size);
    int positive = (tmp[0] & 0x80) != 0;
    tmp[0] ^= 0x80;
    int64_t mask = positive ? 0 : -1;

    char digits[160];
    size_t used = 0;
    if (!positive) digits[used++] = '-';
    int pos = 0;
    if (comp_i) {
        int64_t v = int_be_n(tmp + pos, compressed_bytes[comp_i]) ^ mask;
        int n = snprintf(digits + used, sizeof(digits) - used, "%lld", (long long)v);
        used += (size_t)n;
        pos += compressed_bytes[comp_i];
    }
    for (int i = 0; i < uncomp_i; i++, pos += 4) {
        int64_t v = int_be_n(tmp + pos, 4) ^ mask;
        int n = snprintf(digits + used, sizeof(digits) - used,
                         comp_i || i ? "%09lld" : "%lld", (long long)v);
        used += (size_t)n;
    }
    if (integral == 0) digits[used++] = '0';
    for (int i = 0; i < uncomp_f; i++, pos += 4) {
        int64_t v = int_be_n(tmp + pos, 4) ^ mask;
        int n = snprintf(digits + used, sizeof(digits) - used, "%09lld", (long long)v);
        used += (size_t)n;
    }
    if (comp_f) {
        int64_t v = int_be_n(tmp + pos, compressed_bytes[comp_f]) ^ mask;
        int n = snprintf(digits + used, sizeof(digits) - used, "%0*lld", comp_f,
                         (long long)v);
        used += (size_t)n;
    }
    digits[used] = 0;
    free(tmp);

    struct ArrowDecimal dec;
    ArrowDecimalInit(&dec, 128, precision, scale);
    int rc = ArrowDecimalSetDigits(&dec, ArrowCharView(digits));
    if (rc) return rc;
    return ArrowArrayAppendDecimal(out, &dec);
}

static int append_value(struct ArrowArray* out, const struct ColumnConfig* cfg,
                        const struct ColumnMeta* meta, struct Cursor* c) {
    const uint8_t* p;
    uint64_t u;
    int64_t s;
    switch (meta->type) {
        case MYSQL_TYPE_TINY:
            if (cur_take(c, 1, &p)) return EINVAL;
            return cfg->is_unsigned ? ArrowArrayAppendUInt(out, p[0])
                                    : ArrowArrayAppendInt(out, (int8_t)p[0]);
        case MYSQL_TYPE_SHORT:
            if (cur_take(c, 2, &p)) return EINVAL;
            return cfg->is_unsigned ? ArrowArrayAppendUInt(out, u16le(p))
                                    : ArrowArrayAppendInt(out, (int16_t)u16le(p));
        case MYSQL_TYPE_LONG:
            if (cur_take(c, 4, &p)) return EINVAL;
            return cfg->is_unsigned ? ArrowArrayAppendUInt(out, u32le(p))
                                    : ArrowArrayAppendInt(out, (int32_t)u32le(p));
        case MYSQL_TYPE_INT24:
            if (cur_take(c, 3, &p)) return EINVAL;
            u = u24le(p);
            s = (u & 0x800000) ? (int64_t)u - 0x1000000LL : (int64_t)u;
            return cfg->is_unsigned ? ArrowArrayAppendUInt(out, u) : ArrowArrayAppendInt(out, s);
        case MYSQL_TYPE_LONGLONG:
            if (cur_take(c, 8, &p)) return EINVAL;
            u = u64le(p);
            return cfg->is_unsigned ? ArrowArrayAppendUInt(out, u)
                                    : ArrowArrayAppendInt(out, (int64_t)u);
        case MYSQL_TYPE_FLOAT: {
            if (cur_take(c, 4, &p)) return EINVAL;
            float v; memcpy(&v, p, 4);
            return ArrowArrayAppendDouble(out, v);
        }
        case MYSQL_TYPE_DOUBLE: {
            if (cur_take(c, 8, &p)) return EINVAL;
            double v; memcpy(&v, p, 8);
            return ArrowArrayAppendDouble(out, v);
        }
        case MYSQL_TYPE_VARCHAR:
        case MYSQL_TYPE_VAR_STRING:
        case MYSQL_TYPE_STRING: {
            int nlen = meta->max_length > 255 ? 2 : 1;
            if (cur_take(c, (size_t)nlen, &p)) return EINVAL;
            uint64_t len = uint_le_n(p, nlen);
            if (cur_take(c, (size_t)len, &p)) return EINVAL;
            if (cfg->kind == AK_BINARY)
                return ArrowArrayAppendBytes(out, bytes_view(p, (int64_t)len));
            return ArrowArrayAppendString(out, string_view(p, (int64_t)len));
        }
        case MYSQL_TYPE_TINY_BLOB:
        case MYSQL_TYPE_MEDIUM_BLOB:
        case MYSQL_TYPE_LONG_BLOB:
        case MYSQL_TYPE_BLOB:
        case MYSQL_TYPE_GEOMETRY: {
            int nlen = meta->length_size;
            if (nlen < 1 || nlen > 4 || cur_take(c, (size_t)nlen, &p)) return EINVAL;
            uint64_t len = uint_le_n(p, nlen);
            if (cur_take(c, (size_t)len, &p)) return EINVAL;
            if (cfg->kind == AK_STRING)
                return ArrowArrayAppendString(out, string_view(p, (int64_t)len));
            return ArrowArrayAppendBytes(out, bytes_view(p, (int64_t)len));
        }
        case MYSQL_TYPE_NEWDECIMAL:
            return append_decimal(out, c, meta->precision, meta->scale);
        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_NEWDATE: {
            if (cur_take(c, 3, &p)) return EINVAL;
            uint32_t v = u24le(p);
            if (!v) return ArrowArrayAppendNull(out, 1);
            int day = v & 31, mon = (v >> 5) & 15, year = (int)(v >> 9);
            return ArrowArrayAppendInt(out, days_from_civil(year, (unsigned)mon, (unsigned)day));
        }
        case MYSQL_TYPE_TIMESTAMP: {
            if (cur_take(c, 4, &p)) return EINVAL;
            return ArrowArrayAppendInt(out, (int64_t)u32le(p) * 1000000);
        }
        case MYSQL_TYPE_TIMESTAMP2: {
            if (cur_take(c, 4, &p)) return EINVAL;
            int us;
            int32_t sec = (int32_t)uint_be_n(p, 4);
            if (read_fsp(c, meta->fsp, &us)) return EINVAL;
            return ArrowArrayAppendInt(out, (int64_t)sec * 1000000 + us);
        }
        case MYSQL_TYPE_DATETIME: {
            if (cur_take(c, 8, &p)) return EINVAL;
            uint64_t v = u64le(p);
            if (!v) return ArrowArrayAppendNull(out, 1);
            uint64_t date = v / 1000000, tim = v % 1000000;
            int y = (int)(date / 10000), mon = (int)((date % 10000) / 100), day = (int)(date % 100);
            int h = (int)(tim / 10000), mi = (int)((tim % 10000) / 100), sec = (int)(tim % 100);
            if (!y || !mon || !day) return ArrowArrayAppendNull(out, 1);
            return ArrowArrayAppendInt(out, timestamp_us(y, mon, day, h, mi, sec, 0));
        }
        case MYSQL_TYPE_DATETIME2: {
            if (cur_take(c, 5, &p)) return EINVAL;
            uint64_t raw = uint_be_n(p, 5);
            if (raw < 0x8000000000ULL) return EINVAL;
            uint64_t v = raw - 0x8000000000ULL;
            int ym = (int)(v >> 22);
            int y = ym / 13, mon = ym % 13;
            int day = (int)((v >> 17) & 31), h = (int)((v >> 12) & 31);
            int mi = (int)((v >> 6) & 63), sec = (int)(v & 63), us;
            if (read_fsp(c, meta->fsp, &us)) return EINVAL;
            if (!y || !mon || !day) return ArrowArrayAppendNull(out, 1);
            return ArrowArrayAppendInt(out, timestamp_us(y, mon, day, h, mi, sec, us));
        }
        case MYSQL_TYPE_YEAR:
            if (cur_take(c, 1, &p)) return EINVAL;
            return ArrowArrayAppendInt(out, p[0] ? (int64_t)p[0] + 1900 : 0);
        case MYSQL_TYPE_BIT: {
            int nbits = ((meta->meta & 0xff) * 8) + (meta->meta >> 8);
            int n = (nbits + 7) / 8;
            if (cur_take(c, (size_t)n, &p)) return EINVAL;
            return ArrowArrayAppendBytes(out, bytes_view(p, n));
        }
        case MYSQL_TYPE_TIME:
        case MYSQL_TYPE_TIME2:
        case MYSQL_TYPE_JSON:
        case MYSQL_TYPE_ENUM:
        case MYSQL_TYPE_SET:
            return ENOTSUP;
        default:
            return ENOTSUP;
    }
}

static int decode_image(struct TableMap* map, struct Cursor* c, const uint8_t* bitmap,
                        int op, int64_t* order, struct ArrowArray* array) {
    int present = bit_count(bitmap, map->ncol);
    size_t null_bytes = (size_t)(present + 7) / 8;
    const uint8_t* nulls;
    if (cur_take(c, null_bytes, &nulls)) return EINVAL;
    int null_index = 0;

    for (uint16_t i = 0; i < map->ncol; i++) {
        if (!bit_get(bitmap, i)) return ENOTSUP;
        int is_null = bit_get(nulls, null_index++);
        int rc = is_null ? ArrowArrayAppendNull(array->children[i], 1)
                         : append_value(array->children[i], &map->config->col[i], &map->col[i], c);
        if (rc) return rc;
    }
    int rc = ArrowArrayAppendInt(array->children[map->ncol], op);
    if (rc) return rc;
    rc = ArrowArrayAppendInt(array->children[map->ncol + 1], (*order)++);
    if (rc) return rc;
    return ArrowArrayFinishElement(array);
}

static int decode_rows(struct Decoder* d, const uint8_t* event, size_t size,
                       struct ArrowSchema* schema, struct ArrowArray* array,
                       struct TableConfig** out_cfg) {
    if (size < 19 + 10) return EINVAL;
    uint8_t event_type = event[4];
    if (event_type == PARTIAL_UPDATE_ROWS_EVENT) return ENOTSUP;

    struct Cursor c = {event + 19, size - 19, 0};
    const uint8_t* p;
    if (cur_take(&c, 6, &p)) return EINVAL;
    uint64_t table_id = uint_le_n(p, 6);
    struct TableMap* map = find_map(d, table_id);
    if (!map) return DECODER_UNKNOWN_MAP;
    if (map->state == MAP_IGNORED) return DECODER_IGNORED;
    if (map->state != MAP_SUBSCRIBED || !map->config) return EINVAL;
    if (cur_take(&c, 2, &p)) return EINVAL;

    int v2 = event_type == WRITE_ROWS_EVENT_V2 || event_type == UPDATE_ROWS_EVENT_V2 ||
             event_type == DELETE_ROWS_EVENT_V2 || event_type == PARTIAL_UPDATE_ROWS_EVENT;
    if (v2) {
        uint16_t extra_len;
        if (cur_u16(&c, &extra_len) || extra_len < 2) return EINVAL;
        if (extra_len > 2 && cur_take(&c, extra_len - 2, &p)) return EINVAL;
    }

    uint64_t ncol64;
    if (cur_lenenc(&c, &ncol64) || ncol64 != map->ncol) return EINVAL;
    size_t bitmap_bytes = (map->ncol + 7) / 8;
    const uint8_t *bitmap1, *bitmap2 = NULL;
    if (cur_take(&c, bitmap_bytes, &bitmap1)) return EINVAL;
    int update = event_type == UPDATE_ROWS_EVENT_V1 || event_type == UPDATE_ROWS_EVENT_V2;
    if (update && cur_take(&c, bitmap_bytes, &bitmap2)) return EINVAL;

    int rc = build_schema(map->config, schema);
    if (rc) return rc;
    struct ArrowError error;
    rc = ArrowArrayInitFromSchema(array, schema, &error);
    if (rc) return rc;
    rc = ArrowArrayStartAppending(array);
    if (rc) return rc;

    int64_t order = 0;
    while (c.pos < c.size) {
        int op1 = (event_type == DELETE_ROWS_EVENT_V1 || event_type == DELETE_ROWS_EVENT_V2) ? 1 : 0;
        if (update) op1 = 1;
        rc = decode_image(map, &c, bitmap1, op1, &order, array);
        if (rc) return rc;
        if (update) {
            rc = decode_image(map, &c, bitmap2, 0, &order, array);
            if (rc) return rc;
        }
    }
    rc = ArrowArrayFinishBuildingDefault(array, &error);
    if (rc) return rc;
    *out_cfg = map->config;
    return 0;
}

static int ipc_buffer(struct ArrowSchema* schema, struct ArrowArray* array, struct ArrowBuffer* out) {
    struct ArrowError error;
    struct ArrowArrayView view;
    memset(&view, 0, sizeof(view));
    int rc = ArrowArrayViewInitFromSchema(&view, schema, &error);
    if (rc) return rc;
    rc = ArrowArrayViewSetArray(&view, array, &error);
    if (rc) { ArrowArrayViewReset(&view); return rc; }

    struct ArrowIpcOutputStream stream;
    memset(&stream, 0, sizeof(stream));
    rc = ArrowIpcOutputStreamInitBuffer(&stream, out);
    if (rc) { ArrowArrayViewReset(&view); return rc; }
    struct ArrowIpcWriter writer;
    memset(&writer, 0, sizeof(writer));
    rc = ArrowIpcWriterInit(&writer, &stream);
    if (!rc) rc = ArrowIpcWriterWriteSchema(&writer, schema, &error);
    if (!rc) rc = ArrowIpcWriterWriteArrayView(&writer, &view, &error);
    if (!rc) rc = ArrowIpcWriterWriteArrayView(&writer, NULL, &error);
    ArrowIpcWriterReset(&writer);
    ArrowArrayViewReset(&view);
    return rc;
}

static int write_all(FILE* f, const void* data, size_t n) {
    const uint8_t* p = (const uint8_t*)data;
    size_t done = 0;
    while (done < n) {
        size_t wrote = fwrite(p + done, 1, n - done, f);
        if (wrote) {
            done += wrote;
            continue;
        }
        if (ferror(f) && errno == EINTR) {
            clearerr(f);
            continue;
        }
        return EIO;
    }
    return 0;
}

static int write_frame(uint8_t type, const void* payload, uint32_t len) {
    if (len > MAX_FRAME_BYTES) return EOVERFLOW;
    uint8_t hdr[5] = {type, (uint8_t)len, (uint8_t)(len >> 8), (uint8_t)(len >> 16), (uint8_t)(len >> 24)};
    if (write_all(stdout, hdr, sizeof(hdr)) || (len && write_all(stdout, payload, len))) return EIO;
    return fflush(stdout) == EOF ? EIO : 0;
}

static int emit_error(const char* msg) {
    return write_frame(FRAME_ERROR, msg, (uint32_t)strlen(msg));
}

static int emit_batch(struct TableConfig* cfg, struct ArrowSchema* schema, struct ArrowArray* array) {
    struct ArrowBuffer out;
    ArrowBufferInit(&out);
    int rc = ipc_buffer(schema, array, &out);
    if (rc) { ArrowBufferReset(&out); return rc; }

    size_t db_len = strlen(cfg->db), table_len = strlen(cfg->table);
    size_t total = 2 + db_len + 2 + table_len + out.size_bytes;
    if (total > UINT32_MAX) { ArrowBufferReset(&out); return EOVERFLOW; }
    uint8_t* payload = (uint8_t*)malloc(total);
    if (!payload) { ArrowBufferReset(&out); return ENOMEM; }
    size_t pos = 0;
    payload[pos++] = (uint8_t)db_len; payload[pos++] = (uint8_t)(db_len >> 8);
    memcpy(payload + pos, cfg->db, db_len); pos += db_len;
    payload[pos++] = (uint8_t)table_len; payload[pos++] = (uint8_t)(table_len >> 8);
    memcpy(payload + pos, cfg->table, table_len); pos += table_len;
    memcpy(payload + pos, out.data, (size_t)out.size_bytes);
    rc = write_frame(FRAME_BATCH, payload, (uint32_t)total);
    free(payload);
    ArrowBufferReset(&out);
    return rc;
}

static char* dup_bytes(const uint8_t* p, size_t n) {
    char* s = (char*)malloc(n + 1);
    if (!s) return NULL;
    memcpy(s, p, n); s[n] = 0;
    return s;
}

static int parse_config(struct Decoder* d, const uint8_t* payload, size_t size) {
    decoder_reset(d);
    struct Cursor c = {payload, size, 0};
    uint16_t nt;
    if (cur_u16(&c, &nt) || nt > MAX_TABLES) return EINVAL;
    d->ntables = nt;
    for (uint16_t t = 0; t < nt; t++) {
        struct TableConfig* cfg = &d->tables[t];
        uint16_t n;
        const uint8_t* p;
        if (cur_u16(&c, &n) || cur_take(&c, n, &p)) return EINVAL;
        cfg->db = dup_bytes(p, n);
        if (!cfg->db) return ENOMEM;
        if (cur_u16(&c, &n) || cur_take(&c, n, &p)) return EINVAL;
        cfg->table = dup_bytes(p, n);
        if (!cfg->table) return ENOMEM;
        if (cur_u16(&c, &cfg->ncol) || cfg->ncol > MAX_COLUMNS) return EINVAL;
        cfg->col = (struct ColumnConfig*)calloc(cfg->ncol, sizeof(struct ColumnConfig));
        if (!cfg->col) return ENOMEM;
        for (uint16_t i = 0; i < cfg->ncol; i++) {
            if (cur_u16(&c, &n) || cur_take(&c, n, &p)) return EINVAL;
            cfg->col[i].name = dup_bytes(p, n);
            if (!cfg->col[i].name) return ENOMEM;
            if (cur_u8(&c, &cfg->col[i].kind) || cur_u8(&c, &cfg->col[i].is_unsigned) ||
                cur_u8(&c, &cfg->col[i].precision) || cur_u8(&c, &cfg->col[i].scale))
                return EINVAL;
        }
    }
    return c.pos == c.size ? 0 : EINVAL;
}

static int read_exact(FILE* f, void* out, size_t n) {
    uint8_t* p = (uint8_t*)out;
    size_t done = 0;
    while (done < n) {
        size_t got = fread(p + done, 1, n - done, f);
        if (got) {
            done += got;
            continue;
        }
        if (feof(f)) return EOF;
        if (ferror(f) && errno == EINTR) {
            clearerr(f);
            continue;
        }
        return EIO;
    }
    return 0;
}

static int service(void) {
    struct Decoder d;
    memset(&d, 0, sizeof(d));
    for (;;) {
        uint8_t hdr[5];
        int rr = read_exact(stdin, hdr, sizeof(hdr));
        if (rr == EOF) break;
        if (rr) { decoder_reset(&d); return 2; }
        uint8_t type = hdr[0];
        uint32_t len = u32le(hdr + 1);
        if (len > MAX_FRAME_BYTES) {
            emit_error("native input frame exceeds limit");
            decoder_reset(&d);
            return 3;
        }
        uint8_t* payload = NULL;
        if (len) {
            payload = (uint8_t*)malloc(len);
            if (!payload || read_exact(stdin, payload, len)) {
                free(payload); decoder_reset(&d); return 2;
            }
        }

        int rc = 0;
        if (type == FRAME_CONFIG) {
            rc = parse_config(&d, payload, len);
            if (!rc) rc = write_frame(FRAME_ACK, NULL, 0);
        } else if (type == FRAME_EVENT) {
            if (len < 19) rc = EINVAL;
            else {
                uint8_t event_type = payload[4];
                if (event_type == TABLE_MAP_EVENT) {
                    rc = parse_table_map(&d, payload, len);
                    if (!rc) rc = write_frame(FRAME_ACK, NULL, 0);
                } else if (event_type == WRITE_ROWS_EVENT_V1 || event_type == UPDATE_ROWS_EVENT_V1 ||
                           event_type == DELETE_ROWS_EVENT_V1 || event_type == WRITE_ROWS_EVENT_V2 ||
                           event_type == UPDATE_ROWS_EVENT_V2 || event_type == DELETE_ROWS_EVENT_V2 ||
                           event_type == PARTIAL_UPDATE_ROWS_EVENT) {
                    struct ArrowSchema schema;
                    struct ArrowArray array;
                    memset(&schema, 0, sizeof(schema));
                    memset(&array, 0, sizeof(array));
                    struct TableConfig* cfg = NULL;
                    rc = decode_rows(&d, payload, len, &schema, &array, &cfg);
                    if (rc == DECODER_IGNORED) {
                        rc = write_frame(FRAME_ACK, NULL, 0);
                    } else if (!rc) {
                        rc = emit_batch(cfg, &schema, &array);
                    }
                    if (array.release) array.release(&array);
                    if (schema.release) schema.release(&schema);
                } else {
                    rc = write_frame(FRAME_ACK, NULL, 0);
                }
            }
        } else if (type == FRAME_SNAPSHOT) {
            struct ArrowSchema schema;
            struct ArrowArray array;
            memset(&schema,0,sizeof(schema));
            memset(&array,0,sizeof(array));
            struct TableConfig* cfg = NULL;
            rc = decode_snapshot(&d,payload,len,&schema,&array,&cfg);
            if (!rc) rc = emit_batch(cfg,&schema,&array);
            if (array.release) array.release(&array);
            if (schema.release) schema.release(&schema);
        } else if (type == FRAME_QUIT) {
            free(payload);
            decoder_reset(&d);
            return 0;
        } else {
            rc = EINVAL;
        }
        free(payload);
        if (rc) {
            char msg[160];
            if (rc == DECODER_UNKNOWN_MAP) {
                snprintf(msg, sizeof(msg), "native decode failed: row event references unknown TABLE_MAP");
            } else {
                snprintf(msg, sizeof(msg), "native decode failed rc=%d", rc);
            }
            emit_error(msg);
            decoder_reset(&d);
            return 3;
        }
    }
    decoder_reset(&d);
    return 0;
}

static void put_u16(uint8_t* p, uint16_t v) { p[0] = (uint8_t)v; p[1] = (uint8_t)(v >> 8); }
static void put_u32(uint8_t* p, uint32_t v) {
    p[0]=(uint8_t)v; p[1]=(uint8_t)(v>>8); p[2]=(uint8_t)(v>>16); p[3]=(uint8_t)(v>>24);
}
static void put_u48(uint8_t* p, uint64_t v) { for (int i=0;i<6;i++) p[i]=(uint8_t)(v>>(8*i)); }
static void put_u64(uint8_t* p, uint64_t v) {
    for (int i=0;i<8;i++) p[i]=(uint8_t)(v>>(8*i));
}

static int selftest(void) {
    struct Decoder d;
    memset(&d, 0, sizeof(d));
    d.ntables = 1;
    d.tables[0].db = strdup("d");
    d.tables[0].table = strdup("t");
    d.tables[0].ncol = 2;
    d.tables[0].col = calloc(2, sizeof(struct ColumnConfig));
    d.tables[0].col[0].name = strdup("id");
    d.tables[0].col[0].kind = AK_I32;
    d.tables[0].col[1].name = strdup("name");
    d.tables[0].col[1].kind = AK_STRING;

    uint8_t tm[64] = {0};
    tm[4] = TABLE_MAP_EVENT;
    size_t p = 19;
    put_u48(tm+p, 1); p += 6;
    p += 2;
    tm[p++] = 1; tm[p++]='d'; tm[p++]=0;
    tm[p++] = 1; tm[p++]='t'; tm[p++]=0;
    tm[p++] = 2;
    tm[p++] = MYSQL_TYPE_LONG; tm[p++] = MYSQL_TYPE_VARCHAR;
    tm[p++] = 2; put_u16(tm+p, 100); p += 2;
    tm[p++] = 0;
    put_u32(tm+9, (uint32_t)p);
    size_t tm_size = p;
    if (parse_table_map(&d, tm, tm_size)) { decoder_reset(&d); return 10; }

    uint8_t wr[64] = {0};
    wr[4] = WRITE_ROWS_EVENT_V2;
    p = 19;
    put_u48(wr+p, 1); p += 6;
    p += 2;
    put_u16(wr+p, 2); p += 2;
    wr[p++] = 2;
    wr[p++] = 0x03;
    wr[p++] = 0x00;
    put_u32(wr+p, 42); p += 4;
    wr[p++] = 3; memcpy(wr+p, "abc", 3); p += 3;
    put_u32(wr+9, (uint32_t)p);

    struct ArrowSchema schema;
    struct ArrowArray array;
    memset(&schema,0,sizeof(schema)); memset(&array,0,sizeof(array));
    struct TableConfig* cfg = NULL;
    int rc = decode_rows(&d, wr, p, &schema, &array, &cfg);
    if (rc || !cfg || array.length != 1) rc = 11;
    if (!rc) {
        struct ArrowBuffer out; ArrowBufferInit(&out);
        rc = ipc_buffer(&schema, &array, &out);
        if (!rc && out.size_bytes < 32) rc = 12;
        ArrowBufferReset(&out);
    }
    if (array.release) array.release(&array);
    if (schema.release) schema.release(&schema);

    if (!rc) {
        uint8_t ignored_tm[64];
        memcpy(ignored_tm, tm, sizeof(ignored_tm));
        ignored_tm[31] = 'x';
        if (parse_table_map(&d, ignored_tm, tm_size)) rc = 13;
        struct TableMap* ignored = find_map(&d, 1);
        if (!rc && (!ignored || ignored->state != MAP_IGNORED)) rc = 14;
    }
    if (!rc) {
        struct ArrowSchema ignored_schema;
        struct ArrowArray ignored_array;
        memset(&ignored_schema,0,sizeof(ignored_schema));
        memset(&ignored_array,0,sizeof(ignored_array));
        struct TableConfig* ignored_cfg = NULL;
        int ignored_rc = decode_rows(&d, wr, p, &ignored_schema, &ignored_array, &ignored_cfg);
        if (ignored_rc != DECODER_IGNORED) rc = 15;
        if (ignored_array.release) ignored_array.release(&ignored_array);
        if (ignored_schema.release) ignored_schema.release(&ignored_schema);
    }
    if (!rc) {
        if (parse_table_map(&d, tm, tm_size)) rc = 16;
        struct TableMap* subscribed = find_map(&d, 1);
        if (!rc && (!subscribed || subscribed->state != MAP_SUBSCRIBED)) rc = 17;
    }
    if (!rc) {
        uint8_t unknown_wr[64];
        memcpy(unknown_wr, wr, sizeof(unknown_wr));
        put_u48(unknown_wr + 19, 2);
        struct ArrowSchema unknown_schema;
        struct ArrowArray unknown_array;
        memset(&unknown_schema,0,sizeof(unknown_schema));
        memset(&unknown_array,0,sizeof(unknown_array));
        struct TableConfig* unknown_cfg = NULL;
        int unknown_rc = decode_rows(
            &d, unknown_wr, p, &unknown_schema, &unknown_array, &unknown_cfg);
        if (unknown_rc != DECODER_UNKNOWN_MAP) rc = 18;
        if (unknown_array.release) unknown_array.release(&unknown_array);
        if (unknown_schema.release) unknown_schema.release(&unknown_schema);
    }
    if (!rc) {
        uint8_t snap[128] = {0};
        size_t s = 0;
        put_u16(snap+s,1); s += 2; snap[s++] = 'd';
        put_u16(snap+s,1); s += 2; snap[s++] = 't';
        put_u64(snap+s,5); s += 8;
        put_u32(snap+s,1); s += 4;
        uint8_t row[] = {2,'4','2',3,'a','b','c'};
        put_u32(snap+s,(uint32_t)sizeof(row)); s += 4;
        memcpy(snap+s,row,sizeof(row)); s += sizeof(row);
        struct ArrowSchema snap_schema;
        struct ArrowArray snap_array;
        memset(&snap_schema,0,sizeof(snap_schema));
        memset(&snap_array,0,sizeof(snap_array));
        struct TableConfig* snap_cfg = NULL;
        int snap_rc = decode_snapshot(&d,snap,s,&snap_schema,&snap_array,&snap_cfg);
        if (snap_rc || snap_cfg != &d.tables[0] || snap_array.length != 1) rc = 19;
        if (snap_array.release) snap_array.release(&snap_array);
        if (snap_schema.release) snap_schema.release(&snap_schema);
    }

    decoder_reset(&d);
    if (rc) return rc;
    fprintf(stderr, "native selftest OK\n");
    return 0;
}

int main(int argc, char** argv) {
    if (argc == 2 && !strcmp(argv[1], "--selftest")) return selftest();
    if (argc == 2 && !strcmp(argv[1], "--stdio")) return service();
    fprintf(stderr, "usage: %s --selftest | --stdio\n", argv[0]);
    return 2;
}

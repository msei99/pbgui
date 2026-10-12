"""GPU rolling-history cache for the pinned PB8 shader contract."""

def cache_hsl_shader(source: str) -> str:
    """Memoize complete HSL tree queries; invalidate every modified node."""
    if "struct HslNode {" not in source:
        return source
    if "pending_ema[32]" in source:
        raise ValueError("HSL history cache already applied")
    import hashlib

    begin = source.index("struct HslNode {")
    if hashlib.sha256(source[begin:begin+12835].encode()).hexdigest() != 'b0a5ae5c412365595e0fe909cad3adeecdc48012953a356159454e309640ec67':
        raise ValueError("Unexpected PB8 HSL shader contract; history cache cannot be applied")
    source = source.replace('return 2 * tree_size + (capacity + 3) / 4;', 'return 4 * tree_size + (capacity + 3) / 4;')
    source = source.replace('tree + 2 * tree_size);', 'tree + 4 * tree_size);')
    source = source.replace('i < 2 * tree_size;', 'i < 4 * tree_size;')
    source = source.replace('tree[index] = n;', 'tree[index] = n; tree[2*tree_size+index].count=0;')
    source = source.replace('tree[index] = hsl_join(tree[index * 2], tree[index * 2 + 1]);', 'tree[index] = hsl_join(tree[index * 2], tree[index * 2 + 1]); tree[2*tree_size+index].count=0;')
    source = source.replace('tree[w.tree_size + block] = n;', 'tree[w.tree_size + block] = n; tree[3*w.tree_size + block].count=0;')
    start = source.index('inline void hsl_visit(')
    end = source.index('inline float2 hsl_signal_peak(', start)
    piece = source[start:end]
    piece = piece.replace('int pending_hi[32];', 'int pending_hi[32]; float pending_ema[32]; float pending_peak[32];')
    piece = piece.replace('int lo = pending_lo[count];', 'if(index<0) {\n            index=-index;\n            device HslNode* cache=tree+2*tree_size+index;\n            cache->peak=pending_peak[count]; cache->first=offset; cache->last=peak;\n            cache->loss=ema; cache->decay=tree[index].decay; cache->count=1;\n            ema=pending_ema[count]*cache->decay+ema;\n            continue;\n        }\n        int lo = pending_lo[count];')
    piece = piece.replace('if (begin <= lo && hi <= end) {', 'if (begin <= lo && hi <= end) {\n            device HslNode* cache=tree+2*tree_size+index;\n            if(cache->count && cache->peak==peak && cache->first==offset) {\n                peak=cache->last; ema=ema*cache->decay+cache->loss; continue;\n            }\n            pending_index[count]=-index; pending_ema[count]=ema; pending_peak[count]=peak;\n            ++count; ema=0.0f;')
    return source[:start] + piece + source[end:]

use std::sync::OnceLock;

type BinaryKernel = fn(&[f32], &[f32]) -> f32;

#[inline]
fn squared_l2_scalar(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let mut i = 0usize;
    let mut acc0 = 0.0f32;
    let mut acc1 = 0.0f32;
    let mut acc2 = 0.0f32;
    let mut acc3 = 0.0f32;
    while i + 4 <= a.len() {
        let d0 = a[i] - b[i];
        let d1 = a[i + 1] - b[i + 1];
        let d2 = a[i + 2] - b[i + 2];
        let d3 = a[i + 3] - b[i + 3];
        acc0 += d0 * d0;
        acc1 += d1 * d1;
        acc2 += d2 * d2;
        acc3 += d3 * d3;
        i += 4;
    }
    let mut out = (acc0 + acc1) + (acc2 + acc3);
    while i < a.len() {
        let d = a[i] - b[i];
        out += d * d;
        i += 1;
    }
    out
}

#[inline]
fn dot_scalar(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let mut i = 0usize;
    let mut acc0 = 0.0f32;
    let mut acc1 = 0.0f32;
    let mut acc2 = 0.0f32;
    let mut acc3 = 0.0f32;
    while i + 4 <= a.len() {
        acc0 += a[i] * b[i];
        acc1 += a[i + 1] * b[i + 1];
        acc2 += a[i + 2] * b[i + 2];
        acc3 += a[i + 3] * b[i + 3];
        i += 4;
    }
    let mut out = (acc0 + acc1) + (acc2 + acc3);
    while i < a.len() {
        out += a[i] * b[i];
        i += 1;
    }
    out
}

#[inline]
pub fn squared_l2(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    squared_l2_dispatch()(a, b)
}

#[inline]
pub fn dot(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    dot_dispatch()(a, b)
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2,fma")]
unsafe fn squared_l2_avx2_fma(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::{
        __m256, _mm256_add_ps, _mm256_fmadd_ps, _mm256_loadu_ps, _mm256_setzero_ps,
        _mm256_storeu_ps, _mm256_sub_ps,
    };
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::{
        __m256, _mm256_add_ps, _mm256_fmadd_ps, _mm256_loadu_ps, _mm256_setzero_ps,
        _mm256_storeu_ps, _mm256_sub_ps,
    };

    let mut i = 0usize;
    let mut sum0: __m256 = _mm256_setzero_ps();
    let mut sum1: __m256 = _mm256_setzero_ps();
    while i + 16 <= a.len() {
        // SAFETY: bounds checked by loop guard and unaligned loads are permitted.
        let va0 = unsafe { _mm256_loadu_ps(a.as_ptr().add(i)) };
        // SAFETY: bounds checked by loop guard and unaligned loads are permitted.
        let vb0 = unsafe { _mm256_loadu_ps(b.as_ptr().add(i)) };
        let d0 = _mm256_sub_ps(va0, vb0);
        sum0 = _mm256_fmadd_ps(d0, d0, sum0);

        // SAFETY: bounds checked by loop guard and unaligned loads are permitted.
        let va1 = unsafe { _mm256_loadu_ps(a.as_ptr().add(i + 8)) };
        // SAFETY: bounds checked by loop guard and unaligned loads are permitted.
        let vb1 = unsafe { _mm256_loadu_ps(b.as_ptr().add(i + 8)) };
        let d1 = _mm256_sub_ps(va1, vb1);
        sum1 = _mm256_fmadd_ps(d1, d1, sum1);
        i += 16;
    }
    let mut sum = _mm256_add_ps(sum0, sum1);
    while i + 8 <= a.len() {
        // SAFETY: bounds checked by loop guard and unaligned loads are permitted.
        let va = unsafe { _mm256_loadu_ps(a.as_ptr().add(i)) };
        // SAFETY: bounds checked by loop guard and unaligned loads are permitted.
        let vb = unsafe { _mm256_loadu_ps(b.as_ptr().add(i)) };
        let d = _mm256_sub_ps(va, vb);
        sum = _mm256_fmadd_ps(d, d, sum);
        i += 8;
    }

    let mut lanes = [0f32; 8];
    // SAFETY: writing exactly 8 f32 lanes to a properly sized stack array.
    unsafe { _mm256_storeu_ps(lanes.as_mut_ptr(), sum) };
    let mut out = lanes.iter().sum::<f32>();
    while i < a.len() {
        let d = a[i] - b[i];
        out += d * d;
        i += 1;
    }
    out
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
unsafe fn squared_l2_avx2(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::{
        __m256, _mm256_add_ps, _mm256_loadu_ps, _mm256_mul_ps, _mm256_setzero_ps, _mm256_storeu_ps,
        _mm256_sub_ps,
    };
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::{
        __m256, _mm256_add_ps, _mm256_loadu_ps, _mm256_mul_ps, _mm256_setzero_ps, _mm256_storeu_ps,
        _mm256_sub_ps,
    };

    let mut i = 0usize;
    let mut sum0: __m256 = _mm256_setzero_ps();
    let mut sum1: __m256 = _mm256_setzero_ps();
    while i + 16 <= a.len() {
        // SAFETY: bounds checked by loop guard and unaligned loads are permitted.
        let va0 = unsafe { _mm256_loadu_ps(a.as_ptr().add(i)) };
        // SAFETY: bounds checked by loop guard and unaligned loads are permitted.
        let vb0 = unsafe { _mm256_loadu_ps(b.as_ptr().add(i)) };
        let d0 = _mm256_sub_ps(va0, vb0);
        sum0 = _mm256_add_ps(sum0, _mm256_mul_ps(d0, d0));

        // SAFETY: bounds checked by loop guard and unaligned loads are permitted.
        let va1 = unsafe { _mm256_loadu_ps(a.as_ptr().add(i + 8)) };
        // SAFETY: bounds checked by loop guard and unaligned loads are permitted.
        let vb1 = unsafe { _mm256_loadu_ps(b.as_ptr().add(i + 8)) };
        let d1 = _mm256_sub_ps(va1, vb1);
        sum1 = _mm256_add_ps(sum1, _mm256_mul_ps(d1, d1));
        i += 16;
    }
    let mut sum = _mm256_add_ps(sum0, sum1);
    while i + 8 <= a.len() {
        // SAFETY: bounds checked by loop guard and unaligned loads are permitted.
        let va = unsafe { _mm256_loadu_ps(a.as_ptr().add(i)) };
        // SAFETY: bounds checked by loop guard and unaligned loads are permitted.
        let vb = unsafe { _mm256_loadu_ps(b.as_ptr().add(i)) };
        let d = _mm256_sub_ps(va, vb);
        sum = _mm256_add_ps(sum, _mm256_mul_ps(d, d));
        i += 8;
    }

    let mut lanes = [0f32; 8];
    // SAFETY: writing exactly 8 f32 lanes to a properly sized stack array.
    unsafe { _mm256_storeu_ps(lanes.as_mut_ptr(), sum) };
    let mut out = lanes.iter().sum::<f32>();
    while i < a.len() {
        let d = a[i] - b[i];
        out += d * d;
        i += 1;
    }
    out
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2,fma")]
unsafe fn dot_avx2_fma(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::{
        __m256, _mm256_add_ps, _mm256_fmadd_ps, _mm256_loadu_ps, _mm256_setzero_ps,
        _mm256_storeu_ps,
    };
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::{
        __m256, _mm256_add_ps, _mm256_fmadd_ps, _mm256_loadu_ps, _mm256_setzero_ps,
        _mm256_storeu_ps,
    };

    let mut i = 0usize;
    let mut sum0: __m256 = _mm256_setzero_ps();
    let mut sum1: __m256 = _mm256_setzero_ps();
    while i + 16 <= a.len() {
        // SAFETY: bounds checked by loop guard and unaligned loads are permitted.
        let va0 = unsafe { _mm256_loadu_ps(a.as_ptr().add(i)) };
        // SAFETY: bounds checked by loop guard and unaligned loads are permitted.
        let vb0 = unsafe { _mm256_loadu_ps(b.as_ptr().add(i)) };
        sum0 = _mm256_fmadd_ps(va0, vb0, sum0);

        // SAFETY: bounds checked by loop guard and unaligned loads are permitted.
        let va1 = unsafe { _mm256_loadu_ps(a.as_ptr().add(i + 8)) };
        // SAFETY: bounds checked by loop guard and unaligned loads are permitted.
        let vb1 = unsafe { _mm256_loadu_ps(b.as_ptr().add(i + 8)) };
        sum1 = _mm256_fmadd_ps(va1, vb1, sum1);
        i += 16;
    }
    let mut sum = _mm256_add_ps(sum0, sum1);
    while i + 8 <= a.len() {
        // SAFETY: bounds checked by loop guard and unaligned loads are permitted.
        let va = unsafe { _mm256_loadu_ps(a.as_ptr().add(i)) };
        // SAFETY: bounds checked by loop guard and unaligned loads are permitted.
        let vb = unsafe { _mm256_loadu_ps(b.as_ptr().add(i)) };
        sum = _mm256_fmadd_ps(va, vb, sum);
        i += 8;
    }

    let mut lanes = [0f32; 8];
    // SAFETY: writing exactly 8 f32 lanes to a properly sized stack array.
    unsafe { _mm256_storeu_ps(lanes.as_mut_ptr(), sum) };
    let mut out = lanes.iter().sum::<f32>();
    while i < a.len() {
        out += a[i] * b[i];
        i += 1;
    }
    out
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
unsafe fn dot_avx2(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::{
        __m256, _mm256_add_ps, _mm256_loadu_ps, _mm256_mul_ps, _mm256_setzero_ps, _mm256_storeu_ps,
    };
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::{
        __m256, _mm256_add_ps, _mm256_loadu_ps, _mm256_mul_ps, _mm256_setzero_ps, _mm256_storeu_ps,
    };

    let mut i = 0usize;
    let mut sum0: __m256 = _mm256_setzero_ps();
    let mut sum1: __m256 = _mm256_setzero_ps();
    while i + 16 <= a.len() {
        // SAFETY: bounds checked by loop guard and unaligned loads are permitted.
        let va0 = unsafe { _mm256_loadu_ps(a.as_ptr().add(i)) };
        // SAFETY: bounds checked by loop guard and unaligned loads are permitted.
        let vb0 = unsafe { _mm256_loadu_ps(b.as_ptr().add(i)) };
        sum0 = _mm256_add_ps(sum0, _mm256_mul_ps(va0, vb0));

        // SAFETY: bounds checked by loop guard and unaligned loads are permitted.
        let va1 = unsafe { _mm256_loadu_ps(a.as_ptr().add(i + 8)) };
        // SAFETY: bounds checked by loop guard and unaligned loads are permitted.
        let vb1 = unsafe { _mm256_loadu_ps(b.as_ptr().add(i + 8)) };
        sum1 = _mm256_add_ps(sum1, _mm256_mul_ps(va1, vb1));
        i += 16;
    }
    let mut sum = _mm256_add_ps(sum0, sum1);
    while i + 8 <= a.len() {
        // SAFETY: bounds checked by loop guard and unaligned loads are permitted.
        let va = unsafe { _mm256_loadu_ps(a.as_ptr().add(i)) };
        // SAFETY: bounds checked by loop guard and unaligned loads are permitted.
        let vb = unsafe { _mm256_loadu_ps(b.as_ptr().add(i)) };
        sum = _mm256_add_ps(sum, _mm256_mul_ps(va, vb));
        i += 8;
    }

    let mut lanes = [0f32; 8];
    // SAFETY: writing exactly 8 f32 lanes to a properly sized stack array.
    unsafe { _mm256_storeu_ps(lanes.as_mut_ptr(), sum) };
    let mut out = lanes.iter().sum::<f32>();
    while i < a.len() {
        out += a[i] * b[i];
        i += 1;
    }
    out
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "sse2")]
unsafe fn squared_l2_sse2(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::{
        __m128, _mm_add_ps, _mm_loadu_ps, _mm_mul_ps, _mm_setzero_ps, _mm_storeu_ps, _mm_sub_ps,
    };
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::{
        __m128, _mm_add_ps, _mm_loadu_ps, _mm_mul_ps, _mm_setzero_ps, _mm_storeu_ps, _mm_sub_ps,
    };

    let mut i = 0usize;
    let mut sum0: __m128 = _mm_setzero_ps();
    let mut sum1: __m128 = _mm_setzero_ps();
    while i + 8 <= a.len() {
        // SAFETY: bounds checked by loop guard and unaligned loads are permitted.
        let va0 = unsafe { _mm_loadu_ps(a.as_ptr().add(i)) };
        // SAFETY: bounds checked by loop guard and unaligned loads are permitted.
        let vb0 = unsafe { _mm_loadu_ps(b.as_ptr().add(i)) };
        let d0 = _mm_sub_ps(va0, vb0);
        sum0 = _mm_add_ps(sum0, _mm_mul_ps(d0, d0));

        // SAFETY: bounds checked by loop guard and unaligned loads are permitted.
        let va1 = unsafe { _mm_loadu_ps(a.as_ptr().add(i + 4)) };
        // SAFETY: bounds checked by loop guard and unaligned loads are permitted.
        let vb1 = unsafe { _mm_loadu_ps(b.as_ptr().add(i + 4)) };
        let d1 = _mm_sub_ps(va1, vb1);
        sum1 = _mm_add_ps(sum1, _mm_mul_ps(d1, d1));
        i += 8;
    }
    let mut sum = _mm_add_ps(sum0, sum1);
    while i + 4 <= a.len() {
        // SAFETY: bounds checked by loop guard and unaligned loads are permitted.
        let va = unsafe { _mm_loadu_ps(a.as_ptr().add(i)) };
        // SAFETY: bounds checked by loop guard and unaligned loads are permitted.
        let vb = unsafe { _mm_loadu_ps(b.as_ptr().add(i)) };
        let d = _mm_sub_ps(va, vb);
        sum = _mm_add_ps(sum, _mm_mul_ps(d, d));
        i += 4;
    }

    let mut lanes = [0f32; 4];
    // SAFETY: writing exactly 4 f32 lanes to a properly sized stack array.
    unsafe { _mm_storeu_ps(lanes.as_mut_ptr(), sum) };
    let mut out = lanes.iter().sum::<f32>();
    while i < a.len() {
        let d = a[i] - b[i];
        out += d * d;
        i += 1;
    }
    out
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "sse2")]
unsafe fn dot_sse2(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::{
        __m128, _mm_add_ps, _mm_loadu_ps, _mm_mul_ps, _mm_setzero_ps, _mm_storeu_ps,
    };
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::{
        __m128, _mm_add_ps, _mm_loadu_ps, _mm_mul_ps, _mm_setzero_ps, _mm_storeu_ps,
    };

    let mut i = 0usize;
    let mut sum0: __m128 = _mm_setzero_ps();
    let mut sum1: __m128 = _mm_setzero_ps();
    while i + 8 <= a.len() {
        // SAFETY: bounds checked by loop guard and unaligned loads are permitted.
        let va0 = unsafe { _mm_loadu_ps(a.as_ptr().add(i)) };
        // SAFETY: bounds checked by loop guard and unaligned loads are permitted.
        let vb0 = unsafe { _mm_loadu_ps(b.as_ptr().add(i)) };
        sum0 = _mm_add_ps(sum0, _mm_mul_ps(va0, vb0));

        // SAFETY: bounds checked by loop guard and unaligned loads are permitted.
        let va1 = unsafe { _mm_loadu_ps(a.as_ptr().add(i + 4)) };
        // SAFETY: bounds checked by loop guard and unaligned loads are permitted.
        let vb1 = unsafe { _mm_loadu_ps(b.as_ptr().add(i + 4)) };
        sum1 = _mm_add_ps(sum1, _mm_mul_ps(va1, vb1));
        i += 8;
    }
    let mut sum = _mm_add_ps(sum0, sum1);
    while i + 4 <= a.len() {
        // SAFETY: bounds checked by loop guard and unaligned loads are permitted.
        let va = unsafe { _mm_loadu_ps(a.as_ptr().add(i)) };
        // SAFETY: bounds checked by loop guard and unaligned loads are permitted.
        let vb = unsafe { _mm_loadu_ps(b.as_ptr().add(i)) };
        sum = _mm_add_ps(sum, _mm_mul_ps(va, vb));
        i += 4;
    }

    let mut lanes = [0f32; 4];
    // SAFETY: writing exactly 4 f32 lanes to a properly sized stack array.
    unsafe { _mm_storeu_ps(lanes.as_mut_ptr(), sum) };
    let mut out = lanes.iter().sum::<f32>();
    while i < a.len() {
        out += a[i] * b[i];
        i += 1;
    }
    out
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn squared_l2_neon(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::aarch64::{vaddq_f32, vaddvq_f32, vdupq_n_f32, vld1q_f32, vmlaq_f32, vsubq_f32};

    let mut i = 0usize;
    let mut sum0 = vdupq_n_f32(0.0);
    let mut sum1 = vdupq_n_f32(0.0);
    while i + 8 <= a.len() {
        // SAFETY: bounds checked by loop guard.
        let va0 = unsafe { vld1q_f32(a.as_ptr().add(i)) };
        // SAFETY: bounds checked by loop guard.
        let vb0 = unsafe { vld1q_f32(b.as_ptr().add(i)) };
        let d0 = vsubq_f32(va0, vb0);
        sum0 = vmlaq_f32(sum0, d0, d0);

        // SAFETY: bounds checked by loop guard.
        let va1 = unsafe { vld1q_f32(a.as_ptr().add(i + 4)) };
        // SAFETY: bounds checked by loop guard.
        let vb1 = unsafe { vld1q_f32(b.as_ptr().add(i + 4)) };
        let d1 = vsubq_f32(va1, vb1);
        sum1 = vmlaq_f32(sum1, d1, d1);
        i += 8;
    }
    let mut sum = vaddq_f32(sum0, sum1);
    while i + 4 <= a.len() {
        // SAFETY: bounds checked by loop guard.
        let va = unsafe { vld1q_f32(a.as_ptr().add(i)) };
        // SAFETY: bounds checked by loop guard.
        let vb = unsafe { vld1q_f32(b.as_ptr().add(i)) };
        let d = vsubq_f32(va, vb);
        sum = vmlaq_f32(sum, d, d);
        i += 4;
    }

    let mut out = vaddvq_f32(sum);
    while i < a.len() {
        let d = a[i] - b[i];
        out += d * d;
        i += 1;
    }
    out
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn dot_neon(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::aarch64::{vaddq_f32, vaddvq_f32, vdupq_n_f32, vld1q_f32, vmlaq_f32};

    let mut i = 0usize;
    let mut sum0 = vdupq_n_f32(0.0);
    let mut sum1 = vdupq_n_f32(0.0);
    while i + 8 <= a.len() {
        // SAFETY: bounds checked by loop guard.
        let va0 = unsafe { vld1q_f32(a.as_ptr().add(i)) };
        // SAFETY: bounds checked by loop guard.
        let vb0 = unsafe { vld1q_f32(b.as_ptr().add(i)) };
        sum0 = vmlaq_f32(sum0, va0, vb0);

        // SAFETY: bounds checked by loop guard.
        let va1 = unsafe { vld1q_f32(a.as_ptr().add(i + 4)) };
        // SAFETY: bounds checked by loop guard.
        let vb1 = unsafe { vld1q_f32(b.as_ptr().add(i + 4)) };
        sum1 = vmlaq_f32(sum1, va1, vb1);
        i += 8;
    }
    let mut sum = vaddq_f32(sum0, sum1);
    while i + 4 <= a.len() {
        // SAFETY: bounds checked by loop guard.
        let va = unsafe { vld1q_f32(a.as_ptr().add(i)) };
        // SAFETY: bounds checked by loop guard.
        let vb = unsafe { vld1q_f32(b.as_ptr().add(i)) };
        sum = vmlaq_f32(sum, va, vb);
        i += 4;
    }

    let mut out = vaddvq_f32(sum);
    while i < a.len() {
        out += a[i] * b[i];
        i += 1;
    }
    out
}

#[inline]
fn squared_l2_dispatch() -> BinaryKernel {
    static DISPATCH: OnceLock<BinaryKernel> = OnceLock::new();
    *DISPATCH.get_or_init(resolve_squared_l2_dispatch)
}

#[inline]
fn dot_dispatch() -> BinaryKernel {
    static DISPATCH: OnceLock<BinaryKernel> = OnceLock::new();
    *DISPATCH.get_or_init(resolve_dot_dispatch)
}

#[inline]
fn resolve_squared_l2_dispatch() -> BinaryKernel {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if std::is_x86_feature_detected!("avx2") && std::is_x86_feature_detected!("fma") {
            return squared_l2_avx2_fma_entry;
        }
        if std::is_x86_feature_detected!("avx2") {
            return squared_l2_avx2_entry;
        }
        if std::is_x86_feature_detected!("sse2") {
            return squared_l2_sse2_entry;
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("neon") {
            return squared_l2_neon_entry;
        }
    }
    squared_l2_scalar
}

#[inline]
fn resolve_dot_dispatch() -> BinaryKernel {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if std::is_x86_feature_detected!("avx2") && std::is_x86_feature_detected!("fma") {
            return dot_avx2_fma_entry;
        }
        if std::is_x86_feature_detected!("avx2") {
            return dot_avx2_entry;
        }
        if std::is_x86_feature_detected!("sse2") {
            return dot_sse2_entry;
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("neon") {
            return dot_neon_entry;
        }
    }
    dot_scalar
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[inline]
fn squared_l2_avx2_fma_entry(a: &[f32], b: &[f32]) -> f32 {
    // SAFETY: dispatch resolver only selects this entrypoint when CPU supports avx2+fma.
    unsafe { squared_l2_avx2_fma(a, b) }
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[inline]
fn squared_l2_avx2_entry(a: &[f32], b: &[f32]) -> f32 {
    // SAFETY: dispatch resolver only selects this entrypoint when CPU supports avx2.
    unsafe { squared_l2_avx2(a, b) }
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[inline]
fn dot_avx2_fma_entry(a: &[f32], b: &[f32]) -> f32 {
    // SAFETY: dispatch resolver only selects this entrypoint when CPU supports avx2+fma.
    unsafe { dot_avx2_fma(a, b) }
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[inline]
fn dot_avx2_entry(a: &[f32], b: &[f32]) -> f32 {
    // SAFETY: dispatch resolver only selects this entrypoint when CPU supports avx2.
    unsafe { dot_avx2(a, b) }
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[inline]
fn squared_l2_sse2_entry(a: &[f32], b: &[f32]) -> f32 {
    // SAFETY: dispatch resolver only selects this entrypoint when CPU supports sse2.
    unsafe { squared_l2_sse2(a, b) }
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[inline]
fn dot_sse2_entry(a: &[f32], b: &[f32]) -> f32 {
    // SAFETY: dispatch resolver only selects this entrypoint when CPU supports sse2.
    unsafe { dot_sse2(a, b) }
}

#[cfg(target_arch = "aarch64")]
#[inline]
fn squared_l2_neon_entry(a: &[f32], b: &[f32]) -> f32 {
    // SAFETY: dispatch resolver only selects this entrypoint when CPU supports neon.
    unsafe { squared_l2_neon(a, b) }
}

#[cfg(target_arch = "aarch64")]
#[inline]
fn dot_neon_entry(a: &[f32], b: &[f32]) -> f32 {
    // SAFETY: dispatch resolver only selects this entrypoint when CPU supports neon.
    unsafe { dot_neon(a, b) }
}

pub fn norm2(v: &[f32]) -> f32 {
    dot(v, v)
}

pub fn add_inplace(dst: &mut [f32], src: &[f32]) {
    debug_assert_eq!(dst.len(), src.len());
    for (d, s) in dst.iter_mut().zip(src.iter()) {
        *d += *s;
    }
}

pub fn scale_inplace(dst: &mut [f32], factor: f32) {
    for d in dst {
        *d *= factor;
    }
}

pub fn mean(vectors: &[Vec<f32>], dim: usize) -> Vec<f32> {
    if vectors.is_empty() {
        return vec![0.0; dim];
    }
    let mut out = vec![0.0; dim];
    for v in vectors {
        add_inplace(&mut out, v);
    }
    scale_inplace(&mut out, 1.0 / vectors.len() as f32);
    out
}

pub fn argmin_l2(query: &[f32], centroids: &[Vec<f32>]) -> Option<usize> {
    centroids
        .iter()
        .enumerate()
        .map(|(i, c)| (i, squared_l2(query, c)))
        .min_by(|a, b| a.1.total_cmp(&b.1))
        .map(|(i, _)| i)
}

#[cfg(test)]
mod tests {
    use super::{argmin_l2, dot, mean, squared_l2};

    #[test]
    fn l2_and_dot_are_stable() {
        let a = [1.0, 2.0, 3.0];
        let b = [1.0, 2.5, -1.0];
        assert!((dot(&a, &b) - 3.0).abs() < 1e-5);
        assert!((squared_l2(&a, &b) - 16.25).abs() < 1e-5);
    }

    #[test]
    fn mean_vector_is_correct() {
        let m = mean(&[vec![1.0, 3.0], vec![3.0, 5.0], vec![5.0, 7.0]], 2);
        assert_eq!(m, vec![3.0, 5.0]);
    }

    #[test]
    fn argmin_selects_closest() {
        let query = vec![0.9, 0.1];
        let centroids = vec![vec![0.0, 0.0], vec![1.0, 0.0], vec![0.0, 1.0]];
        assert_eq!(argmin_l2(&query, &centroids), Some(1));
    }

    #[test]
    fn l2_and_dot_match_reference_on_long_vectors() {
        let mut a = Vec::new();
        let mut b = Vec::new();
        for i in 0..513usize {
            a.push((i as f32 * 0.013).sin());
            b.push((i as f32 * 0.017).cos());
        }

        let mut l2_ref = 0.0f32;
        let mut dot_ref = 0.0f32;
        for i in 0..a.len() {
            let d = a[i] - b[i];
            l2_ref += d * d;
            dot_ref += a[i] * b[i];
        }

        let l2 = squared_l2(&a, &b);
        let dp = dot(&a, &b);
        assert!((l2 - l2_ref).abs() < 5e-4, "l2 mismatch: {l2} vs {l2_ref}");
        assert!(
            (dp - dot_ref).abs() < 5e-4,
            "dot mismatch: {dp} vs {dot_ref}"
        );
    }
}

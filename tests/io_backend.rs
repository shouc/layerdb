use layerdb::io::{IoBackend, UringExecutor};

#[test]
fn uring_backend_falls_back_without_native_feature() {
    let io = UringExecutor::with_backend(8, IoBackend::Uring);

    #[cfg(all(feature = "native-uring", target_os = "linux"))]
    {
        assert_eq!(io.backend(), IoBackend::Uring);
    }

    #[cfg(not(all(feature = "native-uring", target_os = "linux")))]
    {
        assert_eq!(io.backend(), IoBackend::Blocking);
    }
}

#[test]
fn native_uring_support_flag_matches_cfg() {
    #[cfg(all(feature = "native-uring", target_os = "linux"))]
    assert!(UringExecutor::supports_native_uring());

    #[cfg(not(all(feature = "native-uring", target_os = "linux")))]
    assert!(!UringExecutor::supports_native_uring());
}

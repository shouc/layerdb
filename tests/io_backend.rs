use layerdb::io::{IoBackend, UringExecutor};
use layerdb::DbOptions;

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

#[test]
fn io_backend_default_is_platform_aware() {
    #[cfg(target_os = "macos")]
    {
        assert_eq!(IoBackend::default(), IoBackend::Kqueue);
        assert_eq!(DbOptions::default().io_backend, IoBackend::Kqueue);
    }

    #[cfg(not(target_os = "macos"))]
    {
        assert_eq!(IoBackend::default(), IoBackend::Uring);
        assert_eq!(DbOptions::default().io_backend, IoBackend::Uring);
    }

    #[cfg(target_os = "linux")]
    {
        assert!(DbOptions::default().sst_use_io_executor_reads);
        assert!(DbOptions::default().sst_use_io_executor_writes);
    }

    #[cfg(not(target_os = "linux"))]
    {
        assert!(!DbOptions::default().sst_use_io_executor_reads);
        assert!(!DbOptions::default().sst_use_io_executor_writes);
    }

    let io = UringExecutor::default();
    assert_eq!(io.backend(), DbOptions::default().io_backend);
}

#[test]
fn uring_executor_roundtrip_when_available() {
    let io = UringExecutor::with_backend(8, IoBackend::Uring);
    if io.backend() != IoBackend::Uring {
        return;
    }

    let dir = tempfile::TempDir::new().expect("tempdir");
    let path = dir.path().join("roundtrip.bin");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&path)
        .expect("open");

    io.write_all_at_file_blocking(&file, 0, b"hello")
        .expect("write");
    io.sync_file_file_blocking(&file).expect("sync");

    let mut out = [0u8; 5];
    io.read_into_at_file_blocking(&file, 0, &mut out)
        .expect("read");
    assert_eq!(&out, b"hello");
}

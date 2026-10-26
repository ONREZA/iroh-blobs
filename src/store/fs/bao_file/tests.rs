use super::*;

#[test]
fn finished_import_restores_poisoned_handle_and_notifies_observers() -> io::Result<()> {
    let (sender, mut observer) = watch::channel(BaoFileStorage::Poisoned);
    let handle = BaoFileHandle(sender);
    let data = Bytes::from_static(b"verified imported data");
    assert!(DataReader(handle.clone())
        .read_bytes_at(0, data.len())
        .is_err());

    handle.complete(MemOrFile::Mem(data.clone()), MemOrFile::empty(), true);

    assert!(observer.has_changed().unwrap());
    assert_eq!(
        observer.borrow_and_update().bitfield(),
        Bitfield::complete(data.len() as u64)
    );
    assert_eq!(DataReader(handle).read_bytes_at(0, data.len())?, data);
    Ok(())
}

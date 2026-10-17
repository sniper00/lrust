use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use tokio::sync::mpsc;
type Reply = (u8, i64, isize);
static RECEIVERS: LazyLock<Mutex<HashMap<u32, mpsc::UnboundedSender<Reply>>>> =
    LazyLock::new(Default::default);
pub(crate) fn subscribe(owner: u32) -> mpsc::UnboundedReceiver<Reply> {
    let (tx, rx) = mpsc::unbounded_channel();
    assert!(RECEIVERS.lock().unwrap().insert(owner, tx).is_none());
    rx
}
pub(crate) fn unsubscribe(owner: u32) {
    RECEIVERS.lock().unwrap().remove(&owner);
}
#[unsafe(no_mangle)]
unsafe extern "C-unwind" fn send_integer_message(
    protocol: u8,
    owner: u32,
    session: i64,
    value: isize,
) {
    RECEIVERS
        .lock()
        .unwrap()
        .get(&owner)
        .expect("unexpected host message; use a Moon integration test")
        .send((protocol, session, value))
        .unwrap();
}
#[unsafe(no_mangle)]
unsafe extern "C-unwind" fn send_message(_: u8, _: u32, _: i64, _: *const i8, _: usize) {
    panic!("byte message delivery requires the Moon integration tests");
}

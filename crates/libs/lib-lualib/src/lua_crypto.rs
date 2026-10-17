use anyhow::{Result, anyhow};
use lib_lua::ffi;
use lib_lua::laux::LuaStack;
use lib_lua::{cstr, laux::LuaState, luaL_newlib};
use lib_lua::{
    laux::{self},
    lreg_null, lreg_try,
};
use ring::aead::{
    AES_128_GCM, AES_256_GCM, Aad, BoundKey, NONCE_LEN, Nonce, NonceSequence, OpeningKey,
    SealingKey, UnboundKey,
};
use ring::error::Unspecified;
use ring::rand::{SecureRandom, SystemRandom};
const AES_128_KEY_LEN: usize = 16;
const AES_256_KEY_LEN: usize = 32;
struct OneNonceSequence(Option<Nonce>);
impl OneNonceSequence {
    fn new(nonce: Nonce) -> Self {
        Self(Some(nonce))
    }
}
impl NonceSequence for OneNonceSequence {
    fn advance(&mut self) -> Result<Nonce, Unspecified> {
        self.0.take().ok_or(Unspecified)
    }
}
/// AES-GCM encryption
/// Parameters: data (string), key (string), nonce (string, optional)
/// Returns: encrypted_data (string), nonce (string)
fn aes_encrypt_imp(lua: &LuaStack<'_>) -> Result<i32> {
    let state = lua.state();
    let data = lua
        .value(1)
        .as_bytes()
        .ok_or_else(|| anyhow!("string expected"))?;
    let key = lua
        .value(2)
        .as_bytes()
        .ok_or_else(|| anyhow!("key string expected"))?;
    if key.len() != AES_256_KEY_LEN && key.len() != AES_128_KEY_LEN {
        return Err(anyhow!(
            "Key must be 32 bytes for AES-256 or 16 bytes for AES-128"
        ));
    }
    let mut nonce_bytes = [0u8; NONCE_LEN];
    let rng = SystemRandom::new();
    let nonce = if let Some(nonce_str) = lua.value(3).as_bytes() {
        if nonce_str.len() != NONCE_LEN {
            return Err(anyhow!("Nonce must be 12 bytes"));
        }
        nonce_bytes.copy_from_slice(nonce_str);
        Nonce::try_assume_unique_for_key(&nonce_bytes).unwrap()
    } else {
        rng.fill(&mut nonce_bytes)
            .map_err(|_| anyhow!("Failed to generate random nonce"))?;
        Nonce::try_assume_unique_for_key(&nonce_bytes).unwrap()
    };
    let nonce_sequence = OneNonceSequence::new(nonce);
    let mut sealing_key = if key.len() == AES_128_KEY_LEN {
        SealingKey::new(UnboundKey::new(&AES_128_GCM, key).unwrap(), nonce_sequence)
    } else {
        SealingKey::new(UnboundKey::new(&AES_256_GCM, key).unwrap(), nonce_sequence)
    };
    let mut in_out = data.to_vec();
    let tag = sealing_key
        .seal_in_place_separate_tag(Aad::empty(), &mut in_out)
        .map_err(|err| anyhow!(format!("Encryption failed: {0}", err)))?;
    in_out.extend_from_slice(tag.as_ref());
    laux::lua_push(state, in_out.as_slice());
    laux::lua_push(state, nonce_bytes.as_slice());
    Ok(2)
}
fn aes_encrypt(lua: &mut LuaStack<'_>) -> std::result::Result<i32, String> {
    aes_encrypt_imp(lua).map_err(|error| error.to_string())
}
/// AES-GCM decryption
/// Parameters: encrypted_data (string), key (string), nonce (string)
/// Returns: decrypted_data (string)
fn aes_decrypt_imp(lua: &LuaStack<'_>) -> Result<i32> {
    let state = lua.state();
    let encrypted_data = lua
        .value(1)
        .as_bytes()
        .ok_or_else(|| anyhow!("string expected"))?;
    let key = lua
        .value(2)
        .as_bytes()
        .ok_or_else(|| anyhow!("key string expected"))?;
    let nonce_bytes = lua
        .value(3)
        .as_bytes()
        .ok_or_else(|| anyhow!("nonce string expected"))?;
    if key.len() != AES_256_KEY_LEN && key.len() != AES_128_KEY_LEN {
        return Err(anyhow!(
            "Key must be 32 bytes for AES-256 or 16 bytes for AES-128"
        ));
    }
    if nonce_bytes.len() != NONCE_LEN {
        return Err(anyhow!("Nonce must be 12 bytes"));
    }
    if encrypted_data.len() < 16 {
        return Err(anyhow!("Encrypted data too short"));
    }
    let nonce = Nonce::try_assume_unique_for_key(nonce_bytes).unwrap();
    let nonce_sequence = OneNonceSequence::new(nonce);
    let mut opening_key = if key.len() == AES_128_KEY_LEN {
        OpeningKey::new(UnboundKey::new(&AES_128_GCM, key).unwrap(), nonce_sequence)
    } else {
        OpeningKey::new(UnboundKey::new(&AES_256_GCM, key).unwrap(), nonce_sequence)
    };
    let mut in_out = encrypted_data.to_vec();
    let plaintext = opening_key
        .open_in_place(Aad::empty(), &mut in_out)
        .map_err(|err| anyhow!(format!("Decryption failed: {}", err)))?;
    laux::lua_push(state, plaintext as &[u8]);
    Ok(1)
}
fn aes_decrypt(lua: &mut LuaStack<'_>) -> std::result::Result<i32, String> {
    aes_decrypt_imp(lua).map_err(|error| error.to_string())
}
/// Generate random key
/// Parameters: key_size (number, optional, default: 32)
/// Returns: key (string)
fn generate_key(lua: &mut laux::LuaStack<'_>) -> std::result::Result<i32, String> {
    let state = lua.state();
    let key_size = lua.opt(1).unwrap_or(32);
    if key_size == 0 || key_size > 64 {
        return Err("Key size must be between 1 and 64 bytes".to_string().into());
    }
    let rng = SystemRandom::new();
    let mut key = vec![0u8; key_size];
    Ok(match rng.fill(&mut key) {
        Ok(_) => {
            laux::lua_push(state, key.as_slice());
            1
        }
        Err(_) => {
            return Err("Failed to generate random key".to_string().into());
        }
    })
}
/// Generate random nonce
/// Returns: nonce (string)
fn generate_nonce(lua: &mut laux::LuaStack<'_>) -> std::result::Result<i32, String> {
    let state = lua.state();
    let rng = SystemRandom::new();
    let mut nonce = [0u8; 12];
    Ok(match rng.fill(&mut nonce) {
        Ok(_) => {
            laux::lua_push(state, nonce.as_slice());
            1
        }
        Err(_) => {
            return Err("Failed to generate random nonce".to_string().into());
        }
    })
}
#[unsafe(no_mangle)]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C-unwind" fn luaopen_rust_crypto(state: LuaState) -> i32 {
    let l = [
        lreg_try!("aes_encrypt", aes_encrypt),
        lreg_try!("aes_decrypt", aes_decrypt),
        lreg_try!("generate_key", generate_key),
        lreg_try!("generate_nonce", generate_nonce),
        lreg_null!(),
    ];
    luaL_newlib!(state, l);
    1
}

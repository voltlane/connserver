use chacha20poly1305::{
    aead::{Aead, OsRng},
    AeadCore, KeyInit, XChaCha20Poly1305, XNonce,
};

const APP_SALT: &[u8] = b"LUUK-AND-LION";

pub fn generate_keypair() -> (
    k256::ecdh::EphemeralSecret, /* private key */
    k256::EncodedPoint,          /* public key */
) {
    let private_key = k256::ecdh::EphemeralSecret::random(&mut OsRng);
    let public_key = k256::EncodedPoint::from(private_key.public_key());
    (private_key, public_key)
}

pub fn generate_shared_secret(
    their_pubkey: &k256::PublicKey,
    my_privkey: &k256::ecdh::EphemeralSecret,
) -> k256::ecdh::SharedSecret {
    my_privkey.diffie_hellman(their_pubkey)
}

pub fn encrypt(key: &k256::ecdh::SharedSecret, data: Vec<u8>) -> Vec<u8> {
    let hkdf = key.extract::<k256::sha2::Sha256>(Some(APP_SALT));
    let mut okm = [0u8; 32];
    hkdf.expand(APP_SALT, &mut okm).unwrap();
    let cipher = XChaCha20Poly1305::new(&okm.into());
    let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
    let mut result = cipher.encrypt(&nonce, data.as_ref()).unwrap();
    result.extend_from_slice(&nonce);
    result
}

pub fn decrypt(
    key: &k256::ecdh::SharedSecret,
    data: Vec<u8>,
) -> Result<Vec<u8>, chacha20poly1305::Error> {
    let hkdf = key.extract::<k256::sha2::Sha256>(Some(APP_SALT));
    let mut okm = [0u8; 32];
    hkdf.expand(APP_SALT, &mut okm).unwrap();
    let cipher = XChaCha20Poly1305::new(&okm.into());
    // 192 bit for the nonce
    let (msg, nonce) = data.split_at(data.len() - 24);
    let nonce = XNonce::from_slice(&nonce);
    cipher.decrypt(nonce, msg)
}

pub fn pubkey_from_bytes(bytes: &[u8]) -> k256::PublicKey {
    k256::PublicKey::from_sec1_bytes(bytes).unwrap()
}

/// Easy to use API for encryption/decryption
///
/// To use this, create a new `Keys` instance, then call `create_encryption`
/// once you have the other party's public key. This will give you an `Encryption`
/// instance, which you can use to encrypt and decrypt messages to/from that party.
pub mod easy {
    pub type PubKey = k256::PublicKey;

    pub fn pubkey_from_bytes(bytes: &[u8]) -> Result<PubKey, k256::elliptic_curve::Error> {
        k256::PublicKey::from_sec1_bytes(bytes)
    }

    pub struct Keys {
        pub pubkey: k256::PublicKey,
        privkey: k256::ecdh::EphemeralSecret,
    }

    impl Keys {
        pub fn new() -> Self {
            let (privkey, pubkey) = super::generate_keypair();
            Self {
                pubkey: k256::PublicKey::from_sec1_bytes(pubkey.as_bytes()).unwrap(),
                privkey,
            }
        }

        /// Create
        pub fn create_encryption(&self, their_pubkey: &PubKey) -> Encryption {
            let shared_secret = super::generate_shared_secret(&their_pubkey, &self.privkey);
            Encryption { shared_secret }
        }

        pub fn pubkey_to_bytes(&self) -> Vec<u8> {
            self.pubkey.to_sec1_bytes().to_vec()
        }
    }

    pub struct Encryption {
        shared_secret: k256::ecdh::SharedSecret,
    }

    impl Encryption {
        // Hi! there's no ::new! Make one via Keys::into_encryption

        pub fn encrypt(&self, data: Vec<u8>) -> Vec<u8> {
            super::encrypt(&self.shared_secret, data)
        }

        pub fn decrypt(&self, data: Vec<u8>) -> Result<Vec<u8>, chacha20poly1305::Error> {
            super::decrypt(&self.shared_secret, data)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encrypt_decrypt() {
        let (my_privkey, my_pubkey) = generate_keypair();
        let (their_privkey, their_pubkey) = generate_keypair();

        let their_pubkey = k256::PublicKey::from_sec1_bytes(&their_pubkey.as_bytes()).unwrap();

        let my_shared_secret = generate_shared_secret(&their_pubkey, &my_privkey);
        let data = b"Hello, world!".to_vec();

        let my_pubkey = k256::PublicKey::from_sec1_bytes(&my_pubkey.as_bytes()).unwrap();
        let their_shared_secret = generate_shared_secret(&my_pubkey, &their_privkey);

        let encrypted_data = encrypt(&my_shared_secret, data.clone());
        let decrypted_data = decrypt(&their_shared_secret, encrypted_data.clone()).unwrap();

        assert_eq!(data, decrypted_data);
    }

    #[test]
    fn test_easy_encrypt_decrypt() {
        let keys = easy::Keys::new();
        let their_keys = easy::Keys::new();
        let data = b"Hello, world!".to_vec();

        let encryption = keys.create_encryption(&their_keys.pubkey);
        let encrypted_data = encryption.encrypt(data.clone());

        let their_encryption = their_keys.create_encryption(&keys.pubkey);
        let decrypted_data = their_encryption.decrypt(encrypted_data).unwrap();

        assert_eq!(data, decrypted_data);
    }
}

use uuid::Uuid;

const CURRENT_FORMAT_VERSION: u32 = 1;
const CHECKSUM_BYTES: usize = 32;

pub fn is_valid_format_version(format_version: u32) -> bool {
    format_version > 0 && format_version <= CURRENT_FORMAT_VERSION
}

pub fn is_valid_keeper_id(keeper_id: &str) -> bool {
    match Uuid::try_parse(keeper_id) {
        Ok(uuid) => !uuid.is_nil(),
        Err(_) => false,
    }
}

pub fn is_valid_checksum(checksum: &str) -> bool {
    if checksum.len() != CHECKSUM_BYTES * 2 {
        return false;
    }

    checksum.chars().all(|c| c.is_ascii_hexdigit())
}

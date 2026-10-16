/**
 * NexusPulse persistence layer: table-driven CRC-32 (IEEE 802.3).
 *
 * Reflected algorithm with polynomial 0xEDB88320, initial value 0xFFFFFFFF and
 * final XOR 0xFFFFFFFF. This is the checksum used by zlib, PNG and gzip.
 */

const TABLE: Uint32Array = (() => {
  const table = new Uint32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) {
      c = (c & 1) !== 0 ? (0xedb88320 ^ (c >>> 1)) >>> 0 : c >>> 1;
    }
    table[n] = c >>> 0;
  }
  return table;
})();

/**
 * Computes the CRC-32 (IEEE, reflected, polynomial 0xEDB88320) of a byte array.
 *
 * The function can be chained: pass the result of a previous call as `seed` to
 * continue a checksum over a following chunk, so that
 * `crc32(b, crc32(a)) === crc32(concat(a, b))`.
 *
 * @param bytes - The bytes to checksum.
 * @param seed - A previous CRC-32 value to continue from (unsigned 32-bit
 *   integer). Defaults to 0, which starts a fresh checksum.
 * @returns The checksum as an unsigned 32-bit number in the range
 *   `0..0xFFFFFFFF`.
 * @throws {TypeError} If `bytes` is not a Uint8Array or `seed` is not an
 *   unsigned 32-bit integer.
 */
export function crc32(bytes: Uint8Array, seed = 0): number {
  if (!(bytes instanceof Uint8Array)) {
    throw new TypeError('crc32: bytes must be a Uint8Array');
  }
  if (!Number.isInteger(seed) || seed < 0 || seed > 0xffffffff) {
    throw new TypeError('crc32: seed must be an unsigned 32-bit integer');
  }
  let crc = (seed ^ 0xffffffff) >>> 0;
  for (let i = 0; i < bytes.length; i++) {
    const b = bytes[i] as number;
    crc = ((TABLE[(crc ^ b) & 0xff] as number) ^ (crc >>> 8)) >>> 0;
  }
  return (crc ^ 0xffffffff) >>> 0;
}

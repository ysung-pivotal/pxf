package org.greenplum.pxf.service.io;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Implements the binary format specification for Postgres Copy
 */
public class GpCopyWritable implements AutoCloseable {

    private transient DataOutputStream buffer;
    private boolean hasOids;

    /**
     * Opens the stream given an output stream
     */
    public void open(final OutputStream out) throws IOException {
        buffer = new DataOutputStream(new BufferedOutputStream(out, 65536));
        writeHeader();
    }

    /**
     * Writes the PGCOPY Header defined in the specification
     * http://gpdb.docs.pivotal.io/5120/ref_guide/sql_commands/COPY.html#topic1__section10
     */
    private void writeHeader() throws IOException {
        // Signature — 11-byte sequence PGCOPY\n\377\r\n\0 —
        // note that the zero byte is a required part of the
        // signature. (The signature is designed to allow
        // easy identification of files that have been munged
        // by a non-8-bit-clean transfer. This signature will
        // be changed by end-of-line-translation filters,
        // dropped zero bytes, dropped high bits, or parity
        // changes.)
        buffer.writeBytes("PGCOPY\n\377\r\n\0");

        // Flags field — 32-bit integer bit mask to denote
        // important aspects of the file format. Bits are
        // numbered from 0 (LSB) to 31 (MSB). Note that this
        // field is stored in network byte order (most
        // significant byte first), as are all the integer
        // fields used in the file format. Bits 16-31 are
        // reserved to denote critical file format issues;
        // a reader should abort if it finds an unexpected
        // bit set in this range. Bits 0-15 are reserved to
        // signal backwards-compatible format issues; a reader
        // should simply ignore any unexpected bits set in
        // this range. Currently only one flag is defined,
        // and the rest must be zero (Bit 16: 1 if data has
        // OIDs, 0 if not).
        if (hasOids) {
            buffer.writeInt(1 << 16); // Bit 16: 1 - Has OIDs
        } else {
            buffer.writeInt(0); // 0 - No OIDs
        }

        // Header extension area length — 32-bit integer,
        // length in bytes of remainder of header, not
        // including self. Currently, this is zero, and the
        // first tuple follows immediately. Future changes
        // to the format might allow additional data to be
        // present in the header. A reader should silently
        // skip over any header extension data it does not
        // know what to do with. The header extension area
        // is envisioned to contain a sequence of
        // self-identifying chunks. The flags field is not
        // intended to tell readers what is in the extension
        // area. Specific design of header extension contents
        // is left for a later release.
        buffer.writeInt(0);
    }

    private void writeTuple(int fieldCount) throws IOException {
        // Tuples — Each tuple begins with a 16-bit integer
        // count of the number of fields in the tuple.
        // (Presently, all tuples in a table will have the
        // same count, but that might not always be true.)
        buffer.writeShort(fieldCount);

        if (hasOids) {
            // If OIDs are included in the file, the OID field
            // immediately follows the field-count word. It is
            // a normal field except that it is not included in
            // the field-count. In particular it has a length
            // word — this will allow handling of 4-byte vs.
            // 8-byte OIDs without too much pain, and will allow
            // OIDs to be shown as null if that ever proves
            // desirable.

        }

        // Then, repeated for each field in the tuple, there
        // is a 32-bit length word followed by that many
        // bytes of field data. (The length word does not
        // include itself, and can be zero.) As a special case,
        // -1 indicates a NULL field value. No value bytes
        // follow in the NULL case.
        // There is no alignment padding or any other extra
        // data between fields.
    }

    /**
     * Writes the file trailer defined in the specification
     */
    private void writeTrailer() throws IOException {
        // The file trailer consists of a 16-bit integer word
        // containing -1. This is easily distinguished from a
        // tuple's field-count word. A reader should report an
        // error if a field-count word is neither -1 nor the
        // expected number of columns.
        // This provides an extra check against somehow getting
        // out of sync with the data.
        buffer.writeShort(-1);
    }

    /**
     * Closes this resource, relinquishing any underlying resources.
     * This method is invoked automatically on objects managed by the
     * {@code try}-with-resources statement.
     *
     * <p>While this interface method is declared to throw {@code
     * Exception}, implementers are <em>strongly</em> encouraged to
     * declare concrete implementations of the {@code close} method to
     * throw more specific exceptions, or to throw no exception at all
     * if the close operation cannot fail.
     *
     * <p> Cases where the close operation may fail require careful
     * attention by implementers. It is strongly advised to relinquish
     * the underlying resources and to internally <em>mark</em> the
     * resource as closed, prior to throwing the exception. The {@code
     * close} method is unlikely to be invoked more than once and so
     * this ensures that the resources are released in a timely manner.
     * Furthermore it reduces problems that could arise when the resource
     * wraps, or is wrapped, by another resource.
     *
     * <p><em>Implementers of this interface are also strongly advised
     * to not have the {@code close} method throw {@link
     * InterruptedException}.</em>
     * <p>
     * This exception interacts with a thread's interrupted status,
     * and runtime misbehavior is likely to occur if an {@code
     * InterruptedException} is {@linkplain Throwable#addSuppressed
     * suppressed}.
     * <p>
     * More generally, if it would cause problems for an
     * exception to be suppressed, the {@code AutoCloseable.close}
     * method should not throw it.
     *
     * <p>Note that unlike the {@link Closeable#close close}
     * method of {@link Closeable}, this {@code close} method
     * is <em>not</em> required to be idempotent.  In other words,
     * calling this {@code close} method more than once may have some
     * visible side effect, unlike {@code Closeable.close} which is
     * required to have no effect if called more than once.
     * <p>
     * However, implementers of this interface are strongly encouraged
     * to make their {@code close} methods idempotent.
     *
     * @throws Exception if this resource cannot be closed
     */
    @Override
    public void close() throws Exception {
        writeTrailer();
        buffer.close();
    }
}

/*
 * The MIT License
 *
 * Copyright (c) 2009 The Broad Institute
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package bgzf4j;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SyncFailedException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Encapsulates file representation of various primitive data types. Forces little-endian disk
 * representation. Note that this class is currently not very efficient. There are plans to increase
 * the size of the ByteBuffer, and move data between the ByteBuffer and the underlying input or
 * output stream in larger chunks.
 *
 * All the read methods throw RuntimeEOFException if the input stream is exhausted before the
 * required number of bytes are read.
 *
 * @author Dave Tefft
 */
public class BinaryCodec {

    // Outstream to write to
    private OutputStream outputStream;
    // If a file or filename was given it will be stored here. Used for error reporting.
    private String outputFileName;

    // Input stream to read from
    private InputStream inputStream;
    // If a file or filename was give to read from it will be stored here. Used for error reporting.
    private String inputFileName;

    /*
     * Mode that the BinaryCodec is in. It is either writing to a binary file or reading from. This
     * is set to true if it is writing to a binary file Right now we don't support reading and
     * writing to the same file with the same BinaryCodec instance
     */
    private boolean isWriting;

    /**
     * For byte swapping.
     */
    private ByteBuffer byteBuffer;

    /**
     * For reading Strings of known length, this can reduce object creation
     */
    private final byte[] scratchBuffer = new byte[16];

    // Byte order used in BAM files.
    private static final ByteOrder LITTLE_ENDIAN = ByteOrder.LITTLE_ENDIAN;
    private static final byte NULL_BYTE[] = {0};

    private static final long MAX_UBYTE = (Byte.MAX_VALUE * 2) + 1;
    private static final long MAX_USHORT = (Short.MAX_VALUE * 2) + 1;
    private static final long MAX_UINT = ((long)Integer.MAX_VALUE * 2) + 1;

    // We never serialize more than this much at a time (except for Strings)
    private static final int MAX_BYTE_BUFFER = 8;

    // ////////////////////////////////////////////////
    // Constructors //
    // ////////////////////////////////////////////////

    /**
     * Constructs BinaryCodec from a file and set it's mode to writing or not
     *
     * @param file file to be written to or read from
     * @param writing whether the file is being written to
     * @throws FileNotFoundException
     */
    public BinaryCodec(final File file, final boolean writing) throws FileNotFoundException {
        this();

        this.isWriting = writing;
        if (this.isWriting) {
            this.outputStream = new FileOutputStream(file);
            this.outputFileName = file.getName();
        } else {
            this.inputStream = new FileInputStream(file);
            this.inputFileName = file.getName();
        }
    }

    /**
     * Constructs BinaryCodec from a file name and set it's mode to writing or not
     *
     * @param fileName name of the file to be written to or read from
     * @param writing writing whether the file is being written to
     * @throws FileNotFoundException
     */
    public BinaryCodec(final String fileName, final boolean writing) throws FileNotFoundException {
        this(new File(fileName), writing);
    }

    /**
     * Constructs BinaryCodec from an output stream
     *
     * @param outputStream Stream to write to, since it's an output stream we know that isWriting
     *        should be set to true
     */
    public BinaryCodec(final OutputStream outputStream) {
        this();
        setOutputStream(outputStream);
    }

    /**
     * Constructs BinaryCodec from an input stream
     *
     * @param inputStream Stream to read from, since we are reading isWriting is set to false
     */
    public BinaryCodec(final InputStream inputStream) {
        this();
        setInputStream(inputStream);
    }

    /**
     * Ambiguous whether reading or writing until set{In,Out}putStream is called
     */
    public BinaryCodec() {
        initByteBuffer();
    }

    /**
     * Shared among ctors. Note that if endianness is changed, all the unsigned methods must also be
     * changed.
     */
    private void initByteBuffer() {
        byteBuffer = ByteBuffer.allocate(MAX_BYTE_BUFFER);
        byteBuffer.order(LITTLE_ENDIAN);
    }

    // ////////////////////////////////////////////////
    // Writing methods //
    // ////////////////////////////////////////////////


    /**
     * Write whatever has been put into the byte buffer
     *
     * @param numBytes -- how much to write. Note that in case of writing an unsigned value, more
     *        bytes were put into the ByteBuffer than will get written out.
     * @throws IOException
     */
    private void writeByteBuffer(final int numBytes) throws IOException {
        assert (numBytes <= byteBuffer.limit());
        writeBytes(byteBuffer.array(), 0, numBytes);
    }

    /**
     * Writes a byte to the output buffer
     *
     * @param bite byte array to write
     * @throws IOException
     */
    public void writeByte(final byte bite) throws IOException {
        byteBuffer.clear();
        byteBuffer.put(bite);
        writeByteBuffer(1);
    }

    public void writeByte(final int b) throws IOException {
        writeByte((byte)b);
    }

    /**
     * Writes a byte array to the output buffer
     *
     * @param bytes value to write
     * @throws IOException
     */
    public void writeBytes(final byte[] bytes) throws IOException {
        writeBytes(bytes, 0, bytes.length);
    }

    public void writeBytes(final byte[] bytes, final int startOffset, final int numBytes) throws IOException {
        if (!isWriting) {
            throw new IllegalStateException("Calling write method on BinaryCodec open for read.");
        }

        outputStream.write(bytes, startOffset, numBytes);
    }

    /**
     * Write a 32-bit int to the output stream
     *
     * @param value int to write
     * @throws IOException
     */
    public void writeInt(final int value) throws IOException {
        byteBuffer.clear();
        byteBuffer.putInt(value);
        writeByteBuffer(4);
    }

    /**
     * Write a double (8 bytes) to the output stream
     *
     * @param value double to write
     * @throws IOException
     */
    public void writeDouble(final double value) throws IOException {
        byteBuffer.clear();
        byteBuffer.putDouble(value);
        writeByteBuffer(8);
    }

    /**
     * Write a 64-bit long to the output stream
     *
     * @param value long to write
     * @throws IOException
     */
    public void writeLong(final long value) throws IOException {
        byteBuffer.clear();
        byteBuffer.putLong(value);
        writeByteBuffer(8);
    }


    /**
     * Write a 16-bit short to output stream
     *
     * @throws IOException
     */
    public void writeShort(final short value) throws IOException {
        byteBuffer.clear();
        byteBuffer.putShort(value);
        writeByteBuffer(2);
    }

    /**
     * Write a float (4 bytes) to the output stream
     *
     * @param value float to write
     * @throws IOException
     */
    public void writeFloat(final float value) throws IOException {
        byteBuffer.clear();
        byteBuffer.putFloat(value);
        writeByteBuffer(4);
    }

    /**
     * Writes a boolean (1 byte) to the output buffer
     *
     * @param value boolean to write
     * @throws IOException
     */
    public void writeBoolean(final boolean value) throws IOException {
        byteBuffer.clear();
        byteBuffer.put(value ? (byte)1 : (byte)0);
        writeByteBuffer(1);
    }

    /**
     * Writes a string to the buffer as ASCII bytes
     *
     * @param value string to write to buffer
     * @param writeLength prefix the string with the length as a 32-bit int
     * @param appendNull add a null byte to the end of the string
     * @throws IOException
     */
    public void writeString(final String value, final boolean writeLength, final boolean appendNull) throws IOException {
        if (writeLength) {
            int lengthToWrite = value.length();
            if (appendNull) lengthToWrite++;
            writeInt(lengthToWrite);
        }

        // Actually writes the string to a buffer
        writeString(value);

        if (appendNull) writeBytes(NULL_BYTE);

    }


    /**
     * Write a string to the buffer as ASCII bytes
     *
     * @param value string to write
     * @throws IOException
     */
    private void writeString(final String value) throws IOException {
        final byte[] byteBuffer = new byte[value.length()];
        final char[] charBuffer = value.toCharArray();
        for (int i = 0; i < charBuffer.length; ++i) {
            byteBuffer[i] = (byte)(charBuffer[i] & 0xff);
        }
        writeBytes(byteBuffer);
    }

    /**
     * Write an 8-bit unsigned byte. NOTE: This method will break if we change to big-endian.
     *
     * @throws IOException
     */
    public void writeUByte(final short val) throws IOException {
        if (val < 0) {
            throw new IllegalArgumentException("Negative value (" + val
                + ") passed to unsigned writing method.");
        }
        if (val > MAX_UBYTE) {
            throw new IllegalArgumentException("Value (" + val
                + ") to large to be written as ubyte.");
        }
        byteBuffer.clear();
        byteBuffer.putShort(val);
        writeByteBuffer(1);
    }

    /**
     * Write a 16-bit unsigned short. NOTE: This method will break if we change to big-endian.
     *
     * @throws IOException
     */
    public void writeUShort(final int val) throws IOException {
        if (val < 0) {
            throw new IllegalArgumentException("Negative value (" + val
                + ") passed to unsigned writing method.");
        }
        if (val > MAX_USHORT) {
            throw new IllegalArgumentException("Value (" + val
                + ") to large to be written as ushort.");
        }
        byteBuffer.clear();
        byteBuffer.putInt(val);
        writeByteBuffer(2);
    }

    /**
     * Write a 32-bit unsigned int. NOTE: This method will break if we change to big-endian.
     *
     * @throws IOException
     */
    public void writeUInt(final long val) throws IOException {
        if (val < 0) {
            throw new IllegalArgumentException("Negative value (" + val
                + ") passed to unsigned writing method.");
        }
        if (val > MAX_UINT) {
            throw new IllegalArgumentException("Value (" + val
                + ") to large to be written as uint.");
        }
        byteBuffer.clear();
        byteBuffer.putLong(val);
        writeByteBuffer(4);
    }

    // ////////////////////////////////////////////////
    // Reading methods //
    // ////////////////////////////////////////////////

    /**
     * Read a byte array from the input stream.
     *
     * @throws IOException
     */
    public void readBytes(final byte[] buffer) throws IOException {
        readBytes(buffer, 0, buffer.length);
    }

    /**
     * Read a byte array from the input stream
     *
     * @param buffer where to put bytes read
     * @param offset offset to start putting bytes into buffer
     * @param length number of bytes to read
     * @throws IOException
     */
    public void readBytes(final byte[] buffer, final int offset, final int length) throws IOException {
        int totalNumRead = 0;
        do {
            final int numRead =
                readBytesOrFewer(buffer, offset + totalNumRead, length - totalNumRead);
            if (numRead < 0) {
                throw new EOFException(constructErrorMessage("Premature EOF"));
            } else {
                totalNumRead += numRead;
            }
        } while (totalNumRead < length);
    }

    /**
     * Reads a byte array from the input stream.
     *
     * @param buffer where to put bytes read
     * @param offset offset to start putting bytes into buffer
     * @param length number of bytes to read. Fewer bytes may be read if EOF is reached before
     *        length bytes have been read.
     * @return the total number of bytes read into the buffer, or -1 if there is no more data
     *         because the end of the stream has been reached.
     * @throws IOException
     */
    public int readBytesOrFewer(final byte[] buffer, final int offset, final int length) throws IOException {
        if (isWriting) {
            throw new IllegalStateException("Calling read method on BinaryCodec open for write.");
        }

        return inputStream.read(buffer, offset, length);
    }

    /**
     * @return a single byte read from the input stream.
     * @throws IOException
     */
    public byte readByte() throws IOException {
        if (isWriting) {
            throw new IllegalStateException("Calling read method on BinaryCodec open for write.");
        }

        final int ret = inputStream.read();
        if (ret == -1) {
            throw new EOFException(constructErrorMessage("Premature EOF"));
        }
        return (byte)ret;
    }

    /**
     * @return true if it is possible to know for sure if at EOF, and it is known for sure. If the
     *         input stream is a ByteArrayInputStream, this is faster than causing a
     *         RuntimeEOFException to be thrown.
     * @throws IOException
     */
    public boolean knownAtEof() throws IOException {
        if (isWriting) {
            throw new IllegalStateException(
                "Calling knownAtEof method on BinaryCodec open for write.");
        }

        return inputStream instanceof ByteArrayInputStream && inputStream.available() == 0;
    }

    /**
     * Read a string off the input stream, as ASCII bytes
     *
     * @param length length of string to read
     * @return String read from stream
     * @throws IOException
     */
    public String readString(final int length) throws IOException {
        final byte[] buffer;
        // Recycle single buffer if possible
        if (length <= scratchBuffer.length) {
            buffer = scratchBuffer;
        } else {
            buffer = new byte[length];

        }
        readBytes(buffer, 0, length);

        final char[] charBuffer = new char[length];
        for (int i = 0; i < length; ++i) {
            charBuffer[i] = (char)buffer[i];
        }
        return new String(charBuffer);
    }

    /**
     * Read ASCII bytes from the input stream until a null byte is read
     *
     * @return String constructed from the ASCII bytes read
     * @throws IOException
     */
    public String readNullTerminatedString() throws IOException {
        final StringBuilder ret = new StringBuilder();
        for (byte b = this.readByte(); b != 0; b = this.readByte()) {
            ret.append((char)(b & 0xff));
        }
        return ret.toString();
    }

    /**
     * Read an int length, and then a String of that length
     *
     * @param devourNull if true, the length include a null terminator, which is read and discarded
     * @throws IOException
     */
    public String readLengthAndString(final boolean devourNull) throws IOException {
        int length = readInt();
        if (devourNull) {
            --length;
        }
        final String ret = readString(length);
        if (devourNull) {
            readByte();
        }
        return ret;
    }

    private void readByteBuffer(final int numBytes) throws IOException {
        assert (numBytes <= byteBuffer.capacity());
        readBytes(byteBuffer.array(), 0, numBytes);
        byteBuffer.limit(byteBuffer.capacity());
        byteBuffer.position(numBytes);
    }

    /**
     * Read an int off the input stream
     *
     * @return int from input stream
     * @throws IOException
     */
    public int readInt() throws IOException {
        readByteBuffer(4);
        byteBuffer.flip();
        return byteBuffer.getInt();
    }

    /**
     * Reads a double off the input stream
     *
     * @return double
     * @throws IOException
     */
    public double readDouble() throws IOException {
        readByteBuffer(8);
        byteBuffer.flip();
        return byteBuffer.getDouble();
    }

    /**
     * Reads a long off the input stream
     *
     * @return long
     * @throws IOException
     */
    public long readLong() throws IOException {
        readByteBuffer(8);
        byteBuffer.flip();
        return byteBuffer.getLong();
    }

    public short readShort() throws IOException {
        readByteBuffer(2);
        byteBuffer.flip();
        return byteBuffer.getShort();
    }

    /**
     * Reads a float off the input stream
     *
     * @return float
     * @throws IOException
     */
    public float readFloat() throws IOException {
        readByteBuffer(4);
        byteBuffer.flip();
        return byteBuffer.getFloat();
    }

    /**
     * Reads a boolean off the input stream, represented as a byte with value 1 or 0
     *
     * @return boolean
     * @throws IOException
     */
    public boolean readBoolean() throws IOException {
        return ((readByte()) == 1);
    }

    /**
     * Reads an 8-bit unsigned byte from the input stream. This method assumes little-endianness.
     *
     * @throws IOException
     */
    public short readUByte() throws IOException {
        readByteBuffer(1);
        byteBuffer.put((byte)0);
        byteBuffer.flip();
        return byteBuffer.getShort();
    }

    /**
     * Reads a 16-bit unsigned short from the input stream. This method assumes little-endianness.
     *
     * @throws IOException
     */
    public int readUShort() throws IOException {
        readByteBuffer(2);
        byteBuffer.putShort((short)0);
        byteBuffer.flip();
        return byteBuffer.getInt();
    }

    /**
     * Reads a 32-bit unsigned int from the input stream. This method assumes little-endianness.
     *
     * @throws IOException
     */
    public long readUInt() throws IOException {
        readByteBuffer(4);
        byteBuffer.putInt(0);
        byteBuffer.flip();
        return byteBuffer.getLong();
    }

    /**
     * Close the appropriate stream
     *
     * @throws IOException
     */
    public void close() throws IOException {
        if (this.isWriting) {
            // To the degree possible, make sure the bytes get forced to the file system,
            // or else cause an exception to be thrown.
            if (this.outputStream instanceof FileOutputStream) {
                this.outputStream.flush();
                FileOutputStream fos = (FileOutputStream)this.outputStream;
                try {
                    fos.getFD().sync();
                } catch (SyncFailedException e) {
                    // Since the sync is belt-and-suspenders anyway, don't throw an exception if it
                    // fails,
                    // because on some OSs it will fail for some types of output. E.g. writing to
                    // /dev/null
                    // on some Unixes.
                }
            }
            this.outputStream.close();
        } else {
            this.inputStream.close();
        }
    }

    private String constructErrorMessage(final String msg) {
        final StringBuilder sb = new StringBuilder(msg);
        sb.append("; BinaryCodec in ");
        sb.append(isWriting ? "write" : "read");
        sb.append("mode; ");
        final String filename = isWriting ? outputFileName : inputFileName;
        if (filename != null) {
            sb.append("file: ");
            sb.append(filename);
        } else {
            sb.append("streamed file (filename not available)");
        }
        return sb.toString();
    }

    // ////////////////////////////////////////////////
    // Some getters //
    // ////////////////////////////////////////////////


    public String getInputFileName() {
        return inputFileName;
    }

    public String getOutputFileName() {
        return outputFileName;
    }

    public void setOutputFileName(final String outputFileName) {
        this.outputFileName = outputFileName;
    }

    public void setInputFileName(final String inputFileName) {
        this.inputFileName = inputFileName;
    }

    public boolean isWriting() {
        return isWriting;
    }

    public OutputStream getOutputStream() {
        return outputStream;
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    public void setInputStream(final InputStream is) {
        isWriting = false;
        this.inputStream = is;
    }

    public void setOutputStream(final OutputStream os) {
        isWriting = true;
        this.outputStream = os;
    }
}

package com.fasterxml.transistore.basic;

import com.fasterxml.storemate.shared.*;
import com.fasterxml.storemate.shared.hash.*;
import com.fasterxml.storemate.shared.util.UTF8Encoder;
import com.fasterxml.storemate.shared.util.WithBytesCallback;

import com.fasterxml.clustermate.api.DecodableRequestPath;
import com.fasterxml.clustermate.api.EntryKeyConverter;
import com.fasterxml.clustermate.api.RequestPathBuilder;

/**
 * Default {@link EntryKeyConverter} used with Basic TransiStore types.
 *<p>
 * In addition class implements encoding and decoding of the entry keys
 * (of type {@link BasicTSKey}): uses {@link BasicTSConstants#TS_QUERY_PARAM_PARTITION_ID}
 * in addition to path
 * (which is regular filename).
 */
public class BasicTSKeyConverter
    extends EntryKeyConverter<BasicTSKey>
{
    /**
     * Keys have 2-byte fixed prefix which just contains length of partition id.
     */
    public final static int DEFAULT_KEY_HEADER_LENGTH = 2;

    public final static int MAX_PARTITION_ID_BYTE_LENGTH = 0x7FFF;
    
    /**
     * Object we use for calculating high-quality hash codes, both for routing (for keys)
     * and for content.
     */
    protected final BlockHasher32 _hasher;

    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */

    protected BasicTSKeyConverter() {
        this(new BlockMurmur3Hasher());
    }
    
    protected BasicTSKeyConverter(BlockHasher32 blockHasher) {
        _hasher = blockHasher;
    }
    
    /**
     * Accessor for getting default converter instance.
     */
    public static BasicTSKeyConverter defaultInstance() {
        return new BasicTSKeyConverter();
    }

    /*
    /**********************************************************************
    /* Basic EntryKeyConverter impl
    /**********************************************************************
     */

    @Override
    public BasicTSKey construct(byte[] rawKey) {
        return rawToEntryKey(new StorableKey(rawKey));
    }

    @Override
    public BasicTSKey construct(byte[] rawKey, int offset, int length) {
        return rawToEntryKey(new StorableKey(rawKey, offset, length));
    }
    
    @Override
    public BasicTSKey rawToEntryKey(final StorableKey rawKey) {
        return rawKey.with(new WithBytesCallback<BasicTSKey>() {
            @Override
            public BasicTSKey withBytes(byte[] buffer, int offset, int length) {
                int partitionIdLength = ((buffer[offset] & 0xFF) << 8)
                        | (buffer[offset+1] & 0xFF);
                return new BasicTSKey(rawKey, partitionIdLength);
            }
        });
    }
    /**
     * Method called to figure out raw hash code to use for routing request
     * regarding given content key.
     */
    @Override
    public int routingHashFor(BasicTSKey key) {
        return _truncateHash(rawHashForRouting(key, _hasher));
    }

    /*
    /**********************************************************************
    /* VKey construction, conversions
    /**********************************************************************
     */

    /**
     * Method called to construct a {@link BasicTSKey}
     * that does not have group id.
     */
    public BasicTSKey construct(String path) {
        return construct(path, 0);
    }
    
    /**
     * Method called to construct an instance given concatenated key (consisting
     * of partition id followed by path), and length indicator for splitting
     * parts as necessary.
     * 
     * @param fullKey Full concatenated id.
     * @param partitionIdLengthInBytes Length of partition id included before path
     * 
     * @return Constructed key
     */
    public BasicTSKey construct(String fullKey, int partitionIdLengthInBytes)
    {
        if (partitionIdLengthInBytes > MAX_PARTITION_ID_BYTE_LENGTH) {
            throw new IllegalArgumentException("Partition id byte length too long ("+partitionIdLengthInBytes
                    +"), can not exceed "+MAX_PARTITION_ID_BYTE_LENGTH);
        }
        byte[] b = UTF8Encoder.encodeAsUTF8(fullKey, DEFAULT_KEY_HEADER_LENGTH, 0, false);
        // group id length is a positive 16-bit short, MSB:
        b[0] = (byte) (partitionIdLengthInBytes >> 8);
        b[1] = (byte) partitionIdLengthInBytes;

        return new BasicTSKey(new StorableKey(b), partitionIdLengthInBytes);
    }

    /**
     * Method called to construct a {@link BasicTSKey} given a two-part
     * path; partition id as prefix, and additional path contextual path.
     */
    public BasicTSKey construct(String partitionId, String path)
    {
        // sanity check for "no group id" case
        if (partitionId == null || partitionId.length() == 0) {
            return construct(path);
        }
        
        byte[] prefixPart = UTF8Encoder.encodeAsUTF8(partitionId, DEFAULT_KEY_HEADER_LENGTH, 0, false);
        final int partitionIdLengthInBytes = prefixPart.length - DEFAULT_KEY_HEADER_LENGTH;
        if (partitionIdLengthInBytes > MAX_PARTITION_ID_BYTE_LENGTH) {
            throw new IllegalArgumentException("Partition id byte length too long ("+partitionIdLengthInBytes
                    +"), can not exceed "+MAX_PARTITION_ID_BYTE_LENGTH);
        }
        prefixPart[0] = (byte) (partitionIdLengthInBytes >> 8);
        prefixPart[1] = (byte) partitionIdLengthInBytes;
        
        // so far so good: and now append actual path
        byte[] fullKey = UTF8Encoder.encodeAsUTF8(prefixPart, path);
        return new BasicTSKey(new StorableKey(fullKey), partitionIdLengthInBytes);
    }

    public StorableKey storableKey(String fullPath, int partitionIdLengthInBytes)
    {
        if (partitionIdLengthInBytes > MAX_PARTITION_ID_BYTE_LENGTH) {
            throw new IllegalArgumentException("Partition id byte length too long ("+partitionIdLengthInBytes
                    +"), can not exceed "+MAX_PARTITION_ID_BYTE_LENGTH);
        }
        byte[] b = UTF8Encoder.encodeAsUTF8(fullPath, DEFAULT_KEY_HEADER_LENGTH, 0, false);
        // group id length is a positive 16-bit short, MSB:
        b[0] = (byte) (partitionIdLengthInBytes >> 8);
        b[1] = (byte) partitionIdLengthInBytes;
        return new StorableKey(b);
    }

    /*
    /**********************************************************************
    /* Path handling
    /**********************************************************************
     */
    
    @SuppressWarnings("unchecked")
    @Override
    public <B extends RequestPathBuilder> B appendToPath(B b, BasicTSKey key)
    {
        String path = key.getExternalPath();
        // Partition id: could be added as path segment; but for now we'll use query param instead
        int partitionIdLen = key.getPartitionIdLength();
        if (partitionIdLen > 0) {
            String partitionId = key.getPartitionId();
            b = (B) b.addParameter(BasicTSConstants.TS_QUERY_PARAM_PARTITION_ID, partitionId);
            // and then remove partition id to leave path
            path = path.substring(partitionId.length());
        }
        // also: while not harmful, let's avoid escaping embedded slashes (slightly more compact)
        b = (B) b.addPathSegmentsRaw(path);
        return b;
    }

    @Override
    public <P extends DecodableRequestPath> BasicTSKey extractFromPath(P path)
    {
        final String partitionId = path.getQueryParameter(BasicTSConstants.TS_QUERY_PARAM_PARTITION_ID);
        // but ignore empty one
        final String filename = path.getDecodedPath();
        if (partitionId != null) {
            if (partitionId.length() > 0) {
                return construct(partitionId, filename);
            }
        }
        return construct(filename);
    }
    
    /*
    /**********************************************************************
    /* Hash code calculation
    /**********************************************************************
     */

    protected int rawHashForRouting(BasicTSKey key, BlockHasher32 hasher)
    {
        // first: skip metadata (group id length indicator)
        int offset = BasicTSKeyConverter.DEFAULT_KEY_HEADER_LENGTH;
        StorableKey rawKey = key.asStorableKey();
        int length = rawKey.length() - offset;
        // second include only group id (if got one), or full thing
        if (key.hasPartitionId()) {
            length = key.getPartitionIdLength();
        }
        return rawKey.hashCode(hasher, offset, length);
    }

    @Override
    public int contentHashFor(ByteContainer bytes) {
        return bytes.hash(_hasher, BlockHasher32.DEFAULT_SEED);
    }

    @Override
    public IncrementalHasher32 createStreamingContentHasher() {
        return new IncrementalMurmur3Hasher();
    }
}

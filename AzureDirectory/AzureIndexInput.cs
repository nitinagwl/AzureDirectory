using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Threading;

namespace Lucene.Net.Store.Azure
{
    /// <summary>
    /// Implements IndexInput semantics for a read only blob
    /// </summary>
    public class AzureIndexInput : IndexInput
    {
        private AzureDirectory _azureDirectory;
        private CloudBlobContainer _blobContainer;
        private ICloudBlob _blob;
        private readonly string _name;

        private IndexInput _indexInput;
        private readonly Mutex _fileMutex;

        public Lucene.Net.Store.Directory CacheDirectory => _azureDirectory.CacheDirectory;

        public AzureIndexInput(AzureDirectory azuredirectory, ICloudBlob blob)
        {
            _name = blob.Uri.Segments[blob.Uri.Segments.Length - 1];

#if FULLDEBUG
            Debug.WriteLine($"opening {_name} ");
#endif
            _fileMutex = BlobMutexManager.GrabMutex(_name);
            _fileMutex.WaitOne();
            try
            {
                _azureDirectory = azuredirectory;
                _blobContainer = azuredirectory.BlobContainer;
                _blob = blob;

                var fileName = _name;

                var fFileNeeded = false;
                if (!CacheDirectory.FileExists(fileName))
                {
                    fFileNeeded = true;
                }
                else
                {
                    long cachedLength = CacheDirectory.FileLength(fileName);
                    string blobLengthMetadata;
                    bool hasMetadataValue = blob.Metadata.TryGetValue("CachedLength", out blobLengthMetadata); 
                    long blobLength = blob.Properties.Length;
                    if (hasMetadataValue) long.TryParse(blobLengthMetadata, out blobLength);

                    string blobLastModifiedMetadata;
                    DateTime blobLastModifiedUtc = blob.Properties.LastModified?.UtcDateTime ?? DateTime.MinValue;
                    if (blob.Metadata.TryGetValue("CachedLastModified", out blobLastModifiedMetadata))
                    {
                        long longLastModified = 0;
                        if (long.TryParse(blobLastModifiedMetadata, out longLastModified))
                            blobLastModifiedUtc = new DateTime(longLastModified).ToUniversalTime();
                    }
                    
                    if (cachedLength != blobLength) fFileNeeded = true;
                    else
                    {

                        // cachedLastModifiedUTC was not ouputting with a date (just time) and the time was always off
                        long unixDate = CacheDirectory.FileModified(fileName);
                        DateTime start = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                        var cachedLastModifiedUtc = start.AddMilliseconds(unixDate).ToUniversalTime();
                        
                        if (cachedLastModifiedUtc != blobLastModifiedUtc)
                        {
                            var timeSpan = blobLastModifiedUtc.Subtract(cachedLastModifiedUtc);
                            if (timeSpan.TotalSeconds > 1)
                                fFileNeeded = true;
                            else
                            {
#if FULLDEBUG
                                Debug.WriteLine(timeSpan.TotalSeconds);
#endif
                                // file not needed
                            }
                        }
                    }
                }

                // if the file does not exist
                // or if it exists and it is older then the lastmodified time in the blobproperties (which always comes from the blob storage)
                if (fFileNeeded)
                {
                    if (_azureDirectory.ShouldCompressFile(_name))
                    {
                        InflateStream(fileName);
                    }
                    else
                    {
                        using (var fileStream = _azureDirectory.CreateCachedOutputAsStream(fileName))
                        {
                            // get the blob
                            if (_blob.Exists()) _blob.DownloadToStream(fileStream);

                            fileStream.Flush();
                            Debug.WriteLine($"GET {_name} RETREIVED {fileStream.Length} bytes");
                        }
                    }

                    // and open it as an input 
                    _indexInput = CacheDirectory.OpenInput(fileName);
                }
                else
                {
#if FULLDEBUG
                    Debug.WriteLine($"Using cached file for {_name}");
#endif

                    // open the file in read only mode
                    _indexInput = CacheDirectory.OpenInput(fileName);
                }
            }
            finally
            {
                _fileMutex.ReleaseMutex();
            }
        }

        private void InflateStream(string fileName)
        {
            // then we will get it fresh into local deflatedName 
            // StreamOutput deflatedStream = new StreamOutput(CacheDirectory.CreateOutput(deflatedName));
            using (var deflatedStream = new MemoryStream())
            {
                // get the deflated blob
                if (_blob.Exists()) _blob.DownloadToStream(deflatedStream);

                Debug.WriteLine($"GET {_name} RETREIVED {deflatedStream.Length} bytes");

                // seek back to begininng
                deflatedStream.Seek(0, SeekOrigin.Begin);

                // open output file for uncompressed contents
                using (var fileStream = _azureDirectory.CreateCachedOutputAsStream(fileName))
                using (var decompressor = new DeflateStream(deflatedStream, CompressionMode.Decompress))
                {
                    var bytes = new byte[65535];
                    var nRead = 0;
                    do
                    {
                        nRead = decompressor.Read(bytes, 0, 65535);
                        if (nRead > 0)
                            fileStream.Write(bytes, 0, nRead);
                    } while (nRead == 65535);
                }
            }
        }

        public AzureIndexInput(AzureIndexInput cloneInput)
        {
            _fileMutex = BlobMutexManager.GrabMutex(cloneInput._name);
            _fileMutex.WaitOne();

            try
            {
#if FULLDEBUG
                Debug.WriteLine($"Creating clone for {cloneInput._name}");
#endif
                _azureDirectory = cloneInput._azureDirectory;
                _blobContainer = cloneInput._blobContainer;
                _blob = cloneInput._blob;
                _indexInput = cloneInput._indexInput.Clone() as IndexInput;
            }
            catch (Exception)
            {
                // sometimes we get access denied on the 2nd stream...but not always. I haven't tracked it down yet
                // but this covers our tail until I do
                Debug.WriteLine($"Dagnabbit, falling back to memory clone for {cloneInput._name}");
            }
            finally
            {
                _fileMutex.ReleaseMutex();
            }
        }

        public override byte ReadByte()
        {
            return _indexInput.ReadByte();
        }

        public override void ReadBytes(byte[] b, int offset, int len)
        {
            _indexInput.ReadBytes(b, offset, len);
        }

        public override long FilePointer => _indexInput.FilePointer;

        public override void Seek(long pos)
        {
            _indexInput.Seek(pos);
        }

        protected override void Dispose(bool disposing)
        {
            _fileMutex.WaitOne();
            try
            {
#if FULLDEBUG
                Debug.WriteLine($"CLOSED READSTREAM local {_name}");
#endif
                _indexInput.Dispose();
                _indexInput = null;
                _azureDirectory = null;
                _blobContainer = null;
                _blob = null;
                GC.SuppressFinalize(this);
            }
            finally
            {
                _fileMutex.ReleaseMutex();
            }
        }

        public override long Length()
        {
            return _indexInput.Length();
        }

        public override System.Object Clone()
        {
            IndexInput clone = null;
            try
            {
                _fileMutex.WaitOne();
                AzureIndexInput input = new AzureIndexInput(this);
                clone = (IndexInput)input;
            }
            catch (System.Exception err)
            {
                Debug.WriteLine(err.ToString());
            }
            finally
            {
                _fileMutex.ReleaseMutex();
            }
            Debug.Assert(clone != null);
            return clone;
        }
    }
}

using System;
using System.IO;
using System.Threading.Tasks;
using ICSharpCode.SharpZipLib.Zip;
using Microsoft.WindowsAzure.Storage.Blob;

namespace AzureBlobToArchive
{
    public class ZipCore : CommonCore
    {
        private ZipOutputStream _archiveStream;

        public ZipCore(int level, CloudBlobClient blobClient, Stream outputStream) : base(blobClient, outputStream)
        {
            Level = level;

            var archiveStream = new ZipOutputStream(outputStream);
            archiveStream.IsStreamOwner = false;
            archiveStream.SetLevel(Level);

            _archiveStream = archiveStream;
        }

        public int Level { get; private set; }

        public override void Dispose()
        {
            _archiveStream?.Dispose();

            base.Dispose();
        }

        protected override Stream ArchiveStream
        {
            get { return _archiveStream; }
        }

        protected override string GetEntryName(CloudBlob blob)
        {
            return ZipEntry.CleanName(Uri.UnescapeDataString(blob.Uri.AbsolutePath.Trim('/')));
        }

        protected override void PutNextEntry(CloudBlob blob)
        {
            var entry = new ZipEntry(GetEntryName(blob));

            entry.Size = blob.Properties.Length;
            entry.DateTime = (blob.Properties.LastModified ?? new DateTimeOffset(2000, 01, 01, 00, 00, 00, TimeSpan.Zero)).UtcDateTime;

            _archiveStream.PutNextEntry(entry);
        }

        protected override Task CopyToArchiveStreamAsync(CloudBlob blob)
        {
            // ZipOutputStream does not support async (BeginWrite is not supported)
            blob.DownloadToStream(ArchiveStream);
            return Task.FromResult(0);
        }

        protected override Task CopyToArchiveStreamAsync(Stream stream)
        {
            // ZipOutputStream does not support async (BeginWrite is not supported)
            stream.CopyTo(ArchiveStream);
            return Task.FromResult(0);
        }

        protected override void CloseEntry()
        {
            _archiveStream.CloseEntry();
        }
    }
}
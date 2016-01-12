using System;
using System.IO;
using System.Threading.Tasks;
using ICSharpCode.SharpZipLib.Tar;
using Microsoft.WindowsAzure.Storage.Blob;

namespace AzureBlobToArchive
{
    public class TarCore : CommonCore
    {
        private TarOutputStream _archiveStream;

        public TarCore(CloudBlobClient blobClient, Stream outputStream) : base(blobClient, outputStream)
        {
            var archiveStream = new TarOutputStream(outputStream);
            archiveStream.IsStreamOwner = false;

            _archiveStream = archiveStream;
        }

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
            return Uri.UnescapeDataString(blob.Uri.AbsolutePath.Trim('/'));
        }

        protected override void PutNextEntry(CloudBlob blob)
        {
            var entry = TarEntry.CreateTarEntry(GetEntryName(blob));

            entry.Size = blob.Properties.Length;
            entry.ModTime = (blob.Properties.LastModified ?? new DateTimeOffset(2000, 01, 01, 00, 00, 00, TimeSpan.Zero)).UtcDateTime;

            _archiveStream.PutNextEntry(entry);
        }

        protected override Task CopyToArchiveStreamAsync(CloudBlob blob)
        {
            return blob.DownloadToStreamAsync(ArchiveStream);
        }

        protected override Task CopyToArchiveStreamAsync(Stream stream)
        {
            return stream.CopyToAsync(ArchiveStream);
        }

        protected override void CloseEntry()
        {
            _archiveStream.CloseEntry();
        }
    }
}
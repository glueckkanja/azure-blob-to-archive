using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;

namespace AzureBlobToArchive
{
    public abstract class CommonCore : IDisposable
    {
        protected readonly AsyncLock ArchiveStreamLock = new AsyncLock();

        public CommonCore(CloudBlobClient blobClient, Stream outputStream)
        {
            BlobClient = blobClient;
            OutputStream = outputStream;
        }

        public virtual void Dispose()
        {
        }

        protected CloudBlobClient BlobClient { get; private set; }
        protected Stream OutputStream { get; private set; }
        protected abstract Stream ArchiveStream { get; }

        public Action<string, int> Log { get; set; }
        public Action<string> LogError { get; set; }
        public Func<CloudBlobContainer, bool> ContainerFilter { get; set; }
        public Func<CloudBlob, bool> BlobFilter { get; set; }

        public int MaxParallelWorkers { get; set; } = 4;
        public int MaxBlobSizeForBuffering { get; set; } = 256 * 1024;

        protected abstract string GetEntryName(CloudBlob blob);
        protected abstract void PutNextEntry(CloudBlob blob);
        protected abstract Task CopyToArchiveStreamAsync(CloudBlob blob);
        protected abstract Task CopyToArchiveStreamAsync(Stream stream);
        protected abstract void CloseEntry();

        public async Task RunAsync()
        {
            var count = 0;

            var maxWorkers = Math.Max(1, MaxParallelWorkers) - 1;
            var maxSize = Math.Max(1024, MaxBlobSizeForBuffering);

            Log?.Invoke($"Using {maxWorkers + 1} workers and buffering blobs smaller than {(maxSize / 1024.0):F0} KB", 1);

            var buffers = new MemoryStream[maxWorkers];

            for (int i = 0; i < maxWorkers; i++)
                buffers[i] = new MemoryStream(maxSize);

            var sw = Stopwatch.StartNew();

            foreach (var container in BlobClient.ListContainers())
            {
                if (ContainerFilter?.Invoke(container) == false)
                    continue;

                count += await ProcessContainerAsync(container, buffers, maxSize);
            }

            sw.Stop();

            Log?.Invoke($"Archived {count} files in {sw.Elapsed.TotalSeconds:F0} seconds", 0);
        }

        private async Task<int> ProcessContainerAsync(CloudBlobContainer container, MemoryStream[] buffers, int maxParallelSize)
        {
            Log($"Processing container `{container.Name}`", 0);

            var count = 0;
            BlobResultSegment blobListSegment = null;
            Func<bool> hasMoreSegments = () => blobListSegment?.ContinuationToken != null;

            do
            {
                blobListSegment = await container.ListBlobsSegmentedAsync(null, true, BlobListingDetails.None, null, blobListSegment?.ContinuationToken, null, null, CancellationToken.None);

                var blobs = blobListSegment.Results.OfType<CloudBlob>()
                    .Where(blob => !(BlobFilter?.Invoke(blob) == false))
                    .ToList();

                Log($"Fetched {(!hasMoreSegments() ? "final" : "partial")} listing of {blobs.Count} blobs", 0);

                if (buffers.Length > 0)
                {
                    using (var queue = new BlockingCollection<Func<MemoryStream, Task<CloudBlob>>>())
                    {
                        var sequentialBlobs = new List<CloudBlob>();

                        foreach (var blob in blobs)
                        {
                            AddBlobToQueue(blob, queue, sequentialBlobs, maxParallelSize);
                        }

                        queue.CompleteAdding();

                        Log($"Transfering {sequentialBlobs.Count} blobs sequentially and {queue.Count} blobs in parallel", 1);

                        var pool = Enumerable.Range(0, buffers.Length)
                            .Select(id => Worker(queue, buffers[id]))
                            .ToList();

                        foreach (var blob in sequentialBlobs)
                        {
                            using (await ArchiveStreamLock.LockAsync())
                            {
                                await WriteEntryAsync(blob);
                            }
                        }

                        await Task.WhenAll(pool);
                    }
                }
                else
                {
                    foreach (var blob in blobs)
                    {
                        await WriteEntryAsync(blob);
                    }
                }

                count += blobs.Count;

            } while (hasMoreSegments());

            return count;
        }

        private async Task Worker(BlockingCollection<Func<MemoryStream, Task<CloudBlob>>> queue, MemoryStream buffer)
        {
            foreach (var item in queue.GetConsumingEnumerable())
            {
                buffer.SetLength(0);

                var blob = await item(buffer);
                buffer.Position = 0;

                using (await ArchiveStreamLock.LockAsync())
                {
                    await WriteEntryAsync(blob, buffer);
                }
            }
        }

        private void AddBlobToQueue(CloudBlob blob, BlockingCollection<Func<MemoryStream, Task<CloudBlob>>> queue, List<CloudBlob> sequentialBlobs, int maxParallelSize)
        {
            if (blob.Properties.Length < maxParallelSize)
            {
                Log($"Queueing `{GetEntryName(blob)}` for parallel transfer", 3);

                queue.Add(async (buffer) =>
                {
                    Log($"Begin buffering {blob.Properties.Length} bytes of `{GetEntryName(blob)}`", 3);
                    await blob.DownloadToStreamAsync(buffer);
                    Log($"Finished buffering {blob.Properties.Length} bytes of `{GetEntryName(blob)}`", 2);
                    return blob;
                });
            }
            else
            {
                Log($"Queueing `{GetEntryName(blob)}` for sequential transfer", 3);

                sequentialBlobs.Add(blob);
            }
        }

        private async Task WriteEntryAsync(CloudBlob blob)
        {
            PutNextEntry(blob);

            Log($"Begin transfering {blob.Properties.Length} bytes of `{GetEntryName(blob)}`", 3);
            await CopyToArchiveStreamAsync(blob);
            Log($"Finished transfering {blob.Properties.Length} bytes of `{GetEntryName(blob)}`", 2);

            CloseEntry();
        }

        private async Task WriteEntryAsync(CloudBlob blob, MemoryStream stream)
        {
            PutNextEntry(blob);

            Log($"Begin writing {blob.Properties.Length} bytes of `{GetEntryName(blob)}`", 3);
            await CopyToArchiveStreamAsync(stream);
            Log($"Finished writing {blob.Properties.Length} bytes of `{GetEntryName(blob)}`", 2);

            CloseEntry();
        }
    }
}
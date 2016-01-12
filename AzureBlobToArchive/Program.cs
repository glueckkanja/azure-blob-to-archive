using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.WindowsAzure.Storage;
using Mono.Options;

namespace AzureBlobToArchive
{
    public class Program
    {
        private static bool _logToStderr;
        private static bool _logToStdout;
        private static int _verbosity;

        public static int Main(string[] args)
        {
            string sourceParam = null;
            string outParam = null;
            string typeParam = null;
            var helpParam = false;
            var compressionParam = 0;
            var bufferParam = 1024 * 1024;
            var workersParam = 8;
            var containerParams = new List<string>();

            var p = new OptionSet {
                { "s|source=", v => sourceParam = v },
                { "o|out=", v => outParam = v },
                { "c|container:", v => { if (v != null) containerParams.Add(v); }},
                { "t|type:", v => typeParam = v },
                { "compression:", (int v) => compressionParam = v },
                { "buffer:", (int v) => bufferParam = v*1024 },
                { "workers:", (int v) => workersParam = v },
                { "v|verbose", v => ++_verbosity },
                { "h|?|help", v => helpParam = v != null },
            };

            List<string> extra;

            try
            {
                extra = p.Parse(args);
            }
            catch (OptionException e)
            {
                Console.WriteLine(e.Message);
                Console.WriteLine("Check `--help` for more information.");
                return 100;
            }

            CloudStorageAccount account;

            if (!CloudStorageAccount.TryParse(sourceParam, out account))
            {
                LogError("Invalid Azure blob storage connection string specified for argument `--source`.\r\nCheck out https://azure.microsoft.com/en-us/documentation/articles/storage-configure-connection-string/");
                return 100;
            }

            if (string.IsNullOrWhiteSpace(outParam))
            {
                LogError("Invalid value specified for argument `--out`.\r\nValid options are:\r\n  - any path\r\n  - stdout");
                return 100;
            }

            var writeToStdout = string.Equals(outParam, "stdout", StringComparison.OrdinalIgnoreCase);
            _logToStdout = !writeToStdout;
            _logToStderr = !_logToStdout;

            Stream output = writeToStdout
                ? Console.OpenStandardOutput()
                : File.Create(outParam);

            if (typeParam == null)
            {
                typeParam = Path.GetExtension(outParam).TrimStart('.');

                if (string.IsNullOrEmpty(typeParam))
                    typeParam = "tar";
            }

            var blobClient = account.CreateCloudBlobClient();

            CommonCore core = null;

            if (typeParam == "tar") core = new TarCore(blobClient, output);
            if (typeParam == "zip") core = new ZipCore(compressionParam, blobClient, output);

            if (core == null)
            {
                LogError($"Unknown archive type `{typeParam}`.");
                return 100;
            }

            core.Log = Log;
            core.LogError = LogError;

            core.MaxBlobSizeForBuffering = bufferParam;
            core.MaxParallelWorkers = workersParam;

            if (containerParams.Any())
                core.ContainerFilter = container => containerParams.Contains(container.Name);

            using (output)
            using (core)
            {
                core.RunAsync().Wait();
                return 0;
            }
        }

        public static void Log(string line, int level)
        {
            if (_verbosity < level) return;

            if (_logToStdout)
            {
                Console.Out.WriteLine(line);
            }
            else if (_logToStderr)
            {
                var prefix = _logToStderr ? " [INFO] " : "";
                Console.Error.WriteLine(prefix + line);
            }
        }

        public static void LogError(string line)
        {
            var prefix = _logToStderr ? "[ERROR] " : "";
            Console.Error.WriteLine(prefix + line);
        }
    }
}

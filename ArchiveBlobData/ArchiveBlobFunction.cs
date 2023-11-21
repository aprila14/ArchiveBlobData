using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ArchiveBlobData
{
    public class ArchiveBlobFunction
    {
        private readonly IConfiguration _configuration;
        public ArchiveBlobFunction(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        [FunctionName("ArchiveBlobFunction")]
        public async Task Run([TimerTrigger("0 0 0 * * *")] TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");
            var connectionString = _configuration.GetValue<string>("AzureFunctionsJobHost:Configurations:Connectionstring");
            var containerName = _configuration.GetValue<string>("AzureFunctionsJobHost:Configurations:ContainerName");


            BlobContainerClient client = new BlobContainerClient(connectionString, containerName);
            var result = client.GetBlobsByHierarchyAsync(delimiter: "/");
            var blobFolders = new List<string>();
            var blobItems = new List<BlobItem>();
            string continuationToken = null;
            int pageSize = 10;
            do
            {
                await foreach (var blobPages in result.AsPages(continuationToken, pageSize))
                {
                    continuationToken = blobPages.ContinuationToken;
                    blobFolders.AddRange(blobPages.Values.Where(b => b.IsPrefix && b.Prefix.Contains("VoCuSensor1-archive")).Select(b => b.Prefix));
                    blobItems.AddRange(blobPages.Values.Where(b => b.IsBlob).Select(b => b.Blob));
                }
            } while (!string.IsNullOrWhiteSpace(continuationToken));


            foreach (var blobItem in blobItems)
            {
                var b1 = client.GetBlobClient(blobItem.Name);
                //var targetBlobClient = client.GetBlockBlobClient($"{blobFolders[0]}{blobItem.Name}-{"year:"}{DateTime.Now.ToString("yyyy")}-{"month:"}{DateTime.Now.ToString("MM")}-{"day:"}{DateTime.Now.ToString("dd")}-{"hour:"}{DateTime.Now.ToString("HH")}-{"minutes:"}{DateTime.Now.ToString("mm")}-{"seconds:"}{DateTime.Now.ToString("ss")}");
                var targetBlobClient = client.GetBlockBlobClient($"{blobFolders[0]}{blobItem.Name}");
                using (var stream = new MemoryStream())
                {
                    await b1.DownloadToAsync(stream);
                    stream.Position = 0;
                    await targetBlobClient.UploadAsync(stream);
                    await b1.DeleteAsync();
                }
            }

        }
    }
}

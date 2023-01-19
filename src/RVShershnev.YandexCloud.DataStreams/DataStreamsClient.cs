using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Amazon.Runtime;
using Amazon.Runtime.Internal;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Threading.Tasks;

namespace RVShershnev.YandexCloud.DataStreams
{

    public class DataStreamsClient : IDataStreamsClient, IDisposable
    {
        internal DataStreamsConfiguration dataStreamConfiguration;
        internal AmazonKinesisConfig config;           

        internal AmazonKinesisClient client { get; private set; }
        public DataStreamsClient([NotNull] DataStreamsConfiguration dataStreamConfiguration)
        {
            this.dataStreamConfiguration = dataStreamConfiguration;
            config = new AmazonKinesisConfig()
            {
                ServiceURL = this.dataStreamConfiguration.ServiceURL,
                AuthenticationRegion = this.dataStreamConfiguration.Region
            };
            client = new AmazonKinesisClient(dataStreamConfiguration.KeyId, dataStreamConfiguration.Secret, config);
        }
             

        /// <summary>
        /// Уменьшает время хранения сообщений в потоке данных.
        /// Доступные значения времени хранения: 4 часа, 12 часов, 24 часа.
        /// </summary>
        /// <param name="request">Тело запроса.</param>
        /// <returns></returns>
        public async Task<DecreaseStreamRetentionPeriodResponse> DecreaseStreamRetentionPeriod(DecreaseStreamRetentionPeriodRequest request)
        {
            return await client.DecreaseStreamRetentionPeriodAsync(request);
        }
        public async Task<DeleteStreamResponse> DeleteStream(DeleteStreamRequest request)
        {
            return await client.DeleteStreamAsync(request);
        }

        public async Task<DescribeStreamResponse> DescribeStream(DescribeStreamRequest request)
        {
            return await client.DescribeStreamAsync(request);
        }

        public async Task<IncreaseStreamRetentionPeriodResponse> IncreaseStreamRetentionPeriod(IncreaseStreamRetentionPeriodRequest request)
        {
            return await client.IncreaseStreamRetentionPeriodAsync(request);
        }

        /// <summary>
        /// Получает сообщения из сегмента потока данных.
        /// </summary>
        /// <param name="request">Тело запроса.</param>
        /// <returns></returns>
        public async Task<GetRecordsResponse> GetRecords(GetRecordsRequest request)
        {
            return await client.GetRecordsAsync(request);
        }

        public async Task<GetShardIteratorResponse> GetShardIterator(GetShardIteratorRequest request)
        {
            return await client.GetShardIteratorAsync(request);
        }

        public async Task<ListStreamsResponse> ListStreams(ListStreamsRequest request)
        {
            return await client.ListStreamsAsync(request);
        }

        public async Task<PutRecordResponse> PutRecord(PutRecordRequest request)
        {
            return await client.PutRecordAsync(request);
        }

        public async Task<UpdateShardCountResponse> UpdateShardCount(UpdateShardCountRequest request)
        {
            return await client.UpdateShardCountAsync(request);
        }

        public async Task<PutRecordsResponse> PutRecords(PutRecordsRequest request)
        {
            return await client.PutRecordsAsync(request);
        }
        public void Dispose()
        {
            client?.Dispose();
        }
               

       
    }
}
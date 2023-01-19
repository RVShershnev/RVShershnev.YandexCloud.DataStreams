using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Amazon.Runtime.Internal;

namespace RVShershnev.YandexCloud.DataStreams
{
    public interface IDataStreamsClient
    {        
        Task<DecreaseStreamRetentionPeriodResponse> DecreaseStreamRetentionPeriod(DecreaseStreamRetentionPeriodRequest request);
        Task<DeleteStreamResponse> DeleteStream(DeleteStreamRequest request);
        Task<DescribeStreamResponse> DescribeStream(DescribeStreamRequest request);
        Task<IncreaseStreamRetentionPeriodResponse> IncreaseStreamRetentionPeriod(IncreaseStreamRetentionPeriodRequest request);
        Task<GetRecordsResponse> GetRecords(GetRecordsRequest request);
        Task<GetShardIteratorResponse> GetShardIterator(GetShardIteratorRequest request);
        Task<ListStreamsResponse> ListStreams(ListStreamsRequest request);
        Task<PutRecordResponse> PutRecord(PutRecordRequest request);
        Task<PutRecordsResponse> PutRecords(PutRecordsRequest request);
        Task<UpdateShardCountResponse> UpdateShardCount(UpdateShardCountRequest request);

    }
}